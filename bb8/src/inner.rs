use std::cmp::{max, min};
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use std::{fmt, mem};

use futures_channel::oneshot;
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::{Mutex, MutexGuard};
use tokio::spawn;
use tokio::time::{delay_for, interval_at, timeout, Interval};

use crate::api::{Builder, ManageConnection, RunError};
use crate::internals::{Approval, ApprovalIter, Conn, PoolInternals, State};

pub(crate) struct PoolInner<M>
where
    M: ManageConnection + Send,
{
    inner: Arc<SharedPool<M>>,
}

impl<M> PoolInner<M>
where
    M: ManageConnection + Send,
{
    pub(crate) fn new(builder: Builder<M>, manager: M) -> Self {
        let inner = Arc::new(SharedPool {
            statics: builder,
            manager,
            internals: Mutex::new(PoolInternals::default()),
        });

        if inner.statics.max_lifetime.is_some() || inner.statics.idle_timeout.is_some() {
            let s = Arc::downgrade(&inner);
            if let Some(shared) = s.upgrade() {
                let start = Instant::now() + shared.statics.reaper_rate;
                let interval = interval_at(start.into(), shared.statics.reaper_rate);
                schedule_reaping(interval, s);
            }
        }

        Self { inner }
    }

    pub(crate) fn spawn_replenishing(&self) {
        let mut locked = self.inner.internals.lock();
        self.spawn_replenishing_approvals(locked.wanted(&self.inner.statics));
    }

    pub(crate) fn wanted(&self) -> ApprovalIter {
        self.inner.internals.lock().wanted(&self.inner.statics)
    }

    fn spawn_replenishing_locked(&self, locked: &mut MutexGuard<PoolInternals<M>>) {
        self.spawn_replenishing_approvals(locked.wanted(&self.inner.statics));
    }

    fn spawn_replenishing_approvals(&self, approvals: ApprovalIter) {
        if approvals.len() == 0 {
            return;
        }

        let this = self.clone();
        spawn(async move {
            let mut stream = this.replenish_idle_connections(approvals);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(()) => {}
                    Err(e) => this.inner.statics.error_sink.sink(e),
                }
            }
        });
    }

    pub(crate) fn replenish_idle_connections(
        &self,
        approvals: ApprovalIter,
    ) -> FuturesUnordered<impl Future<Output = Result<(), M::Error>>> {
        let stream = FuturesUnordered::new();
        for approval in approvals {
            let this = self.clone();
            stream.push(async move { this.add_connection(approval).await });
        }
        stream
    }

    pub(crate) async fn get(&self) -> Result<Conn<M::Connection>, RunError<M::Error>> {
        loop {
            let mut conn = {
                let mut locked = self.inner.internals.lock();
                match locked.pop() {
                    Some(conn) => {
                        self.spawn_replenishing_locked(&mut locked);
                        conn
                    }
                    None => break,
                }
            };

            if self.inner.statics.test_on_check_out {
                match self.inner.manager.is_valid(&mut conn.conn).await {
                    Ok(()) => return Ok(conn),
                    Err(_) => {
                        mem::drop(conn);
                        let mut internals = self.inner.internals.lock();
                        self.drop_connections(&mut internals, 1);
                    }
                }
                continue;
            } else {
                return Ok(conn);
            }
        }

        let (tx, rx) = oneshot::channel();
        {
            let mut locked = self.inner.internals.lock();
            let approvals = locked.push_waiter(tx, &self.inner.statics);
            self.spawn_replenishing_approvals(approvals);
        };

        match timeout(self.inner.statics.connection_timeout, rx).await {
            Ok(Ok(conn)) => Ok(conn),
            _ => Err(RunError::TimedOut),
        }
    }

    pub(crate) async fn connect(&self) -> Result<M::Connection, M::Error> {
        self.inner.manager.connect().await
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, mut conn: Conn<M::Connection>) {
        // Supposed to be fast, but do it before locking anyways.
        let broken = self.inner.manager.has_broken(&mut conn.conn);

        let mut locked = self.inner.internals.lock();
        if broken {
            self.drop_connections(&mut locked, 1);
        } else {
            locked.put(conn, None);
        }
    }

    /// Returns information about the current state of the pool.
    pub(crate) fn state(&self) -> State {
        self.inner.internals.lock().state()
    }

    fn reap(&self) {
        let mut internals = self.inner.internals.lock();
        let dropped = internals.reap(&self.inner.statics);
        self.drop_connections(&mut internals, dropped);
    }

    // Outside of Pool to avoid borrow splitting issues on self
    async fn add_connection(&self, approval: Approval) -> Result<(), M::Error>
    where
        M: ManageConnection,
    {
        let new_shared = Arc::downgrade(&self.inner);
        let shared = match new_shared.upgrade() {
            None => return Ok(()),
            Some(shared) => shared,
        };

        let start = Instant::now();
        let mut delay = Duration::from_secs(0);
        loop {
            match shared.manager.connect().await {
                Ok(conn) => {
                    let conn = Conn::new(conn);
                    shared.internals.lock().put(conn, Some(approval));
                    return Ok(());
                }
                Err(e) => {
                    if Instant::now() - start > self.inner.statics.connection_timeout {
                        let mut locked = shared.internals.lock();
                        locked.connect_failed(approval);
                        return Err(e);
                    } else {
                        delay = max(Duration::from_millis(200), delay);
                        delay = min(self.inner.statics.connection_timeout / 2, delay * 2);
                        delay_for(delay).await;
                    }
                }
            }
        }
    }

    // Drop connections
    // NB: This is called with the pool lock held.
    fn drop_connections(&self, internals: &mut MutexGuard<PoolInternals<M>>, dropped: usize)
    where
        M: ManageConnection,
    {
        internals.dropped(dropped as u32);
        // We might need to spin up more connections to maintain the idle limit, e.g.
        // if we hit connection lifetime limits
        self.spawn_replenishing_locked(internals);
    }
}

impl<M> Clone for PoolInner<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Self {
        PoolInner {
            inner: self.inner.clone(),
        }
    }
}

impl<M> fmt::Debug for PoolInner<M>
where
    M: ManageConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("PoolInner({:p})", self.inner))
    }
}

/// The guts of a `Pool`.
#[allow(missing_debug_implementations)]
struct SharedPool<M>
where
    M: ManageConnection + Send,
{
    statics: Builder<M>,
    manager: M,
    internals: Mutex<PoolInternals<M>>,
}

fn schedule_reaping<M>(mut interval: Interval, weak_shared: Weak<SharedPool<M>>)
where
    M: ManageConnection,
{
    spawn(async move {
        loop {
            let _ = interval.tick().await;
            if let Some(inner) = weak_shared.upgrade() {
                PoolInner { inner }.reap()
            } else {
                break;
            }
        }
    });
}
