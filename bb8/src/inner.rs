use std::cmp::{max, min};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use std::{fmt, mem};

use futures_channel::oneshot;
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::{Mutex, MutexGuard};
use tokio::spawn;
use tokio::time::{delay_for, interval_at, timeout, Interval};

use crate::api::{Builder, ManageConnection, RunError, State};

#[derive(Debug)]
pub(crate) struct Conn<C>
where
    C: Send,
{
    pub(crate) conn: C,
    birth: Instant,
}

struct IdleConn<C>
where
    C: Send,
{
    conn: Conn<C>,
    idle_start: Instant,
}

impl<C> IdleConn<C>
where
    C: Send,
{
    pub(crate) fn make_idle(conn: Conn<C>) -> IdleConn<C> {
        let now = Instant::now();
        IdleConn {
            conn,
            idle_start: now,
        }
    }
}

/// The pool data that must be protected by a lock.
#[allow(missing_debug_implementations)]
struct PoolInternals<C>
where
    C: Send,
{
    waiters: VecDeque<oneshot::Sender<Conn<C>>>,
    conns: VecDeque<IdleConn<C>>,
    num_conns: u32,
    pending_conns: u32,
}

impl<C> PoolInternals<C>
where
    C: Send,
{
    fn put_idle_conn(&mut self, mut conn: IdleConn<C>) {
        loop {
            if let Some(waiter) = self.waiters.pop_front() {
                // This connection is no longer idle, send it back out.
                match waiter.send(conn.conn) {
                    Ok(_) => break,
                    // Oops, that receiver was gone. Loop and try again.
                    Err(c) => conn.conn = c,
                }
            } else {
                // Queue it in the idle queue.
                self.conns.push_back(conn);
                break;
            }
        }
    }

    fn approvals<M: ManageConnection>(&mut self, config: &Builder<M>, num: u32) -> ApprovalIter {
        let current = self.num_conns + self.pending_conns;
        let allowed = if current < config.max_size {
            config.max_size - current
        } else {
            0
        };

        let num = min(num, allowed);
        self.pending_conns += num;
        ApprovalIter { num: num as usize }
    }

    fn want_more<M: ManageConnection>(&self, config: &Builder<M>) -> u32 {
        let available = self.conns.len() as u32 + self.pending_conns;
        let min_idle = config.min_idle.unwrap_or(0);
        if available < min_idle {
            min_idle - available
        } else {
            0
        }
    }
}

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
        let internals = PoolInternals {
            waiters: VecDeque::new(),
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
        };

        let inner = Arc::new(SharedPool {
            statics: builder,
            manager,
            internals: Mutex::new(internals),
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

    pub(crate) fn spawn_replenishing(self, approvals: ApprovalIter) {
        if approvals.len() == 0 {
            return;
        }

        spawn(async move {
            let mut stream = self.replenish_idle_connections(approvals);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(()) => {}
                    Err(e) => self.inner.statics.error_sink.sink(e),
                }
            }
        });
    }

    pub(crate) async fn get(&self) -> Result<Conn<M::Connection>, RunError<M::Error>> {
        loop {
            let conn = {
                let mut locked = self.inner.internals.lock();
                match locked.conns.pop_front() {
                    Some(conn) => {
                        let wanted = locked.want_more(&self.inner.statics);
                        let approvals = locked.approvals(&self.inner.statics, wanted);
                        self.clone().spawn_replenishing(approvals);
                        conn
                    }
                    None => break,
                }
            };

            if self.inner.statics.test_on_check_out {
                let (mut conn, birth) = (conn.conn.conn, conn.conn.birth);

                match self.inner.manager.is_valid(&mut conn).await {
                    Ok(()) => return Ok(Conn { conn, birth }),
                    Err(_) => {
                        mem::drop(conn);
                        let mut internals = self.inner.internals.lock();
                        self.drop_connections(&mut internals, 1);
                    }
                }
                continue;
            } else {
                return Ok(conn.conn);
            }
        }

        let (tx, rx) = oneshot::channel();
        {
            let mut locked = self.inner.internals.lock();
            locked.waiters.push_back(tx);
            let approvals = locked.approvals(&self.inner.statics, 1);
            self.clone().spawn_replenishing(approvals);
        };

        match timeout(self.inner.statics.connection_timeout, rx).await {
            Ok(Ok(conn)) => Ok(conn),
            _ => Err(RunError::TimedOut),
        }
    }

    pub(crate) async fn connect(&self) -> Result<M::Connection, M::Error> {
        self.inner.manager.connect().await
    }

    pub(crate) fn wanted(&self) -> ApprovalIter {
        let mut internals = self.inner.internals.lock();
        let wanted = internals.want_more(&self.inner.statics);
        internals.approvals(&self.inner.statics, wanted)
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, birth: Instant, mut conn: M::Connection) {
        // Supposed to be fast, but do it before locking anyways.
        let broken = self.inner.manager.has_broken(&mut conn);

        let mut locked = self.inner.internals.lock();
        if broken {
            self.drop_connections(&mut locked, 1);
        } else {
            let conn = IdleConn::make_idle(Conn { conn, birth });
            locked.put_idle_conn(conn);
        }
    }

    /// Returns information about the current state of the pool.
    pub(crate) fn state(&self) -> State {
        let locked = self.inner.internals.lock();
        State {
            connections: locked.num_conns,
            idle_connections: locked.conns.len() as u32,
        }
    }

    fn reap(&self) {
        let mut internals = self.inner.internals.lock();
        let now = Instant::now();
        let before = internals.conns.len();

        internals.conns.retain(|conn| {
            let mut keep = true;
            if let Some(timeout) = self.inner.statics.idle_timeout {
                keep &= now - conn.idle_start < timeout;
            }
            if let Some(lifetime) = self.inner.statics.max_lifetime {
                keep &= now - conn.conn.birth < lifetime;
            }
            keep
        });

        let dropped = before - internals.conns.len();
        self.drop_connections(&mut internals, dropped);
    }

    // Outside of Pool to avoid borrow splitting issues on self
    async fn add_connection(&self, _: Approval) -> Result<(), M::Error>
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
                    let now = Instant::now();
                    let conn = IdleConn {
                        conn: Conn { conn, birth: now },
                        idle_start: now,
                    };

                    let mut locked = shared.internals.lock();
                    locked.pending_conns -= 1;
                    locked.num_conns += 1;
                    locked.put_idle_conn(conn);
                    return Ok(());
                }
                Err(e) => {
                    if Instant::now() - start > self.inner.statics.connection_timeout {
                        let mut locked = shared.internals.lock();
                        locked.pending_conns -= 1;
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
    fn drop_connections(
        &self,
        internals: &mut MutexGuard<PoolInternals<M::Connection>>,
        dropped: usize,
    ) where
        M: ManageConnection,
    {
        internals.num_conns -= dropped as u32;
        // We might need to spin up more connections to maintain the idle limit, e.g.
        // if we hit connection lifetime limits
        let num = internals.want_more(&self.inner.statics);
        self.clone()
            .spawn_replenishing(internals.approvals(&self.inner.statics, num));
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
    internals: Mutex<PoolInternals<M::Connection>>,
}

pub(crate) struct ApprovalIter {
    num: usize,
}

impl Iterator for ApprovalIter {
    type Item = Approval;

    fn next(&mut self) -> Option<Self::Item> {
        match self.num {
            0 => None,
            _ => {
                self.num -= 1;
                Some(Approval { _priv: () })
            }
        }
    }
}

impl ExactSizeIterator for ApprovalIter {
    fn len(&self) -> usize {
        self.num
    }
}

pub(crate) struct Approval {
    _priv: (),
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
