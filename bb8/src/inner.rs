use std::cmp::{max, min};
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use std::{fmt, mem};

use futures_channel::oneshot;
use parking_lot::{Mutex, MutexGuard};
use tokio::spawn;
use tokio::time::{delay_for, interval_at, timeout, Interval};

use crate::api::{Builder, ManageConnection, Pool, RunError, State};

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

    pub(crate) async fn get_conn<E>(&self) -> Result<Conn<M::Connection>, RunError<E>> {
        while let Some((conn, approvals)) = self.get() {
            // Spin up a new connection if necessary to retain our minimum idle count
            if approvals.len() > 0 {
                Pool {
                    inner: self.clone(),
                }
                .spawn_replenishing(approvals);
            }

            if self.inner.statics.test_on_check_out {
                let (mut conn, birth) = (conn.conn.conn, conn.conn.birth);

                match self.inner.manager.is_valid(&mut conn).await {
                    Ok(()) => return Ok(Conn { conn, birth }),
                    Err(_) => {
                        mem::drop(conn);
                        let mut internals = self.inner.internals.lock();
                        drop_connections(&self.inner, &mut internals, 1);
                    }
                }
                continue;
            } else {
                return Ok(conn.conn);
            }
        }

        let (tx, rx) = oneshot::channel();
        if let Some(approval) = self.can_add_more(Some(tx)) {
            let this = self.clone();
            spawn(async move { this.sink_error(add_connection(this.clone(), approval).await) });
        }

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
        let num = self.inner.want_more(&mut internals);
        self.inner.approvals(&mut internals, num)
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, birth: Instant, mut conn: M::Connection) {
        let inner = self.inner.clone();

        // Supposed to be fast, but do it before locking anyways.
        let broken = inner.manager.has_broken(&mut conn);

        let mut locked = inner.internals.lock();
        if broken {
            drop_connections(&inner, &mut locked, 1);
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

    pub(crate) fn sink_error(&self, result: Result<(), M::Error>) {
        match result {
            Ok(()) => {}
            Err(e) => self.inner.statics.error_sink.sink(e),
        }
    }

    fn get(&self) -> Option<(IdleConn<M::Connection>, ApprovalIter)> {
        let mut locked = self.inner.internals.lock();
        locked.conns.pop_front().map(|conn| {
            let wanted = self.inner.want_more(&mut locked);
            (conn, self.inner.approvals(&mut locked, wanted))
        })
    }

    fn can_add_more(
        &self,
        waiter: Option<oneshot::Sender<Conn<M::Connection>>>,
    ) -> Option<Approval> {
        let mut locked = self.inner.internals.lock();
        if let Some(waiter) = waiter {
            locked.waiters.push_back(waiter);
        }

        if locked.num_conns + locked.pending_conns < self.inner.statics.max_size {
            locked.pending_conns += 1;
            Some(Approval { _priv: () })
        } else {
            None
        }
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

impl<M> SharedPool<M>
where
    M: ManageConnection + Send,
{
    fn approvals(
        &self,
        locked: &mut MutexGuard<PoolInternals<M::Connection>>,
        num: u32,
    ) -> ApprovalIter {
        let current = locked.num_conns + locked.pending_conns;
        let allowed = if current < self.statics.max_size {
            self.statics.max_size - current
        } else {
            0
        };

        let num = min(num, allowed);
        locked.pending_conns += num;
        ApprovalIter { num: num as usize }
    }

    fn want_more(&self, locked: &mut MutexGuard<PoolInternals<M::Connection>>) -> u32 {
        let available = locked.conns.len() as u32 + locked.pending_conns;
        let min_idle = self.statics.min_idle.unwrap_or(0);
        if available < min_idle {
            min_idle - available
        } else {
            0
        }
    }
}

// Outside of Pool to avoid borrow splitting issues on self
pub(crate) async fn add_connection<M>(pool: PoolInner<M>, _: Approval) -> Result<(), M::Error>
where
    M: ManageConnection,
{
    let new_shared = Arc::downgrade(&pool.inner);
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
                if Instant::now() - start > pool.inner.statics.connection_timeout {
                    let mut locked = shared.internals.lock();
                    locked.pending_conns -= 1;
                    return Err(e);
                } else {
                    delay = max(Duration::from_millis(200), delay);
                    delay = min(pool.inner.statics.connection_timeout / 2, delay * 2);
                    delay_for(delay).await;
                }
            }
        }
    }
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

// Drop connections
// NB: This is called with the pool lock held.
fn drop_connections<'a, M>(
    pool: &Arc<SharedPool<M>>,
    internals: &mut MutexGuard<'a, PoolInternals<M::Connection>>,
    dropped: usize,
) where
    M: ManageConnection,
{
    internals.num_conns -= dropped as u32;
    // We might need to spin up more connections to maintain the idle limit, e.g.
    // if we hit connection lifetime limits
    let num = pool.want_more(internals);
    if num > 0 {
        Pool {
            inner: PoolInner {
                inner: pool.clone(),
            },
        }
        .spawn_replenishing(pool.approvals(internals, num));
    }
}

fn schedule_reaping<M>(mut interval: Interval, weak_shared: Weak<SharedPool<M>>)
where
    M: ManageConnection,
{
    spawn(async move {
        loop {
            let _ = interval.tick().await;
            if let Some(pool) = weak_shared.upgrade() {
                let mut internals = pool.internals.lock();
                let now = Instant::now();
                let before = internals.conns.len();

                internals.conns.retain(|conn| {
                    let mut keep = true;
                    if let Some(timeout) = pool.statics.idle_timeout {
                        keep &= now - conn.idle_start < timeout;
                    }
                    if let Some(lifetime) = pool.statics.max_lifetime {
                        keep &= now - conn.conn.birth < lifetime;
                    }
                    keep
                });

                let dropped = before - internals.conns.len();
                drop_connections(&pool, &mut internals, dropped);
            } else {
                break;
            }
        }
    });
}
