use std::cmp::{max, min};
use std::collections::VecDeque;
use std::mem;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use futures::prelude::*;
use tokio::spawn;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{delay_for, timeout, Interval};

use super::{Builder, ManageConnection, Pool};

#[derive(Debug)]
pub(crate) struct Conn<C>
where
    C: Send,
{
    pub(crate) conn: C,
    pub(crate) birth: Instant,
}

pub(crate) struct IdleConn<C>
where
    C: Send,
{
    pub(crate) conn: Conn<C>,
    pub(crate) idle_start: Instant,
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
pub(crate) struct PoolInternals<C>
where
    C: Send,
{
    pub(crate) waiters: VecDeque<oneshot::Sender<Conn<C>>>,
    pub(crate) conns: VecDeque<IdleConn<C>>,
    pub(crate) num_conns: u32,
    pub(crate) pending_conns: u32,
}

impl<C> PoolInternals<C>
where
    C: Send,
{
    pub(crate) fn put_idle_conn(&mut self, mut conn: IdleConn<C>) {
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

/// The guts of a `Pool`.
#[allow(missing_debug_implementations)]
pub(crate) struct SharedPool<M>
where
    M: ManageConnection + Send,
{
    pub(crate) statics: Builder<M>,
    pub(crate) manager: M,
    pub(crate) internals: Mutex<PoolInternals<M::Connection>>,
}

impl<M> SharedPool<M>
where
    M: ManageConnection,
{
    pub(crate) async fn sink_error<'a, E, F, T>(&self, f: F) -> Result<T, ()>
    where
        F: Future<Output = Result<T, E>> + Send + 'a,
        E: Into<M::Error>,
    {
        let sink = self.statics.error_sink.boxed_clone();
        f.await.map_err(|e| sink.sink(e.into()))
    }

    pub(crate) async fn or_timeout<'a, E, F, T>(&self, f: F) -> Result<Option<T>, E>
    where
        F: Future<Output = Result<T, E>> + Send + 'a,
        T: Send + 'a,
        E: Send + ::std::fmt::Debug + 'a,
    {
        timeout(self.statics.connection_timeout, f)
            .map(|r| match r {
                Ok(Ok(item)) => Ok(Some(item)),
                Ok(Err(e)) => Err(e),
                Err(_) => Ok(None),
            })
            .await
    }
}

// Outside of Pool to avoid borrow splitting issues on self
pub(crate) async fn add_connection<M>(pool: Arc<SharedPool<M>>) -> Result<(), M::Error>
where
    M: ManageConnection,
{
    let mut internals = pool.internals.lock().await;
    if internals.num_conns + internals.pending_conns >= pool.statics.max_size {
        return Ok(());
    }

    internals.pending_conns += 1;
    mem::drop(internals);

    let new_shared = Arc::downgrade(&pool);
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

                let mut locked = shared.internals.lock().await;
                locked.pending_conns -= 1;
                locked.num_conns += 1;
                locked.put_idle_conn(conn);
                return Ok(());
            }
            Err(e) => {
                if Instant::now() - start > pool.statics.connection_timeout {
                    let mut locked = shared.internals.lock().await;
                    locked.pending_conns -= 1;
                    return Err(e);
                } else {
                    delay = max(Duration::from_millis(200), delay);
                    delay = min(pool.statics.connection_timeout / 2, delay * 2);
                    delay_for(delay).await;
                }
            }
        }
    }
}

// Drop connections
// NB: This is called with the pool lock held.
pub(crate) fn drop_connections<'a, M>(
    pool: &Arc<SharedPool<M>>,
    internals: &mut MutexGuard<'a, PoolInternals<M::Connection>>,
    dropped: usize,
) where
    M: ManageConnection,
{
    internals.num_conns -= dropped as u32;
    // We might need to spin up more connections to maintain the idle limit, e.g.
    // if we hit connection lifetime limits
    if internals.num_conns + internals.pending_conns < pool.statics.max_size {
        Pool {
            inner: pool.clone(),
        }
        .spawn_replenishing();
    }
}

pub(crate) fn schedule_reaping<M>(mut interval: Interval, weak_shared: Weak<SharedPool<M>>)
where
    M: ManageConnection,
{
    spawn(async move {
        loop {
            let _ = interval.tick().await;
            if let Some(pool) = weak_shared.upgrade() {
                let mut internals = pool.internals.lock().await;
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
