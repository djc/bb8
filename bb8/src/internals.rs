use std::cmp::min;
use std::time::Instant;

use futures_channel::oneshot;

use crate::api::{Builder, ManageConnection};
use std::collections::VecDeque;

/// The pool data that must be protected by a lock.
#[allow(missing_debug_implementations)]
pub(crate) struct PoolInternals<M>
where
    M: ManageConnection,
{
    waiters: VecDeque<oneshot::Sender<Conn<M::Connection>>>,
    conns: VecDeque<IdleConn<M::Connection>>,
    num_conns: u32,
    pending_conns: u32,
}

impl<M> PoolInternals<M>
where
    M: ManageConnection,
{
    pub(crate) fn pop(&mut self) -> Option<Conn<M::Connection>> {
        self.conns.pop_front().map(|idle| idle.conn)
    }

    pub(crate) fn put(&mut self, conn: Conn<M::Connection>, approval: Option<Approval>) {
        let mut conn = IdleConn::from(conn);
        if approval.is_some() {
            self.pending_conns -= 1;
            self.num_conns += 1;
        }

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

    pub(crate) fn connect_failed(&mut self, _: Approval) {
        self.pending_conns -= 1;
    }

    pub(crate) fn dropped(&mut self, num: u32) {
        self.num_conns -= num;
    }

    pub(crate) fn wanted(&mut self, config: &Builder<M>) -> ApprovalIter {
        let available = self.conns.len() as u32 + self.pending_conns;
        let min_idle = config.min_idle.unwrap_or(0);
        let wanted = if available < min_idle {
            min_idle - available
        } else {
            0
        };

        self.approvals(&config, wanted)
    }

    pub(crate) fn push_waiter(
        &mut self,
        waiter: oneshot::Sender<Conn<M::Connection>>,
        config: &Builder<M>,
    ) -> ApprovalIter {
        self.waiters.push_back(waiter);
        self.approvals(config, 1)
    }

    pub(crate) fn approvals(&mut self, config: &Builder<M>, num: u32) -> ApprovalIter {
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

    pub(crate) fn reap(&mut self, config: &Builder<M>) {
        let now = Instant::now();
        let before = self.conns.len();

        self.conns.retain(|conn| {
            let mut keep = true;
            if let Some(timeout) = config.idle_timeout {
                keep &= now - conn.idle_start < timeout;
            }
            if let Some(lifetime) = config.max_lifetime {
                keep &= now - conn.conn.birth < lifetime;
            }
            keep
        });

        self.dropped((before - self.conns.len()) as u32);
    }

    pub(crate) fn state(&self) -> State {
        State {
            connections: self.num_conns,
            idle_connections: self.conns.len() as u32,
        }
    }
}

impl<M> Default for PoolInternals<M>
where
    M: ManageConnection,
{
    fn default() -> Self {
        Self {
            waiters: VecDeque::new(),
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
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

#[derive(Debug)]
pub(crate) struct Conn<C>
where
    C: Send,
{
    pub(crate) conn: C,
    birth: Instant,
}

impl<C: Send> Conn<C> {
    pub(crate) fn new(conn: C) -> Self {
        Self {
            conn,
            birth: Instant::now(),
        }
    }
}

impl<C: Send> From<IdleConn<C>> for Conn<C> {
    fn from(conn: IdleConn<C>) -> Self {
        conn.conn
    }
}

struct IdleConn<C>
where
    C: Send,
{
    conn: Conn<C>,
    idle_start: Instant,
}

impl<C: Send> From<Conn<C>> for IdleConn<C> {
    fn from(conn: Conn<C>) -> Self {
        IdleConn {
            conn,
            idle_start: Instant::now(),
        }
    }
}

/// Information about the state of a `Pool`.
#[derive(Debug)]
#[non_exhaustive]
pub struct State {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
}
