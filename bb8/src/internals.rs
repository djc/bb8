use std::cmp::min;
use std::collections::VecDeque;
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(not(target_has_atomic = "64"))]
use portable_atomic::AtomicU64;
use tokio::sync::Notify;

use crate::api::{Builder, ManageConnection, QueueStrategy, State, Statistics};
use crate::lock::Mutex;

/// The guts of a `Pool`.
#[allow(missing_debug_implementations)]
pub(crate) struct SharedPool<M: ManageConnection + Send> {
    pub(crate) statics: Builder<M>,
    pub(crate) manager: M,
    pub(crate) internals: Mutex<PoolInternals<M>>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) statistics: AtomicStatistics,
}

impl<M: ManageConnection + Send> SharedPool<M> {
    pub(crate) fn new(statics: Builder<M>, manager: M) -> Self {
        Self {
            statics,
            manager,
            internals: Mutex::new(PoolInternals::default()),
            notify: Arc::new(Notify::new()),
            statistics: AtomicStatistics::default(),
        }
    }

    pub(crate) fn try_put(self: &Arc<Self>, conn: M::Connection) -> Result<(), M::Connection> {
        let mut locked = self.internals.lock();
        let mut approvals = locked.approvals(&self.statics, 1);
        let Some(approval) = approvals.next() else {
            return Err(conn);
        };
        let conn = Conn::new(conn);
        locked.put(conn, Some(approval), self.clone());
        Ok(())
    }

    pub(crate) fn reap(&self) -> ApprovalIter {
        let mut locked = self.internals.lock();
        let (iter, closed_idle_timeout, closed_max_lifetime) = locked.reap(&self.statics);
        drop(locked);
        self.statistics
            .record_connections_reaped(closed_idle_timeout, closed_max_lifetime);
        iter
    }

    pub(crate) fn start_get(self: &Arc<Self>) -> Getting<M> {
        Getting::new(self.clone())
    }

    pub(crate) fn forward_error(&self, err: M::Error) {
        self.statics.error_sink.sink(err);
    }
}

/// The pool data that must be protected by a lock.
#[allow(missing_debug_implementations)]
pub(crate) struct PoolInternals<M: ManageConnection> {
    conns: VecDeque<IdleConn<M::Connection>>,
    num_conns: u32,
    pending_conns: u32,
    in_flight: u32,
}

impl<M: ManageConnection> PoolInternals<M> {
    pub(crate) fn put(
        &mut self,
        conn: Conn<M::Connection>,
        approval: Option<Approval>,
        pool: Arc<SharedPool<M>>,
    ) {
        if approval.is_some() {
            #[cfg(debug_assertions)]
            {
                self.pending_conns -= 1;
                self.num_conns += 1;
            }
            #[cfg(not(debug_assertions))]
            {
                self.pending_conns = self.pending_conns.saturating_sub(1);
                self.num_conns = self.num_conns.saturating_add(1);
            }
        }

        // Queue it in the idle queue
        let conn = IdleConn::from(conn);
        match pool.statics.queue_strategy {
            QueueStrategy::Fifo => self.conns.push_back(conn),
            QueueStrategy::Lifo => self.conns.push_front(conn),
        }

        pool.notify.notify_one();
    }

    pub(crate) fn connect_failed(&mut self, _: Approval) {
        #[cfg(debug_assertions)]
        {
            self.pending_conns -= 1;
        }
        #[cfg(not(debug_assertions))]
        {
            self.pending_conns = self.pending_conns.saturating_sub(1);
        }
    }

    pub(crate) fn dropped(&mut self, num: u32, config: &Builder<M>) -> ApprovalIter {
        #[cfg(debug_assertions)]
        {
            self.num_conns -= num;
        }
        #[cfg(not(debug_assertions))]
        {
            self.num_conns = self.num_conns.saturating_sub(num);
        }

        self.wanted(config)
    }

    pub(crate) fn wanted(&mut self, config: &Builder<M>) -> ApprovalIter {
        let available = self.conns.len() as u32 + self.pending_conns;
        let min_idle = config.min_idle.unwrap_or(0);
        let wanted = min_idle.saturating_sub(available);
        self.approvals(config, wanted)
    }

    fn approvals(&mut self, config: &Builder<M>, num: u32) -> ApprovalIter {
        let current = self.num_conns + self.pending_conns;
        let num = min(num, config.max_size.saturating_sub(current));
        self.pending_conns += num;
        ApprovalIter { num: num as usize }
    }

    pub(crate) fn reap(&mut self, config: &Builder<M>) -> (ApprovalIter, u64, u64) {
        let mut closed_max_lifetime = 0;
        let mut closed_idle_timeout = 0;
        let now = Instant::now();
        let before = self.conns.len();

        self.conns.retain(|conn| {
            let mut keep = true;
            if let Some(timeout) = config.idle_timeout {
                if now - conn.idle_start >= timeout {
                    closed_idle_timeout += 1;
                    keep &= false;
                }
            }
            if let Some(lifetime) = config.max_lifetime {
                if conn.conn.is_expired(now, lifetime) {
                    closed_max_lifetime += 1;
                    keep &= false;
                }
            }
            keep
        });

        (
            self.dropped((before - self.conns.len()) as u32, config),
            closed_idle_timeout,
            closed_max_lifetime,
        )
    }

    pub(crate) fn state(&self, statistics: Statistics) -> State {
        State {
            connections: self.num_conns,
            idle_connections: self.conns.len() as u32,
            statistics,
        }
    }
}

impl<M: ManageConnection> Default for PoolInternals<M> {
    fn default() -> Self {
        Self {
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
            in_flight: 0,
        }
    }
}

#[must_use]
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

#[must_use]
pub(crate) struct Approval {
    _priv: (),
}

pub(crate) struct Getting<M: ManageConnection + Send> {
    inner: Arc<SharedPool<M>>,
}

impl<M: ManageConnection + Send> Getting<M> {
    pub(crate) fn get(&self) -> (Option<Conn<M::Connection>>, ApprovalIter) {
        let mut locked = self.inner.internals.lock();
        if let Some(IdleConn { conn, .. }) = locked.conns.pop_front() {
            return (Some(conn), locked.wanted(&self.inner.statics));
        }

        let approvals = match locked.in_flight > locked.pending_conns {
            true => 1,
            false => 0,
        };

        (None, locked.approvals(&self.inner.statics, approvals))
    }
}

impl<M: ManageConnection + Send> Getting<M> {
    fn new(inner: Arc<SharedPool<M>>) -> Self {
        {
            let mut locked = inner.internals.lock();
            locked.in_flight += 1;
        }
        Getting { inner }
    }
}

impl<M: ManageConnection + Send> Drop for Getting<M> {
    fn drop(&mut self) {
        let mut locked = self.inner.internals.lock();
        locked.in_flight -= 1;
    }
}

#[derive(Debug)]
pub(crate) struct Conn<C: Send> {
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

    pub(crate) fn is_expired(&self, now: Instant, max: Duration) -> bool {
        // The current age of the connection is longer than the maximum allowed lifetime
        now - self.birth >= max
    }
}

impl<C: Send> From<IdleConn<C>> for Conn<C> {
    fn from(conn: IdleConn<C>) -> Self {
        conn.conn
    }
}

struct IdleConn<C: Send> {
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

#[derive(Default)]
pub(crate) struct AtomicStatistics {
    pub(crate) get_started: AtomicU64,
    pub(crate) get_direct: AtomicU64,
    pub(crate) get_waited: AtomicU64,
    pub(crate) get_timed_out: AtomicU64,
    pub(crate) get_wait_time_micros: AtomicU64,
    pub(crate) connections_created: AtomicU64,
    pub(crate) connections_closed_broken: AtomicU64,
    pub(crate) connections_closed_invalid: AtomicU64,
    pub(crate) connections_closed_max_lifetime: AtomicU64,
    pub(crate) connections_closed_idle_timeout: AtomicU64,
}

impl AtomicStatistics {
    pub(crate) fn record_get_started(&self) {
        self.get_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get(&self, kind: StatsGetKind, wait_time_start: Option<Instant>) {
        match kind {
            StatsGetKind::Direct => self.get_direct.fetch_add(1, Ordering::Relaxed),
            StatsGetKind::Waited => self.get_waited.fetch_add(1, Ordering::Relaxed),
            StatsGetKind::TimedOut => self.get_timed_out.fetch_add(1, Ordering::Relaxed),
        };

        if let Some(wait_time_start) = wait_time_start {
            let wait_time = Instant::now() - wait_time_start;
            self.get_wait_time_micros
                .fetch_add(wait_time.as_micros() as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record(&self, kind: StatsKind) {
        match kind {
            StatsKind::Created => &self.connections_created,
            StatsKind::ClosedBroken => &self.connections_closed_broken,
            StatsKind::ClosedInvalid => &self.connections_closed_invalid,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_connections_reaped(
        &self,
        closed_idle_timeout: u64,
        closed_max_lifetime: u64,
    ) {
        self.connections_closed_idle_timeout
            .fetch_add(closed_idle_timeout, Ordering::Relaxed);
        self.connections_closed_max_lifetime
            .fetch_add(closed_max_lifetime, Ordering::Relaxed);
    }
}

impl From<&AtomicStatistics> for Statistics {
    fn from(item: &AtomicStatistics) -> Self {
        Self {
            get_started: item.get_started.load(Ordering::Relaxed),
            get_direct: item.get_direct.load(Ordering::Relaxed),
            get_waited: item.get_waited.load(Ordering::Relaxed),
            get_timed_out: item.get_timed_out.load(Ordering::Relaxed),
            get_wait_time: Duration::from_micros(item.get_wait_time_micros.load(Ordering::Relaxed)),
            connections_created: item.connections_created.load(Ordering::Relaxed),
            connections_closed_broken: item.connections_closed_broken.load(Ordering::Relaxed),
            connections_closed_invalid: item.connections_closed_invalid.load(Ordering::Relaxed),
            connections_closed_max_lifetime: item
                .connections_closed_max_lifetime
                .load(Ordering::Relaxed),
            connections_closed_idle_timeout: item
                .connections_closed_idle_timeout
                .load(Ordering::Relaxed),
        }
    }
}

pub(crate) enum StatsGetKind {
    Direct,
    Waited,
    TimedOut,
}

pub(crate) enum StatsKind {
    Created,
    ClosedBroken,
    ClosedInvalid,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::internals::Conn;

    #[test]
    fn test_conn_is_expired() {
        let conn = Conn {
            conn: (),
            birth: Instant::now(),
        };

        assert!(
            !conn.is_expired(conn.birth, Duration::from_nanos(1)),
            "at birth, the connection has not expired"
        );
        assert!(
            !conn.is_expired(Instant::now(), Duration::from_secs(5)),
            "from setup to now is shorter than 5s, so the connection has not expired"
        );
        assert!(
            conn.is_expired(Instant::now(), Duration::from_nanos(1)),
            "from setup to now is longer than 1ns, so the connection has expired"
        );
    }
}
