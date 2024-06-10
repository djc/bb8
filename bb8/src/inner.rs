use std::cmp::{max, min};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::TryFutureExt;
use tokio::spawn;
use tokio::time::{interval_at, sleep, timeout, Interval};

use crate::api::{Builder, ConnectionState, ManageConnection, PooledConnection, RunError, State};
use crate::internals::{
    Approval, ApprovalIter, Conn, SharedPool, StatsConnectionClosedKind, StatsGetKind,
};

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
        let inner = Arc::new(SharedPool::new(builder, manager));

        if inner.statics.max_lifetime.is_some() || inner.statics.idle_timeout.is_some() {
            let start = Instant::now() + inner.statics.reaper_rate;
            let interval = interval_at(start.into(), inner.statics.reaper_rate);
            tokio::spawn(
                Reaper {
                    interval,
                    pool: Arc::downgrade(&inner),
                }
                .run(),
            );
        }

        Self { inner }
    }

    pub(crate) async fn start_connections(&self) -> Result<(), M::Error> {
        let wanted = self.inner.internals.lock().wanted(&self.inner.statics);
        let mut stream = self.replenish_idle_connections(wanted);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    pub(crate) fn spawn_start_connections(&self) {
        let mut locked = self.inner.internals.lock();
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
                    Err(e) => this.inner.forward_error(e),
                }
            }
        });
    }

    fn replenish_idle_connections(
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

    pub(crate) async fn get(&self) -> Result<PooledConnection<'_, M>, RunError<M::Error>> {
        let mut kind = StatsGetKind::Direct;
        let mut wait_time_start = None;

        let future = async {
            loop {
                let (conn, approvals) = self.inner.pop();
                self.spawn_replenishing_approvals(approvals);

                // Cancellation safety: make sure to wrap the connection in a `PooledConnection`
                // before allowing the code to hit an `await`, so we don't lose the connection.

                let mut conn = match conn {
                    Some(conn) => PooledConnection::new(self, conn),
                    None => {
                        wait_time_start = Some(Instant::now());
                        kind = StatsGetKind::Waited;
                        self.inner.notify.notified().await;
                        continue;
                    }
                };

                if !self.inner.statics.test_on_check_out {
                    return Ok(conn);
                }

                match self.inner.manager.is_valid(&mut conn).await {
                    Ok(()) => return Ok(conn),
                    Err(e) => {
                        self.inner.forward_error(e);
                        self.inner
                            .statistics
                            .record_connection_closed(StatsConnectionClosedKind::Invalid);
                        conn.state = ConnectionState::Invalid;
                        continue;
                    }
                }
            }
        };

        let result = match timeout(self.inner.statics.connection_timeout, future).await {
            Ok(result) => result,
            _ => {
                kind = StatsGetKind::TimedOut;
                Err(RunError::TimedOut)
            }
        };

        self.inner.statistics.record_get(kind, wait_time_start);
        result
    }

    pub(crate) async fn connect(&self) -> Result<M::Connection, M::Error> {
        let mut conn = self.inner.manager.connect().await?;
        self.on_acquire_connection(&mut conn).await?;
        Ok(conn)
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, mut conn: Conn<M::Connection>, state: ConnectionState) {
        debug_assert!(
            !matches!(state, ConnectionState::Extracted),
            "handled in caller"
        );

        let mut locked = self.inner.internals.lock();
        match (state, self.inner.manager.has_broken(&mut conn.conn)) {
            (ConnectionState::Present, false) => locked.put(conn, None, self.inner.clone()),
            (_, is_broken) => {
                if is_broken {
                    self.inner
                        .statistics
                        .record_connection_closed(StatsConnectionClosedKind::Broken);
                }
                let approvals = locked.dropped(1, &self.inner.statics);
                self.spawn_replenishing_approvals(approvals);
                self.inner.notify.notify_waiters();
            }
        }
    }

    /// Returns information about the current state of the pool.
    pub(crate) fn state(&self) -> State {
        self.inner
            .internals
            .lock()
            .state((&self.inner.statistics).into())
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
            let conn = shared
                .manager
                .connect()
                .and_then(|mut c| async { self.on_acquire_connection(&mut c).await.map(|_| c) })
                .await;

            match conn {
                Ok(conn) => {
                    let conn = Conn::new(conn);
                    shared
                        .internals
                        .lock()
                        .put(conn, Some(approval), self.inner.clone());
                    return Ok(());
                }
                Err(e) => {
                    if !self.inner.statics.retry_connection
                        || Instant::now() - start > self.inner.statics.connection_timeout
                    {
                        let mut locked = shared.internals.lock();
                        locked.connect_failed(approval);
                        return Err(e);
                    } else {
                        delay = max(Duration::from_millis(200), delay);
                        delay = min(self.inner.statics.connection_timeout / 2, delay * 2);
                        sleep(delay).await;
                    }
                }
            }
        }
    }

    async fn on_acquire_connection(&self, conn: &mut M::Connection) -> Result<(), M::Error> {
        match self.inner.statics.connection_customizer.as_ref() {
            Some(customizer) => customizer.on_acquire(conn).await,
            None => Ok(()),
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

struct Reaper<M: ManageConnection> {
    interval: Interval,
    pool: Weak<SharedPool<M>>,
}

impl<M: ManageConnection> Reaper<M> {
    async fn run(mut self) {
        loop {
            let _ = self.interval.tick().await;
            let pool = match self.pool.upgrade() {
                Some(inner) => PoolInner { inner },
                None => break,
            };

            let approvals = pool.inner.reap();
            pool.spawn_replenishing_approvals(approvals);
        }
    }
}
