use std::borrow::Cow;
use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use async_trait::async_trait;

use crate::inner::PoolInner;
use crate::internals::Conn;

/// A generic connection pool.
pub struct Pool<M>
where
    M: ManageConnection,
{
    pub(crate) inner: PoolInner<M>,
}

impl<M> Clone for Pool<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

impl<M> fmt::Debug for Pool<M>
where
    M: ManageConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("Pool({:?})", self.inner))
    }
}

impl<M: ManageConnection> Pool<M> {
    /// Returns a `Builder` instance to configure a new pool.
    pub fn builder() -> Builder<M> {
        Builder::new()
    }

    /// Retrieves a connection from the pool.
    pub async fn get(&self) -> Result<PooledConnection<'_, M>, RunError<M::Error>> {
        self.inner.get().await
    }

    /// Retrieves an owned connection from the pool
    ///
    /// Using an owning `PooledConnection` makes it easier to leak the connection pool. Therefore, [`Pool::get`]
    /// (which stores a lifetime-bound reference to the pool) should be preferred whenever possible.
    pub async fn get_owned(&self) -> Result<PooledConnection<'static, M>, RunError<M::Error>> {
        Ok(PooledConnection {
            conn: self.get().await?.take(),
            pool: Cow::Owned(self.inner.clone()),
            state: ConnectionState::Present,
        })
    }

    /// Get a new dedicated connection that will not be managed by the pool.
    /// An application may want a persistent connection (e.g. to do a
    /// postgres LISTEN) that will not be closed or repurposed by the pool.
    ///
    /// This method allows reusing the manager's configuration but otherwise
    /// bypassing the pool
    pub async fn dedicated_connection(&self) -> Result<M::Connection, M::Error> {
        self.inner.connect().await
    }

    /// Returns information about the current state of the pool.
    pub fn state(&self) -> State {
        self.inner.state()
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
    /// Statistics about the historical usage of the pool.
    pub statistics: Statistics,
}

/// Statistics about the historical usage of the `Pool`.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct Statistics {
    /// Total gets performed that did not have to wait for a connection.
    pub get_direct: u64,
    /// Total gets performed that had to wait for a connection available.
    pub get_waited: u64,
    /// Total gets performed that timed out while waiting for a connection.
    pub get_timed_out: u64,
    /// Total time accumulated waiting for a connection.
    pub get_wait_time: Duration,
}

/// A builder for a connection pool.
#[derive(Debug)]
pub struct Builder<M: ManageConnection> {
    /// The maximum number of connections allowed.
    pub(crate) max_size: u32,
    /// The minimum idle connection count the pool will attempt to maintain.
    pub(crate) min_idle: Option<u32>,
    /// Whether or not to test the connection on checkout.
    pub(crate) test_on_check_out: bool,
    /// The maximum lifetime, if any, that a connection is allowed.
    pub(crate) max_lifetime: Option<Duration>,
    /// The duration, if any, after which idle_connections in excess of `min_idle` are closed.
    pub(crate) idle_timeout: Option<Duration>,
    /// The duration to wait to start a connection before giving up.
    pub(crate) connection_timeout: Duration,
    /// Enable/disable automatic retries on connection creation.
    pub(crate) retry_connection: bool,
    /// The error sink.
    pub(crate) error_sink: Box<dyn ErrorSink<M::Error>>,
    /// The time interval used to wake up and reap connections.
    pub(crate) reaper_rate: Duration,
    /// Queue strategy (FIFO or LIFO)
    pub(crate) queue_strategy: QueueStrategy,
    /// User-supplied trait object responsible for initializing connections
    pub(crate) connection_customizer: Option<Box<dyn CustomizeConnection<M::Connection, M::Error>>>,
    _p: PhantomData<M>,
}

/// bb8's queue strategy when getting pool resources
#[derive(Debug, Default, Clone, Copy)]
pub enum QueueStrategy {
    /// First in first out
    /// This strategy behaves like a queue
    /// It will evenly spread load on all existing connections, resetting their idle timeouts, maintaining the pool size
    #[default]
    Fifo,
    /// Last in first out
    /// This behaves like a stack
    /// It will use the most recently used connection and help to keep the total pool size small by evicting idle connections
    Lifo,
}

impl<M: ManageConnection> Default for Builder<M> {
    fn default() -> Self {
        Builder {
            max_size: 10,
            min_idle: None,
            test_on_check_out: true,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(30),
            retry_connection: true,
            error_sink: Box::new(NopErrorSink),
            reaper_rate: Duration::from_secs(30),
            queue_strategy: QueueStrategy::default(),
            connection_customizer: None,
            _p: PhantomData,
        }
    }
}

impl<M: ManageConnection> Builder<M> {
    /// Constructs a new `Builder`.
    ///
    /// Parameters are initialized with their default values.
    #[must_use]
    pub fn new() -> Self {
        Builder::default()
    }

    /// Sets the maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    ///
    /// # Panics
    ///
    /// Will panic if `max_size` is 0.
    #[must_use]
    pub fn max_size(mut self, max_size: u32) -> Self {
        assert!(max_size > 0, "max_size must be greater than zero!");
        self.max_size = max_size;
        self
    }

    /// Sets the minimum idle connection count maintained by the pool.
    ///
    /// If set, the pool will try to maintain at least this many idle
    /// connections at all times, while respecting the value of `max_size`.
    ///
    /// Defaults to None.
    #[must_use]
    pub fn min_idle(mut self, min_idle: impl Into<Option<u32>>) -> Self {
        self.min_idle = min_idle.into();
        self
    }

    /// If true, the health of a connection will be verified through a call to
    /// `ManageConnection::is_valid` before it is provided to a pool user.
    ///
    /// Defaults to true.
    #[must_use]
    pub fn test_on_check_out(mut self, test_on_check_out: bool) -> Self {
        self.test_on_check_out = test_on_check_out;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reaches its maximum lifetime while checked out it will be
    /// closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    ///
    /// # Panics
    ///
    /// Will panic if `max_lifetime` is 0.
    #[must_use]
    pub fn max_lifetime(mut self, max_lifetime: impl Into<Option<Duration>>) -> Self {
        let max_lifetime = max_lifetime.into();
        assert_ne!(
            max_lifetime,
            Some(Duration::from_secs(0)),
            "max_lifetime must be greater than zero!"
        );
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout used by the pool.
    ///
    /// If set, idle connections in excess of `min_idle` will be closed at the
    /// next reaping after remaining idle past this duration.
    ///
    /// Defaults to 10 minutes.
    ///
    /// # Panics
    ///
    /// Will panic if `idle_timeout` is 0.
    #[must_use]
    pub fn idle_timeout(mut self, idle_timeout: impl Into<Option<Duration>>) -> Self {
        let idle_timeout = idle_timeout.into();
        assert_ne!(
            idle_timeout,
            Some(Duration::from_secs(0)),
            "idle_timeout must be greater than zero!"
        );
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// Futures returned by `Pool::get` will wait this long before giving up and
    /// resolving with an error.
    ///
    /// Defaults to 30 seconds.
    ///
    /// # Panics
    ///
    /// Will panic if `connection_timeout` is 0.
    #[must_use]
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Self {
        assert!(
            connection_timeout > Duration::from_secs(0),
            "connection_timeout must be non-zero"
        );
        self.connection_timeout = connection_timeout;
        self
    }

    /// Instructs the pool to automatically retry connection creation if it fails, until the `connection_timeout` has expired.
    ///
    /// Useful for transient connectivity errors like temporary DNS resolution failure
    /// or intermittent network failures. Some applications however are smart enough to
    /// know that the server is down and retries won't help (and could actually hurt recovery).
    /// In that case, it's better to disable retries here and let the pool error out.
    ///
    /// Defaults to enabled.
    #[must_use]
    pub fn retry_connection(mut self, retry: bool) -> Self {
        self.retry_connection = retry;
        self
    }

    /// Set the sink for errors that are not associated with any particular operation
    /// on the pool. This can be used to log and monitor failures.
    ///
    /// Defaults to `NopErrorSink`.
    #[must_use]
    pub fn error_sink(mut self, error_sink: Box<dyn ErrorSink<M::Error>>) -> Self {
        self.error_sink = error_sink;
        self
    }

    /// Used by tests
    #[allow(dead_code)]
    #[must_use]
    pub fn reaper_rate(mut self, reaper_rate: Duration) -> Self {
        self.reaper_rate = reaper_rate;
        self
    }

    /// Sets the queue strategy to be used by the pool
    ///
    /// Defaults to `Fifo`.
    #[must_use]
    pub fn queue_strategy(mut self, queue_strategy: QueueStrategy) -> Self {
        self.queue_strategy = queue_strategy;
        self
    }

    /// Set the connection customizer to customize newly checked out connections
    #[must_use]
    pub fn connection_customizer(
        mut self,
        connection_customizer: Box<dyn CustomizeConnection<M::Connection, M::Error>>,
    ) -> Self {
        self.connection_customizer = Some(connection_customizer);
        self
    }

    fn build_inner(self, manager: M) -> Pool<M> {
        if let Some(min_idle) = self.min_idle {
            assert!(
                self.max_size >= min_idle,
                "min_idle must be no larger than max_size"
            );
        }

        Pool {
            inner: PoolInner::new(self, manager),
        }
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    ///
    /// The `Pool` will not be returned until it has established its configured
    /// minimum number of connections, or it times out.
    pub async fn build(self, manager: M) -> Result<Pool<M>, M::Error> {
        let pool = self.build_inner(manager);
        pool.inner.start_connections().await.map(|()| pool)
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    ///
    /// Unlike `build`, this does not wait for any connections to be established
    /// before returning.
    pub fn build_unchecked(self, manager: M) -> Pool<M> {
        let p = self.build_inner(manager);
        p.inner.spawn_start_connections();
        p
    }
}

/// A trait which provides connection-specific functionality.
#[async_trait]
pub trait ManageConnection: Sized + Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;
    /// The error type returned by `Connection`s.
    type Error: fmt::Debug + Send + 'static;

    /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error>;
    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error>;
    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
}

/// A trait which provides functionality to initialize a connection
#[async_trait]
pub trait CustomizeConnection<C: Send + 'static, E: 'static>:
    fmt::Debug + Send + Sync + 'static
{
    /// Called with connections immediately after they are returned from
    /// `ManageConnection::connect`.
    ///
    /// The default implementation simply returns `Ok(())`. If this method returns an
    /// error, it will be forwarded to the configured error sink.
    async fn on_acquire(&self, _connection: &mut C) -> Result<(), E> {
        Ok(())
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<'a, M>
where
    M: ManageConnection,
{
    pool: Cow<'a, PoolInner<M>>,
    conn: Option<Conn<M::Connection>>,
    pub(crate) state: ConnectionState,
}

impl<'a, M> PooledConnection<'a, M>
where
    M: ManageConnection,
{
    pub(crate) fn new(pool: &'a PoolInner<M>, conn: Conn<M::Connection>) -> Self {
        Self {
            pool: Cow::Borrowed(pool),
            conn: Some(conn),
            state: ConnectionState::Present,
        }
    }

    pub(crate) fn take(mut self) -> Option<Conn<M::Connection>> {
        self.state = ConnectionState::Extracted;
        self.conn.take()
    }
}

impl<'a, M> Deref for PooledConnection<'a, M>
where
    M: ManageConnection,
{
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<'a, M> DerefMut for PooledConnection<'a, M>
where
    M: ManageConnection,
{
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<'a, M> fmt::Debug for PooledConnection<'a, M>
where
    M: ManageConnection,
    M::Connection: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.conn.as_ref().unwrap().conn, fmt)
    }
}

impl<'a, M> Drop for PooledConnection<'a, M>
where
    M: ManageConnection,
{
    fn drop(&mut self) {
        if let ConnectionState::Extracted = self.state {
            return;
        }

        debug_assert!(self.conn.is_some(), "incorrect state {:?}", self.state);
        if let Some(conn) = self.conn.take() {
            self.pool.as_ref().put_back(conn, self.state);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ConnectionState {
    Present,
    Extracted,
    Invalid,
}

/// bb8's error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError<E> {
    /// An error returned from user code.
    User(E),
    /// bb8 attempted to get a connection but the provided timeout was exceeded.
    TimedOut,
}

impl<E> fmt::Display for RunError<E>
where
    E: error::Error + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RunError::User(ref err) => write!(f, "{err}"),
            RunError::TimedOut => write!(f, "Timed out in bb8"),
        }
    }
}

impl<E> error::Error for RunError<E>
where
    E: error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            RunError::User(ref err) => Some(err),
            RunError::TimedOut => None,
        }
    }
}

impl<E> From<E> for RunError<E>
where
    E: error::Error,
{
    fn from(error: E) -> Self {
        Self::User(error)
    }
}

/// A trait to receive errors generated by connection management that aren't
/// tied to any particular caller.
pub trait ErrorSink<E>: fmt::Debug + Send + Sync + 'static {
    /// Receive an error
    fn sink(&self, error: E);

    /// Clone this sink.
    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>>;
}

/// An `ErrorSink` implementation that does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NopErrorSink;

impl<E> ErrorSink<E> for NopErrorSink {
    fn sink(&self, _: E) {}

    fn boxed_clone(&self) -> Box<dyn ErrorSink<E>> {
        Box::new(*self)
    }
}
