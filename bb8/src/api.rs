use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use async_trait::async_trait;

use crate::inner::PoolInner;
use crate::internals::Conn;
pub use crate::internals::State;

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

    /// Returns information about the current state of the pool.
    pub fn state(&self) -> State {
        self.inner.state()
    }

    /// Retrieves a connection from the pool.
    pub async fn get(&self) -> Result<PooledConnection<'_, M>, RunError<M::Error>> {
        self.inner.get().await
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
    /// The error sink.
    pub(crate) error_sink: Box<dyn ErrorSink<M::Error>>,
    /// The time interval used to wake up and reap connections.
    pub(crate) reaper_rate: Duration,
    /// User-supplied trait object responsible for initializing and cleaning-up connections
    pub(crate) connection_customizer: Box<dyn CustomizeConnection<M::Connection, M::Error>>,
    _p: PhantomData<M>,
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
            error_sink: Box::new(NopErrorSink),
            reaper_rate: Duration::from_secs(30),
            connection_customizer: Box::new(NopConnectionCustomizer {}),
            _p: PhantomData,
        }
    }
}

impl<M: ManageConnection> Builder<M> {
    /// Constructs a new `Builder`.
    ///
    /// Parameters are initialized with their default values.
    pub fn new() -> Builder<M> {
        Default::default()
    }

    /// Sets the maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    pub fn max_size(mut self, max_size: u32) -> Builder<M> {
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
    pub fn min_idle(mut self, min_idle: Option<u32>) -> Builder<M> {
        self.min_idle = min_idle;
        self
    }

    /// If true, the health of a connection will be verified through a call to
    /// `ManageConnection::is_valid` before it is provided to a pool user.
    ///
    /// Defaults to true.
    pub fn test_on_check_out(mut self, test_on_check_out: bool) -> Builder<M> {
        self.test_on_check_out = test_on_check_out;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reachs its maximum lifetime while checked out it will be
    /// closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Builder<M> {
        assert!(
            max_lifetime != Some(Duration::from_secs(0)),
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
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder<M> {
        assert!(
            idle_timeout != Some(Duration::from_secs(0)),
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
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder<M> {
        assert!(
            connection_timeout > Duration::from_secs(0),
            "connection_timeout must be non-zero"
        );
        self.connection_timeout = connection_timeout;
        self
    }

    /// Set the sink for errors that are not associated with any particular operation
    /// on the pool. This can be used to log and monitor failures.
    ///
    /// Defaults to `NopErrorSink`.
    pub fn error_sink(mut self, error_sink: Box<dyn ErrorSink<M::Error>>) -> Builder<M> {
        self.error_sink = error_sink;
        self
    }

    /// Used by tests
    #[allow(dead_code)]
    pub fn reaper_rate(mut self, reaper_rate: Duration) -> Builder<M> {
        self.reaper_rate = reaper_rate;
        self
    }

    /// Set the connection customizer which will be used to initialize and cleanup
    /// connections created and destroyed by the pool.
    ///
    /// Defaults to `NopConnectionCustomizer`.
    pub fn connection_customizer(
        mut self,
        connection_customizer: Box<dyn CustomizeConnection<M::Connection, M::Error>>,
    ) -> Builder<M> {
        self.connection_customizer = connection_customizer;
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
    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error>;
    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
}

/// A trait which provides functionality to initialize and cleanup a connection
#[async_trait]
pub trait CustomizeConnection<C: Send + 'static, E: 'static>:
    std::fmt::Debug + Send + Sync + 'static
{
    /// Called with connections immediately after they are returned from
    /// `ManageConnection::connect`.
    ///
    /// The default implementation simply returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// If this method returns an error, the connection will be discarded.
    #[allow(unused_variables)]
    async fn on_acquire(&self, connection: &mut C) -> Result<(), E> {
        Ok(())
    }

    /// Called with connections when they are removed from the pool.
    ///
    /// The connections may be broken (as reported by `ManageConnection::is_valid` or
    /// `ManageConnection::has_broken`), or have simply timed out.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn on_release(&self, connection: &mut C) {}
}

#[derive(Copy, Clone, Debug)]
struct NopConnectionCustomizer;
impl<C: Send + 'static, E: 'static> CustomizeConnection<C, E> for NopConnectionCustomizer {}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<'a, M>
where
    M: ManageConnection,
{
    pool: &'a PoolInner<M>,
    conn: Option<Conn<M::Connection>>,
}

impl<'a, M> PooledConnection<'a, M>
where
    M: ManageConnection,
{
    pub(crate) fn new(pool: &'a PoolInner<M>, conn: Conn<M::Connection>) -> Self {
        Self {
            pool,
            conn: Some(conn),
        }
    }

    pub(crate) fn drop_invalid(mut self) {
        let _ = self.conn.take();
    }
}

impl<'a, M> Deref for PooledConnection<'a, M>
where
    M: ManageConnection,
{
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
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
        self.pool.put_back(self.conn.take());
    }
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
            RunError::User(ref err) => write!(f, "{}", err),
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
