//! Redis support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use redis;

use futures::future::{Future, FutureExt};

use async_trait::async_trait;
use redis::aio::Connection;
use redis::{Client, RedisError};

/// `RedisPool` is a convenience wrapper around `bb8::Pool` that hides the fact that
/// `RedisConnectionManager` uses an `Option<Connection>` to smooth over the API incompatibility.
#[derive(Debug)]
pub struct RedisPool {
    pool: bb8::Pool<RedisConnectionManager>,
}

impl RedisPool {
    /// Constructs a new `RedisPool`, see the `bb8::Builder` documentation for description of
    /// parameters.
    pub fn new(pool: bb8::Pool<RedisConnectionManager>) -> RedisPool {
        RedisPool { pool }
    }

    /// Access the `bb8::Pool` directly.
    pub fn pool(&self) -> &bb8::Pool<RedisConnectionManager> {
        &self.pool
    }

    /// Run the function with a connection provided by the pool.
    pub async fn run<'a, T, E, U, F>(&self, f: F) -> Result<T, bb8::RunError<E>>
    where
        F: FnOnce(Connection) -> U + Send + 'a,
        U: Future<Output = Result<(Connection, T), E>> + Send + 'a,
        E: From<<RedisConnectionManager as bb8::ManageConnection>::Error> + Send + 'a,
        T: Send + 'a,
    {
        let f = move |conn: Option<Connection>| {
            let conn = conn.unwrap();
            f(conn).map(|res| match res {
                Ok((conn, item)) => Ok((item, Some(conn))),
                Err(err) => Err((err, None)),
            })
        };
        self.pool.run(f).await
    }
}

/// A `bb8::ManageConnection` for `redis::async::Connection`s.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    pub fn new(client: Client) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager { client })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = Option<Connection>;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match self.client.get_async_connection().await {
            Ok(conn) => Ok(Some(conn)),
            Err(e) => Err(e),
        }
    }

    async fn is_valid(
        &self,
        mut conn: Self::Connection,
    ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        // The connection should only be None after a failure.
        match redis::cmd("PING").query_async(conn.as_mut().unwrap()).await {
            Ok(()) => Ok(conn),
            Err(e) => Err((e, None)),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_none()
    }
}
