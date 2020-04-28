//! Redis support for the `bb8` connection pool.
//!
//! # Example
//!
//! ```
//! use futures::future::join_all;
//! use bb8_redis::{
//!     bb8,
//!     redis::{cmd, AsyncCommands},
//!     RedisConnectionManager, RedisPool
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = RedisConnectionManager::new("redis://localhost").unwrap();
//!     let pool = RedisPool::new(bb8::Pool::builder().build(manager).await.unwrap());
//!
//!     let mut handles = vec![];
//!
//!     for _i in 0..10 {
//!         let pool = pool.clone();
//!
//!         handles.push(tokio::spawn(async move {
//!             let mut conn = pool.get().await.unwrap();
//!             let conn = conn.as_mut().unwrap();
//!
//!             let reply: String = cmd("PING").query_async(conn).await.unwrap();
//!
//!             assert_eq!("PONG", reply);
//!         }));
//!     }
//!
//!     join_all(handles).await;
//! }
//! ```
#![allow(clippy::needless_doctest_main)]
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use redis;

use futures::future::{Future, FutureExt};

use async_trait::async_trait;
use redis::aio::Connection;
use redis::{Client, IntoConnectionInfo, RedisError};

/// `RedisPool` is a convenience wrapper around `bb8::Pool` that hides the fact that
/// `RedisConnectionManager` uses an `Option<Connection>` to smooth over the API incompatibility.
#[derive(Debug, Clone)]
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

    /// Retrieve the pooled connection
    pub async fn get(
        &self,
    ) -> Result<bb8::PooledConnection<'_, RedisConnectionManager>, bb8::RunError<RedisError>> {
        self.pool().get().await
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

/// A `bb8::ManageConnection` for `redis::Client::get_async_connection`.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    /// See `redis::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(info: T) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager {
            client: Client::open(info.into_connection_info()?)?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = Option<Connection>;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await.map(Some)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // The connection should only be None after a failure.
        redis::cmd("PING")
            .query_async(conn.as_mut().unwrap())
            .await
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_none()
    }
}
