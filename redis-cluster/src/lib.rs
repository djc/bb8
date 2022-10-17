//! Redis Cluster support for the `bb8` connection pool.
//!
//! # Example
//!
//! ```
//! use futures_util::future::join_all;
//! use bb8_redis_cluster::{
//!     bb8,
//!     redis_cluster_async::redis::{cmd, AsyncCommands},
//!     RedisConnectionManager
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = RedisConnectionManager::new(vec!["redis://localhost"]).unwrap();
//!     let pool = bb8::Pool::builder().build(manager).await.unwrap();
//!
//!     let mut handles = vec![];
//!
//!     for _i in 0..10 {
//!         let pool = pool.clone();
//!
//!         handles.push(tokio::spawn(async move {
//!             let mut conn = pool.get().await.unwrap();
//!
//!             let reply: String = cmd("PING").query_async(&mut *conn).await.unwrap();
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
pub use redis_cluster_async;

use async_trait::async_trait;
use redis_cluster_async::{
    redis::{ErrorKind, IntoConnectionInfo, RedisError},
    Client, Connection,
};

/// A `bb8::ManageConnection` for `redis_cluster_async::Client::get_connection`.
#[derive(Clone)]
pub struct RedisConnectionManager {
    client: Client,
}

// Because redis_cluster_async::Client does not support Debug derive.
impl std::fmt::Debug for RedisConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnectionManager")
            .field("client", &format!("pointer({:p})", &self.client))
            .finish()
    }
}

impl RedisConnectionManager {
    /// Create a new `RedisConnectionManager`.
    /// See `redis_cluster_async::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(infos: Vec<T>) -> Result<RedisConnectionManager, RedisError> {
        Ok(RedisConnectionManager {
            client: Client::open(infos.into_iter().collect())?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisConnectionManager {
    type Connection = Connection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
