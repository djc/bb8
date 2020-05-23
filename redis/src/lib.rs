//! Redis support for the `bb8` connection pool.
//!
//! # Example
//!
//! ```
//! use futures::future::join_all;
//! use bb8_redis::{
//!     bb8,
//!     redis::{cmd, AsyncCommands},
//!     RedisConnectionManager
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = RedisConnectionManager::new("redis://localhost").unwrap();
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
pub use redis;

use async_trait::async_trait;
use redis::aio::{Connection, PubSub};
use redis::{Client, IntoConnectionInfo, RedisError};

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
    type Connection = Connection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        redis::cmd("PING").query_async(conn).await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// A `bb8::ManageConnection` for `redis::Client::get_async_connection`.
#[derive(Clone, Debug)]
pub struct RedisPubSubConnectionManager {
    client: Client,
}

impl RedisPubSubConnectionManager {
    /// Create a new `RedisPubSubConnectionManager`.
    /// See `redis::Client::open` for a description of the parameter types.
    pub fn new<T: IntoConnectionInfo>(info: T) -> Result<RedisPubSubConnectionManager, RedisError> {
        Ok(RedisPubSubConnectionManager {
            client: Client::open(info.into_connection_info()?)?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisPubSubConnectionManager {
    type Connection = PubSub;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client
            .get_async_connection()
            .await
            .map(|conn| conn.into_pubsub())
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let test_sub_name = "__test__-sub-from-bb8";
        conn.subscribe(test_sub_name).await?;
        conn.unsubscribe(test_sub_name).await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
