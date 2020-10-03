//! Lapin support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use lapin;

/// Basic types to create a `LapinConnectionManager` instance.
pub mod prelude;

use async_trait::async_trait;
use lapin::protocol::{AMQPError, AMQPErrorKind, AMQPHardError};
use lapin::types::ShortString;
use lapin::{ConnectionProperties, ConnectionState};

/// A `bb8::ManageConnection` implementation for `lapin::Connection`s.
///
/// # Example
/// ```no_run
/// use bb8_lapin::prelude::*;
///
/// let manager = LapinConnectionManager::new("amqp://guest:guest@127.0.0.1:5672//", ConnectionProperties::default());
/// let pool = bb8::Pool::builder()
///     .max_size(15)
///     .build(manager)
///     .await
///     .unwrap();
///
/// for _ in 0..20 {
///     let pool = pool.clone();
///     tokio::spawn(async move {
///         let conn = pool.get().await.unwrap();
///         // use the connection
///         // it will be returned to the pool when it falls out of scope.
///     });
/// }
/// ```
#[derive(Debug)]
pub struct LapinConnectionManager {
    amqp_address: String,
    conn_properties: ConnectionProperties,
}

impl LapinConnectionManager {
    /// Initialize the connection manager with the data needed to create new connections.
    /// Refer to the documentation of [`lapin::ConnectionProperties`](https://docs.rs/lapin/1.2.8/lapin/struct.ConnectionProperties.html)
    /// for further details on the available connection parameters.
    ///
    /// # Example
    /// ```
    /// let manager = bb8_lapin::LapinConnectionManager::new("amqp://guest:guest@127.0.0.1:5672//", lapin::ConnectionProperties::default());
    /// ```
    pub fn new(amqp_address: &str, conn_properties: ConnectionProperties) -> Self {
        Self {
            amqp_address: amqp_address.to_string(),
            conn_properties,
        }
    }
}

#[async_trait]
impl bb8::ManageConnection for LapinConnectionManager {
    type Connection = lapin::Connection;
    type Error = lapin::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        lapin::Connection::connect(&self.amqp_address, self.conn_properties.clone()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let valid_states = vec![
            ConnectionState::Initial,
            ConnectionState::Connecting,
            ConnectionState::Connected,
        ];
        if valid_states.contains(&conn.status().state()) {
            Ok(())
        } else {
            Err(lapin::Error::ProtocolError(AMQPError::new(
                AMQPErrorKind::Hard(AMQPHardError::CONNECTIONFORCED),
                ShortString::from("Invalid connection"),
            )))
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        let broken_states = vec![ConnectionState::Closed, ConnectionState::Error];
        broken_states.contains(&conn.status().state())
    }
}
