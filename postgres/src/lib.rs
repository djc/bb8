//! Postgres support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use tokio_postgres;

use async_trait::async_trait;
use futures::prelude::*;
use tokio_postgres::config::Config;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Client, Error, Socket};

use std::fmt;
use std::str::FromStr;

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
#[derive(Clone)]
pub struct PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
}

impl<Tls> PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresConnectionManager` with the specified `config`.
    pub fn new(config: Config, tls: Tls) -> PostgresConnectionManager<Tls> {
        PostgresConnectionManager { config, tls }
    }

    /// Create a new `PostgresConnectionManager`, parsing the config from `params`.
    pub fn new_from_stringlike<T>(
        params: T,
        tls: Tls,
    ) -> Result<PostgresConnectionManager<Tls>, Error>
    where
        T: ToString,
    {
        let stringified_params = params.to_string();
        let config = Config::from_str(&stringified_params)?;
        Ok(Self::new(config, tls))
    }
}

#[async_trait]
impl<Tls> bb8::ManageConnection for PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.config
            .connect(self.tls.clone())
            .await
            .map(|(client, connection)| {
                // The connection object performs the actual communication with the database,
                // so spawn it off to run on its own.
                tokio::spawn(connection.map(|_| ()));

                client
            })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("").await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }
}

impl<Tls> fmt::Debug for PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("config", &self.config)
            .finish()
    }
}
