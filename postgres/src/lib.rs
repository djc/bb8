//! Postgres support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use tokio_postgres;

use async_trait::async_trait;
use tokio_postgres::config::Config;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Client, Error, Socket};

use std::str::FromStr;
use std::{error, fmt};

/// An error type for the postgres connection manager builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuilderError(String);

impl fmt::Display for BuilderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Builder error: {}", &self)
    }
}

impl error::Error for BuilderError {}

/// A builder for a postgres connection manager.
#[derive(Debug)]
pub struct PostgresConnectionManagerBuilder<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Option<Config>,
    validation_query: String,
    tls: Option<Tls>,
}

impl<Tls> PostgresConnectionManagerBuilder<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            config: None,
            validation_query: "".into(),
            tls: None,
        }
    }

    /// Set the postgres configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the validation query used by [`bb8::ManageConnection::is_valid`].
    pub fn validation_query(mut self, query: String) -> Self {
        self.validation_query = query;
        self
    }

    /// Set the TLS configuraiton.
    pub fn tls(mut self, tls: Tls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Build a postgres connection manager.
    pub fn build(self) -> Result<PostgresConnectionManager<Tls>, Box<dyn std::error::Error>> {
        Ok(PostgresConnectionManager {
            tls: self.tls.ok_or_else(|| {
                BuilderError("the tls config was set during the build process".into())
            })?,
            config: self.config.ok_or_else(|| {
                BuilderError("the config was set during the build process".into())
            })?,
            validation_query: self.validation_query,
        })
    }
}

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
#[derive(Clone)]
pub struct PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    config: Config,
    tls: Tls,
    validation_query: String,
}

impl<Tls> PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresConnectionManager` with the specified `config`.
    pub fn new(config: Config, tls: Tls) -> PostgresConnectionManager<Tls> {
        PostgresConnectionManager {
            config,
            tls,
            validation_query: "".into(),
        }
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
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move { connection.await.map(|_| ()) });
        Ok(client)
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        conn.simple_query(&self.validation_query).await?;
        Ok(())
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
