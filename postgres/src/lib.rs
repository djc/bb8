//! Postgres support for the `bb8` connection pool.
#![deny(missing_docs)]

pub extern crate bb8;
pub extern crate tokio_postgres;

extern crate futures;
extern crate postgres_shared;
extern crate tokio_core;

use futures::Future;
use tokio_core::reactor::Handle;
use tokio_postgres::{Connection, Error, TlsMode};
use tokio_postgres::params::{ConnectParams, IntoConnectParams};
use tokio_postgres::tls::Handshake;

use std::fmt;
use std::io;

type Result<T> = std::result::Result<T, Error>;

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
pub struct PostgresConnectionManager {
    params: ConnectParams,
    tls_mode: Box<Fn() -> TlsMode + Send + Sync>,
}

impl PostgresConnectionManager {
    /// Create a new `PostgresConnectionManager`.
    pub fn new<F, T>(params: T, tls_mode: F) -> Result<PostgresConnectionManager>
    where
        T: IntoConnectParams,
        F: Fn() -> TlsMode + Send + Sync + 'static,
    {
        Ok(PostgresConnectionManager {
            params: params.into_connect_params()
                .map_err(postgres_shared::error::connect)?,
            tls_mode: Box::new(tls_mode),
        })
    }
}

impl bb8::ManageConnection for PostgresConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect<'a>(
        &'a self,
        handle: Handle,
    ) -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'a> {
        Connection::connect(self.params.clone(), (self.tls_mode)(), &handle)
    }

    fn is_valid
        (&self,
         conn: Self::Connection)
         -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)>> {
        conn.batch_execute("")
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_desynchronized()
    }

    fn timed_out(&self) -> Self::Error {
        postgres_shared::error::io(io::ErrorKind::TimedOut.into())
    }
}
