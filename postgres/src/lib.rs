//! Postgres support for the `bb8` connection pool.
#![deny(missing_docs)]

pub extern crate bb8;
pub extern crate tokio_postgres;

extern crate futures;
extern crate postgres_shared;
extern crate tokio_core;

use futures::Future;
use tokio_core::reactor::Handle;
use tokio_postgres::{Connection, Error};
use tokio_postgres::params::{ConnectParams, IntoConnectParams};
use tokio_postgres::tls::Handshake;

use std::fmt;
use std::io;

type Result<T> = std::result::Result<T, Error>;

/// Like `tokio_postgres::TlsMode` except that it owns its `Handshake` instance.
pub enum TlsMode {
    /// Like `postgres::TlsMode::None`.
    None,
    /// Like `postgres::TlsMode::Prefer`.
    Prefer(Box<Handshake + Sync + Send>),
    /// Like `postgres::TlsMode::Require`.
    Require(Box<Handshake + Sync + Send>),
}

impl fmt::Debug for TlsMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            TlsMode::None => &"TlsMode::None",
            TlsMode::Prefer(_) => &"TlsMode::Prefer",
            TlsMode::Require(_) => &"TlsMode::Require",
        })
    }
}

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
#[derive(Debug)]
pub struct PostgresConnectionManager {
    params: ConnectParams,
    tls_mode: TlsMode,
}

impl PostgresConnectionManager {
    /// Create a new `PostgresConnectionManager`.
    pub fn new<T>(params: T, tls_mode: TlsMode) -> Result<PostgresConnectionManager>
        where T: IntoConnectParams
    {
        Ok(PostgresConnectionManager {
            params: params.into_connect_params()
                .map_err(postgres_shared::error::connect)?,
            tls_mode: tls_mode,
        })
    }
}

impl bb8::ManageConnection for PostgresConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect<'a>(&'a self,
                   handle: Handle)
                   -> Box<Future<Item = Self::Connection, Error = Self::Error> + 'a> {
        let tls_mode = match self.tls_mode {
            TlsMode::None => tokio_postgres::TlsMode::None,
            TlsMode::Prefer(ref n) => tokio_postgres::TlsMode::Prefer(&**n),
            TlsMode::Require(ref n) => tokio_postgres::TlsMode::Require(&**n),
        };
        Connection::connect(self.params.clone(), tls_mode, &handle)
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
