//! Postgres support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub extern crate bb8;
pub extern crate tokio_postgres;

extern crate futures;

use futures::prelude::*;
use tokio_postgres::{Client, Error, Socket};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};

use std::fmt;
use std::io;

/// A `bb8::ManageConnection` for `tokio_postgres::Connection`s.
#[derive(Clone)]
pub struct PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    params: String,
    tls: Tls,
}

impl<Tls> PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    /// Create a new `PostgresConnectionManager`.
    pub fn new<T>(params: T, tls: Tls) -> PostgresConnectionManager<Tls>
    where
        T: ToString,
    {
        PostgresConnectionManager {
            params: params.to_string(),
            tls: tls,
        }
    }
}

impl<Tls> bb8::ManageConnection for PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = Client;
    type Error = Error;

    fn connect(
        &self,
    ) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send + 'static> {
        Box::new(tokio_postgres::connect(&self.params, self.tls.clone()).map(
            |(client, connection)| {
                // The connection object performs the actual communication with the database,
                // so spawn it off to run on its own.
                tokio::spawn(connection.map_err(|_| panic!()));

                client
            },
        ))
    }

    fn is_valid(
        &self,
        mut conn: Self::Connection,
    ) -> Box<Future<Item = Self::Connection, Error = (Self::Error, Self::Connection)> + Send> {
        let f = conn.simple_query("")
            .collect();
        Box::new(f.then(move |r| match r {
            Ok(_) => Ok(conn),
            Err(e) => Err((e, conn)),
        }))
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_closed()
    }

    fn timed_out(&self) -> Self::Error {
        tokio_postgres::Error::connect(io::ErrorKind::TimedOut.into())
    }
}

impl<Tls> fmt::Debug for PostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PostgresConnectionManager")
            .field("params", &self.params)
            .finish()
    }
}
