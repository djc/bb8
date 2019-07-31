extern crate bb8;
extern crate bb8_postgres;
extern crate futures;
extern crate futures_state_stream;
extern crate tokio;
extern crate tokio_postgres;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::{
    future::{err, lazy, Either},
    Future, Stream,
};

use std::str::FromStr;

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
fn main() {
    let config = tokio_postgres::config::Config::from_str("postgresql://postgres:mysecretpassword@localhost:5432").unwrap();
    let pg_mgr = PostgresConnectionManager::new(
        config,
        tokio_postgres::NoTls,
    );

    tokio::run(lazy(|| {
        Pool::builder()
            .build(pg_mgr)
            .map_err(|e| bb8::RunError::User(e))
            .and_then(|pool| {
                pool.run(|mut connection| {
                    connection.prepare("SELECT 1").then(move |r| match r {
                        Ok(select) => {
                            let f = connection
                                .query(&select, &[])
                                .for_each(|row| {
                                    println!("result: {}", row.get::<usize, i32>(0));
                                    Ok(())
                                })
                                .then(move |r| match r {
                                    Ok(_) => Ok(((), connection)),
                                    Err(e) => Err((e, connection)),
                                });
                            Either::A(f)
                        }
                        Err(e) => Either::B(err((e, connection))),
                    })
                })
            })
            .map_err(|e| panic!("{:?}", e))
    }));
}
