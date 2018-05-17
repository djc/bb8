extern crate bb8;
extern crate bb8_postgres;
extern crate futures;
extern crate futures_state_stream;
extern crate tokio_core;
extern crate tokio_postgres;

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::Future;
use futures_state_stream::StateStream;
use tokio_core::reactor::Core;

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
fn main() {
    let mut core = Core::new().unwrap();

    let pg_mgr = PostgresConnectionManager::new(
        "postgresql://postgres:mysecretpassword@localhost:5432",
        || tokio_postgres::TlsMode::None,
    ).unwrap();

    let f = Pool::builder()
        .build(pg_mgr, core.remote())
        .and_then(|pool| {
            pool.run(|connection| {
                connection
                    .prepare("SELECT 1")
                    .and_then(|(select, connection)| {
                        connection.query(&select, &[]).for_each(|row| {
                            println!("result: {}", row.get::<i32, usize>(0));
                        })
                    })
                    .map(|connection| ((), connection))
            })
        });

    core.run(f).unwrap();
}
