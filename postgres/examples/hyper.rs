extern crate bb8;
extern crate bb8_postgres;
extern crate futures;
extern crate futures_state_stream;
extern crate hyper;
extern crate tokio;
extern crate tokio_postgres;

use std::sync::{Arc, Mutex};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::{
    future::{err, lazy, Either},
    Future, Stream,
};
use hyper::{Body, Request, Response, Server};
use hyper::service::service_fn;

// Select some static data from a Postgres DB and return it via hyper.
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
fn main() {
    let addr = ([127, 0, 0, 1], 3000).into();

    let pool: Arc<Mutex<Option<Pool<PostgresConnectionManager<tokio_postgres::NoTls>>>>> =
        Arc::new(Mutex::new(None));
    let pool2 = pool.clone();
    let svc = move || {
        let pool = pool.clone();
        service_fn(move |_: Request<Body>| {
            println!("Got request");
            let locked = pool.lock()
                .unwrap();
            let pool = locked.as_ref()
                .expect("bb8 should have been started before hyper");
            let f: Box<Future<Item = Response<Body>, Error = hyper::Error> + Send> =
                Box::new(pool.run(move |mut connection| {
                    connection.prepare("SELECT 1").then(move |r| match r {
                        Ok(select) => {
                            let f = connection
                                .query(&select, &[])
                                .map(|row| row.get::<usize, i32>(0))
                                .collect()
                                .map(|v| {
                                    println!("Sending success response");
                                    Response::new(Body::from(format!("Got results {:?}", v)))
                                })
                                .then(move |r| match r {
                                    Ok(v) => Ok((v, connection)),
                                    Err(e) => Err((e, connection)),
                                });
                            Either::A(f)
                        }
                        Err(e) => Either::B(err((e, connection))),
                    })
                }).or_else(|e| {
                    println!("Sending error response");
                    Ok(Response::new(Body::from(format!("Internal error {:?}", e))))
                }).map_err::<_, hyper::Error>(|_: bb8::RunError<tokio_postgres::Error>| unreachable!()));
            f
        })
    };

    let server = Server::bind(&addr)
        .serve(svc)
        .map_err(|e| eprintln!("server error: {}", e));

    hyper::rt::run(lazy(move || {
        // First start the bb8 pool.
        let pg_mgr = PostgresConnectionManager::new_from_stringlike(
            "postgresql://postgres:mysecretpassword@localhost:5432",
            tokio_postgres::NoTls,
        ).unwrap();

        Pool::builder()
            .build(pg_mgr)
            .map_err(|e| eprintln!("bb8 error {}", e))
            .and_then(move |p| {
                *pool2.lock().unwrap() = Some(p);
                // Only now can we run the HTTP server.
                println!("bb8 running, starting hyper");
                server
            })
    }));
}
