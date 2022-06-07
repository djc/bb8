use std::convert::Infallible;

use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error, Response, Server};
use tokio_postgres::NoTls;

// Select some static data from a Postgres DB and return it via hyper.
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let addr = ([127, 0, 0, 1], 3000).into();

    let pg_mgr = PostgresConnectionManager::new_from_stringlike(
        "postgresql://postgres:mysecretpassword@localhost:5432",
        NoTls,
    )
    .unwrap();

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("bb8 error {e}"),
    };

    let _ = Server::bind(&addr)
        .serve(make_service_fn(move |_| {
            let pool = pool.clone();
            async move {
                Ok::<_, Error>(service_fn(move |_| {
                    let pool = pool.clone();
                    async move {
                        println!("Got request");
                        Ok::<_, Infallible>(match handler(pool).await {
                            Ok(rsp) => {
                                println!("Sending success response");
                                rsp
                            }
                            Err(e) => {
                                println!("Sending error response");
                                Response::new(Body::from(format!("Internal error {e:?}")))
                            }
                        })
                    }
                }))
            }
        }))
        .await;
}

async fn handler(
    pool: Pool<PostgresConnectionManager<NoTls>>,
) -> Result<Response<Body>, RunError<tokio_postgres::Error>> {
    let conn = pool.get().await?;
    let stmt = conn.prepare("SELECT 1").await?;
    let row = conn.query_one(&stmt, &[]).await?;
    let v = row.get::<usize, i32>(0);
    Ok(Response::new(Body::from(format!("Got results {v:?}"))))
}
