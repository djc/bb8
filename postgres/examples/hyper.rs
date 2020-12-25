use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures_util::future::{FutureExt, TryFutureExt};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error, Response, Server};

// Select some static data from a Postgres DB and return it via hyper.
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let addr = ([127, 0, 0, 1], 3000).into();

    let pg_mgr = PostgresConnectionManager::new_from_stringlike(
        "postgresql://postgres:mysecretpassword@localhost:5432",
        tokio_postgres::NoTls,
    )
    .unwrap();

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("bb8 error {}", e),
    };

    let _ = Server::bind(&addr)
        .serve(make_service_fn(move |_| {
            let pool = pool.clone();
            async move {
                Ok::<_, Error>(service_fn(move |_| {
                    let pool = pool.clone();
                    async move {
                        println!("Got request");
                        Ok::<_, Error>(
                            pool.get()
                                .and_then(|connection| async {
                                    let select = connection.prepare("SELECT 1").await?;
                                    Ok((connection, select))
                                })
                                .and_then(|(connection, select)| async move {
                                    let row = connection.query_one(&select, &[]).await?;
                                    Ok(row)
                                })
                                .map(|result| match result {
                                    Ok(row) => {
                                        let v = row.get::<usize, i32>(0);
                                        println!("Sending success response");
                                        Response::new(Body::from(format!("Got results {:?}", v)))
                                    }
                                    Err(e) => {
                                        println!("Sending error response");
                                        Response::new(Body::from(format!("Internal error {:?}", e)))
                                    }
                                })
                                .await,
                        )
                    }
                }))
            }
        }))
        .await;
}
