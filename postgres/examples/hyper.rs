use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
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

                        let result = pool
                            .run(move |connection| async move {
                                let select = connection.prepare("SELECT 1").await?;
                                let row = connection.query_one(&select, &[]).await?;
                                Ok::<_, bb8_postgres::tokio_postgres::Error>(
                                    row.get::<usize, i32>(0),
                                )
                            })
                            .await;

                        Ok::<_, Error>(Response::new(Body::from(match result {
                            Ok(v) => {
                                println!("Sending success response");
                                format!("Got results {:?}", v)
                            }
                            Err(e) => {
                                println!("Sending error response");
                                format!("Internal error {:?}", e)
                            }
                        })))
                    }
                }))
            }
        }))
        .await;
}
