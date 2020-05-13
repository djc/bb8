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
                            .run(move |connection| async {
                                match connection.prepare("SELECT 1").await {
                                    Ok(select) => match connection.query_one(&select, &[]).await {
                                        Ok(row) => {
                                            let v = row.get::<usize, i32>(0);
                                            println!("Sending success response");
                                            let rsp = Response::new(Body::from(format!(
                                                "Got results {:?}",
                                                v
                                            )));
                                            Ok((rsp, connection))
                                        }
                                        Err(e) => Err((e, connection)),
                                    },
                                    Err(e) => Err((e, connection)),
                                }
                            })
                            .await;

                        Ok::<_, Error>(match result {
                            Ok(rsp) => rsp,
                            Err(e) => {
                                println!("Sending error response");
                                Response::new(Body::from(format!("Internal error {:?}", e)))
                            }
                        })
                    }
                }))
            }
        }))
        .await;
}
