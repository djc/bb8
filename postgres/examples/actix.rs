use actix_rt;
use actix_web::{get, middleware, web, App, HttpRequest, HttpServer};
use std::io;
use tokio_postgres;

use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;
use std::str::FromStr;

type AsyncPool = Pool<PostgresConnectionManager<tokio_postgres::NoTls>>;

async fn get_async_connection_pool(url: &str) -> AsyncPool {
    let config = tokio_postgres::config::Config::from_str(&url).unwrap();
    let pg_mgr = PostgresConnectionManager::new(config, tokio_postgres::NoTls);

    bb8::Pool::builder()
        .build(pg_mgr)
        .await
        .expect("Failed to create pool")
}

async fn select(pool: &AsyncPool) -> Result<i32, RunError<tokio_postgres::Error>> {
    let connection = pool.get().await?;

    // although prepare and query_one return tokio_postgres::Error
    // `RunError` impl `From<std::error::Error>`
    let stmt = connection.prepare("SELECT 1").await?;
    let row = connection.query_one(&stmt, &[]).await?;

    Ok(row.get::<usize, i32>(0))
}

#[get("/")]
async fn index(_: HttpRequest, context: web::Data<AsyncPool>) -> String {
    match select(&context).await {
        Ok(result) => format!("result: {}", result),
        Err(error) => format!("Error occurred: {}", error),
    }
}

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[actix_rt::main]
async fn main() -> io::Result<()> {
    let pool = get_async_connection_pool("postgresql://postgres:docker@localhost:5432").await;

    HttpServer::new(move || {
        App::new()
            .data(pool.clone())
            .wrap(middleware::Compress::default())
            .service(index)
    })
    .bind("127.0.0.1:8080")?
    .workers(2)
    .run()
    .await
}
