use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;

use std::str::FromStr;

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let config =
        tokio_postgres::config::Config::from_str("postgresql://postgres:docker@localhost:5432")
            .unwrap();
    let pg_mgr = PostgresConnectionManager::new(config, tokio_postgres::NoTls);

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    match select(&pool).await {
        Ok(result) => println!("result: {}", result),
        Err(error) => eprintln!("Error occurred: {}", error),
    }
}

async fn select(
    pool: &Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
) -> Result<i32, RunError<tokio_postgres::Error>> {
    let connection = pool.get().await?;

    // although prepare and query_one return tokio_postgres::Error
    // `RunError` impl `From<std::error::Error>`
    let stmt = connection.prepare("SELECT 1").await?;

    let row = connection.query_one(&stmt, &[]).await?;

    Ok(row.get::<usize, i32>(0))
}
