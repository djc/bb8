use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;

use std::str::FromStr;

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let config = tokio_postgres::config::Config::from_str(
        "postgresql://postgres:mysecretpassword@localhost:5432",
    )
    .unwrap();
    let pg_mgr = PostgresConnectionManager::new(config, tokio_postgres::NoTls);

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    let connection = pool.get().await.unwrap();
    let select = connection.prepare("SELECT 1").await.unwrap();
    let row = connection.query_one(&select, &[]).await.unwrap();
    println!("result: {}", row.get::<usize, i32>(0));
}
