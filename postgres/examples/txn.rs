use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let pg_mgr = PostgresConnectionManager::new_from_stringlike(
        "postgresql://postgres:mysecretpassword@localhost:5432",
        tokio_postgres::NoTls,
    )
    .unwrap();

    let pool = Pool::builder().build(pg_mgr).await.unwrap();
    let mut connection = pool.get().await.unwrap();
    let transaction = connection.transaction().await.unwrap();

    let statement = transaction.prepare("SELECT 1").await.unwrap();
    let row = transaction.query_one(&statement, &[]).await.unwrap();
    println!("result: {}", row.get::<usize, i32>(0));

    transaction.commit().await.unwrap();
}
