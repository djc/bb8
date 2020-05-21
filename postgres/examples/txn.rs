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

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    let _ = pool
        .run(|connection| async move {
            let _ = connection.simple_query("BEGIN").await?;
            let result = match connection.prepare("SELECT 1").await {
                Ok(select) => connection.query_one(&select, &[]).await.map(|row| {
                    println!("result: {}", row.get::<usize, i32>(0));
                }),
                Err(e) => Err(e),
            };

            let finalize_query = match &result {
                Ok(()) => "COMMIT",
                Err(_) => "ROLLBACK",
            };

            let _ = connection.simple_query(finalize_query).await;
            result
        })
        .await
        .map_err(|e| panic!("{:?}", e));
}
