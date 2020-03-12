use tokio;
use tokio_postgres;

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
        .run(|connection| async {
            if let Err(e) = connection.simple_query("BEGIN").await {
                return Err((e, connection));
            }

            let err = match connection.prepare("SELECT 1").await {
                Ok(select) => match connection.query_one(&select, &[]).await {
                    Ok(row) => {
                        println!("result: {}", row.get::<usize, i32>(0));
                        None
                    }
                    Err(e) => Some(e),
                },
                Err(e) => Some(e),
            };

            let finalize_query = match &err {
                None => "COMMIT",
                Some(_) => "ROLLBACK",
            };

            let _ = connection.simple_query(finalize_query).await;
            match err {
                Some(e) => Err((e, connection)),
                None => Ok(((), connection)),
            }
        })
        .await
        .map_err(|e| panic!("{:?}", e));
}
