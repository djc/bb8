use bb8::{Pool, PooledConnection, RunError};
use bb8_postgres::{PostgresConnectionManager, PostgresConnectionManagerBuilder};

use std::error::Error;
use std::str::FromStr;

// Demonstrate the postgres connection manager builder and its validation query.
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config =
        tokio_postgres::config::Config::from_str("postgresql://postgres:docker@localhost:5432")
            .unwrap();

    let pg_mgr = PostgresConnectionManagerBuilder::new()
        .tls(tokio_postgres::NoTls)
        .validation_query("DISCARD ALL".into())
        .config(config)
        .build()?;

    let pool = match Pool::builder().build(pg_mgr).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    // The first checkout sets a session parameter.
    {
        let connection = pool.get().await?;
        println!(
            "The current connection PID: {}",
            get_pid(&connection).await?
        );
        set_session_parameter(&connection, "bb8.greeting", "beep boop").await?;
        let greeting = show_session_parameter(&connection, "bb8.greeting").await?;
        println!("BB8 says, {:?}", greeting);
    }

    // The second checkout issues the DISCARD ALL validation query.
    {
        let connection = pool.get().await?;
        println!(
            "The current connection PID: {}",
            get_pid(&connection).await?
        );
        let greeting = show_session_parameter(&connection, "bb8.greeting").await?;
        println!("After DISCARD ALL on checkout, BB8 says, {:?}", greeting);
    }

    Ok(())
}

async fn get_pid(
    connection: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
) -> Result<i32, RunError<tokio_postgres::Error>> {
    let row = connection.query_one("select pg_backend_pid()", &[]).await?;
    Ok(row.get::<usize, i32>(0))
}

async fn set_session_parameter(
    connection: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
    key: &str,
    value: &str,
) -> Result<(), RunError<tokio_postgres::Error>> {
    let query = format!("SET {} = '{}'", key, value);
    connection.query(query.as_str(), &[]).await?;
    Ok(())
}

async fn show_session_parameter(
    connection: &PooledConnection<'_, PostgresConnectionManager<tokio_postgres::NoTls>>,
    key: &str,
) -> Result<String, RunError<tokio_postgres::Error>> {
    let query = format!("SHOW {}", key);
    let row = connection.query_one(query.as_str(), &[]).await?;
    Ok(row.get::<usize, String>(0))
}
