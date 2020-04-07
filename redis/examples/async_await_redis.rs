use bb8::{Pool, RunError};
use redis::AsyncCommands;
use redis::RedisError;

type RedisPool = Pool<bb8_redis::RedisConnectionManager>;

// Select some data from Redis
#[tokio::main]
async fn main() {
    let redis_uri = String::from("redis://127.0.0.1:6379");
    let manager = bb8_redis::RedisConnectionManager::new(redis_uri)
        .expect("Unable to create redis connection manager");

    let pool = match Pool::builder().build(manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    let result = get_value(pool).await;
    println!("Got result: {:?}", result);
}

async fn get_value(pool: RedisPool) -> Result<Option<String>, RunError<RedisError>> {
    let mut connection = pool.get().await?;

    let _ = connection.set::<&str, &str, ()>("my-key", "a value").await;
    connection.get("my-key").await.map_err(RunError::User)
}
