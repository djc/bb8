use bb8::{Pool, RunError};
use futures_util::StreamExt as _;
use redis::AsyncCommands;
use redis::RedisError;

type RedisPool = Pool<bb8_redis::RedisConnectionManager>;

// Select some data from Redis
#[tokio::main]
async fn main() -> Result<(), RunError<RedisError>> {
    let redis_uri = String::from("redis://127.0.0.1:6379");

    let manager = bb8_redis::RedisConnectionManager::new(redis_uri.clone())
        .expect("Unable to create redis connection manager");

    let pool = match Pool::builder().build(manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {:?}", e),
    };

    let pubsub_manager = bb8_redis::RedisPubSubConnectionManager::new(redis_uri)
        .expect("Unable to create redis connection manager");

    let pubsub_pool = match Pool::builder().build(pubsub_manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("pubsub builder error: {:?}", e),
    };

    let value = get_value(pool.clone()).await?;
    println!("Got value: {:?}", value);

    let mut pubsub_conn = pubsub_pool.get().await?;

    pubsub_conn.subscribe("a-test-channel").await?;
    let mut pubsub_stream = pubsub_conn.on_message();

    publish(pool, "a-test-channel", "it works").await?;

    let pubsub_msg: String = pubsub_stream.next().await.unwrap().get_payload()?;
    println!("Got pubsub message: {:?}", pubsub_msg);

    Ok(())
}

async fn get_value(pool: RedisPool) -> Result<Option<String>, RunError<RedisError>> {
    let mut connection = pool.get().await?;

    connection
        .set::<&str, &str, ()>("my-key", "a value")
        .await?;
    connection.get("my-key").await.map_err(RunError::User)
}

async fn publish(pool: RedisPool, channel: &str, value: &str) -> Result<(), RunError<RedisError>> {
    let mut conn = pool.get().await?;

    conn.publish(channel, value).await.map_err(RunError::User)
}
