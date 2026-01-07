use bb8::Pool;
use bb8_faktory::FaktoryConnectionManager;
use faktory::Job;

// Enqueue a job to a Faktory work server
//
// Run with: FAKTORY_URL="tcp://localhost:7419" cargo run --example enqueue
#[tokio::main]
async fn main() {
    let manager = FaktoryConnectionManager::from_env();

    let pool = match Pool::builder().build(manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {e:?}"),
    };

    let mut conn = pool.get().await.unwrap();
    conn.enqueue(Job::new("email", vec!["user@example.com"]))
        .await
        .unwrap();

    println!("Job enqueued successfully!");
}
