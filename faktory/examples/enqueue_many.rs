use bb8::Pool;
use bb8_faktory::FaktoryConnectionManager;
use faktory::Job;

// Enqueue multiple jobs to a Faktory work server
//
// Run with: FAKTORY_URL="tcp://localhost:7419" cargo run --example enqueue_many
#[tokio::main]
async fn main() {
    let manager = FaktoryConnectionManager::from_env();

    let pool = Pool::builder().build(manager).await.unwrap();

    let mut conn = pool.get().await.unwrap();

    let jobs = vec![
        Job::new("email", vec!["user1@example.com"]),
        Job::new("email", vec!["user2@example.com"]),
        Job::new("email", vec!["user3@example.com"]),
    ];

    let (count, errors) = conn.enqueue_many(jobs).await.unwrap();

    match errors {
        Some(errs) => println!("Enqueued {count} jobs with errors: {errs:?}"),
        None => println!("Successfully enqueued {count} jobs"),
    }
}
