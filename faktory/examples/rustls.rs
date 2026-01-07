use bb8::Pool;
use bb8_faktory::{FaktoryConnectionManager, TlsConnector};
use faktory::Job;

// Enqueue a job using rustls for secure connections
// Start Faktory with TLS enabled (requires TLS configuration)
#[tokio::main]
async fn main() {
    let manager = FaktoryConnectionManager::new("tcp+tls://localhost:7419")
        .with_tls(TlsConnector::rustls_default());

    // Or with custom ClientConfig:
    // let config = rustls::ClientConfig::builder()
    //     .with_root_certificates(your_root_store)
    //     .with_no_client_auth();
    // let manager = FaktoryConnectionManager::new("tcp+tls://localhost:7419")
    //     .with_tls(TlsConnector::rustls_with_config(config));

    // Or from environment (auto-selects TLS when feature enabled):
    // let manager = FaktoryConnectionManager::from_env();

    let pool = match Pool::builder().build(manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("builder error: {e:?}"),
    };

    let mut conn = pool.get().await.unwrap();
    conn.enqueue(Job::new("email", vec!["user@example.com"]))
        .await
        .unwrap();

    println!("Job enqueued successfully over TLS (rustls)!");
}
