use bb8::Pool;
use bb8_faktory::{FaktoryConnectionManager, TlsConnector};
use faktory::Job;

// Enqueue a job using native-tls for secure connections//
// Start Faktory with TLS enabled (requires TLS configuration)
#[tokio::main]
async fn main() {
    let manager = FaktoryConnectionManager::new("tcp+tls://localhost:7419")
        .with_tls(TlsConnector::native_tls_default().unwrap());

    // Or with custom connector (e.g., accepting invalid certs for testing):
    // let connector = native_tls::TlsConnector::builder()
    //     .danger_accept_invalid_certs(true)
    //     .build()
    //     .unwrap();
    // let manager = FaktoryConnectionManager::new("tcp+tls://localhost:7419")
    //     .with_tls(TlsConnector::NativeTls(connector));

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

    println!("Job enqueued successfully over TLS (native-tls)!");
}
