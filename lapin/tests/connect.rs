use bb8_lapin::prelude::*;
use std::sync::Arc;
use tokio_amqp::LapinTokioExt;

lazy_static::lazy_static! {
    static ref AMQP_URL: String = {
        dotenv::dotenv().ok();
        std::env::var("TEST_AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672//".to_string())
    };
}

#[tokio::test]
async fn can_connect() {
    let manager =
        LapinConnectionManager::new(&AMQP_URL, ConnectionProperties::default().with_tokio());
    let pool = Arc::new(
        bb8::Pool::builder()
            .max_size(2)
            .test_on_check_out(true)
            .build(manager)
            .await
            .expect("Should create pool"),
    );
    let n_tasks = 100;
    for i in 0..n_tasks {
        let pool = pool.clone();
        tokio::spawn(async move {
            let delay_ms = i - n_tasks;
            tokio::time::delay_for(tokio::time::Duration::from_millis(delay_ms as u64)).await;
            let conn = pool.get().await.expect("Should get connection");
            conn.create_channel()
                .await
                .expect("Should create lapin channel");
        });
    }
}
