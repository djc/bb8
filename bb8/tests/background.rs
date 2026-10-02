use bb8::{ManageConnection, Pool};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Wake, Waker},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct Failure;

struct Controlled {
    attempts: mpsc::UnboundedSender<oneshot::Sender<Result<(), Failure>>>,
    fatal: bool,
}

impl ManageConnection for Controlled {
    type Connection = ();
    type Error = Failure;

    async fn connect(&self) -> Result<(), Failure> {
        let (tx, rx) = oneshot::channel();
        self.attempts.send(tx).unwrap();
        rx.await.unwrap()
    }

    async fn is_valid(&self, _: &mut ()) -> Result<(), Failure> {
        Ok(())
    }

    fn has_broken(&self, _: &mut ()) -> bool {
        false
    }

    fn error_is_fatal(&self, _: &Failure) -> bool {
        self.fatal
    }
}

#[derive(Default)]
struct Wakes(std::sync::atomic::AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn failed_background_attempt_wakes_capacity_waiter() {
    let (attempts, mut rx) = mpsc::unbounded_channel();
    let pool = Pool::builder()
        .max_size(1)
        .min_idle(1)
        .retry_connection(false)
        .connection_timeout(Duration::from_secs(1))
        .build_unchecked(Controlled {
            attempts,
            fatal: false,
        });
    let background = rx.recv().await.unwrap();
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut get = Box::pin(pool.get());
    assert!(get
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let before = wakes.0.load(std::sync::atomic::Ordering::SeqCst);
    background.send(Err(Failure)).unwrap();
    tokio::task::yield_now().await;
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) > before);
    assert!(get
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    rx.recv().await.unwrap().send(Ok(())).unwrap();
    get.await.unwrap();
}

#[derive(Debug)]
struct FailingCustomizer;

impl bb8::CustomizeConnection<(), Failure> for FailingCustomizer {
    fn on_acquire<'a>(
        &'a self,
        _: &'a mut (),
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Failure>> + Send + 'a>> {
        Box::pin(async { Err(Failure) })
    }
}

#[tokio::test]
async fn fatal_connect_and_customizer_errors_stop_build_retries() {
    // Fail either connect() or the customizer after connect() succeeds.
    for result in [Err(Failure), Ok(())] {
        let (attempts, mut rx) = mpsc::unbounded_channel();
        let build = tokio::spawn(
            Pool::builder()
                .min_idle(1)
                .connection_timeout(Duration::from_secs(2))
                .connection_customizer(Box::new(FailingCustomizer))
                .build(Controlled {
                    attempts,
                    fatal: true,
                }),
        );
        rx.recv().await.unwrap().send(result).unwrap();
        tokio::time::timeout(Duration::from_millis(100), build)
            .await
            .expect("fatal initialization error must return before retry backoff")
            .unwrap()
            .unwrap_err();
        assert!(rx.try_recv().is_err());
    }
}
