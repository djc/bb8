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
        .build_unchecked(Controlled { attempts });
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
