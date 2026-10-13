use bb8::{ManageConnection, Pool, RunError};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

#[derive(Debug)]
struct Invalid;

#[derive(Default)]
struct ImmediatelyInvalid(OnceLock<Instant>);

impl ManageConnection for ImmediatelyInvalid {
    type Connection = ();
    type Error = Invalid;

    async fn connect(&self) -> Result<(), Invalid> {
        Ok(())
    }

    async fn is_valid(&self, _: &mut ()) -> Result<(), Invalid> {
        // Eventually succeed so a checkout that stops yielding fails instead of hanging.
        if self.0.get_or_init(Instant::now).elapsed() < Duration::from_millis(250) {
            Err(Invalid)
        } else {
            Ok(())
        }
    }

    fn has_broken(&self, _: &mut ()) -> bool {
        false
    }
}

#[tokio::test]
async fn immediate_validation_failures_obey_get_timeout() {
    let pool = Pool::builder()
        .max_size(1)
        .test_on_check_out(true)
        .connection_timeout(Duration::from_millis(50))
        .build_unchecked(ImmediatelyInvalid::default());
    pool.add(()).unwrap();
    let mut get = Box::pin(pool.get());
    let waker = futures_util::task::noop_waker();
    assert!(get
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    assert_eq!(pool.state().statistics.connections_closed_invalid, 1);
    assert!(matches!(get.await, Err(RunError::TimedOut)));
}

struct SlowValidator {
    pause_next: Arc<AtomicBool>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl ManageConnection for SlowValidator {
    type Connection = ();
    type Error = Invalid;

    async fn connect(&self) -> Result<(), Invalid> {
        Ok(())
    }

    async fn is_valid(&self, _: &mut ()) -> Result<(), Invalid> {
        if self.pause_next.swap(false, Ordering::SeqCst) {
            let release = self.release.lock().unwrap().take().unwrap();
            release.await.unwrap();
        }
        Ok(())
    }

    fn has_broken(&self, _: &mut ()) -> bool {
        false
    }
}

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn slow_validation_does_not_consume_capacity_waiter_notification() {
    let pause_next = Arc::new(AtomicBool::new(false));
    let (release, receiver) = oneshot::channel();
    let pool = Pool::builder()
        .max_size(2)
        .min_idle(Some(2))
        .test_on_check_out(true)
        .connection_timeout(Duration::from_secs(1))
        .build(SlowValidator {
            pause_next: pause_next.clone(),
            release: Mutex::new(Some(receiver)),
        })
        .await
        .unwrap();
    let held = pool.get().await.unwrap();

    pause_next.store(true, Ordering::SeqCst);
    let mut slow = Box::pin(pool.get());
    let slow_waker = Waker::from(Arc::new(Wakes::default()));
    assert!(slow
        .as_mut()
        .poll(&mut Context::from_waker(&slow_waker))
        .is_pending());
    assert!(!pause_next.load(Ordering::SeqCst));
    assert_eq!(pool.state().idle_connections, 0);

    let wakes = Arc::new(Wakes::default());
    let waiter_waker = Waker::from(wakes.clone());
    let mut waiter = Box::pin(pool.get());
    assert!(waiter
        .as_mut()
        .poll(&mut Context::from_waker(&waiter_waker))
        .is_pending());
    let before = wakes.0.load(Ordering::SeqCst);

    drop(held);
    assert_eq!(pool.state().idle_connections, 1);
    assert!(wakes.0.load(Ordering::SeqCst) > before);

    let _acquired = waiter.await.unwrap();
    release.send(()).unwrap();
    slow.await.unwrap();
}
