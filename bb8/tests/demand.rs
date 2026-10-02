use bb8::{ErrorSink, ManageConnection, Pool, PooledConnection, RunError};
use std::{
    cell::Cell,
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

// Deliberately non-Clone and non-Sync, but Send: bb8's existing Error bound.
#[derive(Debug)]
struct Failure {
    id: usize,
    fatal: bool,
    _not_sync: Cell<u8>,
}
impl Failure {
    fn new(id: usize, fatal: bool) -> Self {
        Self {
            id,
            fatal,
            _not_sync: Cell::new(0),
        }
    }
}
#[derive(Debug)]
struct Connection {
    id: usize,
    broken: bool,
}
#[derive(Debug)]
struct Attempt {
    id: usize,
    finish: oneshot::Sender<Result<Connection, Failure>>,
}
impl Attempt {
    fn fail(self, fatal: bool) {
        self.finish
            .send(Err(Failure::new(self.id, fatal)))
            .expect("live connect future");
    }
    fn succeed(self) {
        self.finish
            .send(Ok(Connection {
                id: self.id,
                broken: false,
            }))
            .expect("live connect future");
    }
}
struct Manager {
    attempts: mpsc::UnboundedSender<Attempt>,
    count: AtomicUsize,
}
impl ManageConnection for Manager {
    type Connection = Connection;
    type Error = Failure;
    async fn connect(&self) -> Result<Connection, Failure> {
        let id = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        let (finish, result) = oneshot::channel();
        self.attempts.send(Attempt { id, finish }).unwrap();
        result.await.expect("controller must finish attempt")
    }
    async fn is_valid(&self, _: &mut Connection) -> Result<(), Failure> {
        Ok(())
    }
    fn has_broken(&self, conn: &mut Connection) -> bool {
        conn.broken
    }
    fn error_is_fatal(&self, e: &Failure) -> bool {
        e.fatal
    }
}
#[derive(Clone, Debug)]
struct Sink {
    errors: Arc<Mutex<Vec<usize>>>,
}
impl ErrorSink<Failure> for Sink {
    fn sink(&self, e: Failure) {
        self.errors.lock().unwrap().push(e.id);
    }
    fn boxed_clone(&self) -> Box<dyn ErrorSink<Failure>> {
        Box::new(self.clone())
    }
}
fn setup(
    retry: bool,
    max: u32,
    ms: u64,
    idle: Option<u32>,
) -> (Pool<Manager>, mpsc::UnboundedReceiver<Attempt>, Sink) {
    let (attempts, rx) = mpsc::unbounded_channel();
    let manager = Manager {
        attempts,
        count: AtomicUsize::new(0),
    };
    let sink = Sink {
        errors: Arc::new(Mutex::new(Vec::new())),
    };
    let pool = Pool::builder()
        .retry_connection(retry)
        .max_size(max)
        .min_idle(idle)
        .connection_timeout(Duration::from_millis(ms))
        .test_on_check_out(false)
        .error_sink(Box::new(sink.clone()))
        .build_unchecked(manager);
    (pool, rx, sink)
}
async fn attempt(rx: &mut mpsc::UnboundedReceiver<Attempt>) -> Attempt {
    timeout(Duration::from_millis(900), rx.recv())
        .await
        .expect("connection attempt should start")
        .unwrap()
}
async fn start(
    pool: &Pool<Manager>,
) -> tokio::task::JoinHandle<Result<PooledConnection<'static, Manager>, RunError<Failure>>> {
    let pool = pool.clone();
    let (polled, ready) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut get = Box::pin(pool.get_owned());
        let mut polled = Some(polled);
        poll_fn(|cx| {
            let result = get.as_mut().poll(cx);
            if let Some(polled) = polled.take() {
                polled.send(()).unwrap();
            }
            result
        })
        .await
    });
    ready.await.unwrap();
    task
}
async fn user_error(
    task: tokio::task::JoinHandle<Result<PooledConnection<'static, Manager>, RunError<Failure>>>,
    id: usize,
) {
    match timeout(Duration::from_millis(100), task)
        .await
        .expect("must fail promptly, before pool deadline")
        .unwrap()
    {
        Err(RunError::User(e)) => assert_eq!(e.id, id),
        other => panic!("expected owned manager error {id}, got {other:?}"),
    }
}

#[tokio::test]
async fn fatal_error_stops_enabled_retries_promptly() {
    let (pool, mut rx, sink) = setup(true, 1, 2000, None);
    let task = start(&pool).await;
    attempt(&mut rx).await.fail(true);
    user_error(task, 1).await;
    assert!(rx.try_recv().is_err());
    assert!(sink.errors.lock().unwrap().is_empty());
}

#[tokio::test]
async fn transient_error_retries_then_recovers_and_is_logged() {
    let (pool, mut rx, sink) = setup(true, 1, 2000, None);
    let task = start(&pool).await;
    attempt(&mut rx).await.fail(false);
    let next = attempt(&mut rx).await;
    assert_eq!(next.id, 2);
    next.succeed();
    assert_eq!(task.await.unwrap().unwrap().id, 2);
    assert_eq!(*sink.errors.lock().unwrap(), vec![1]);
}

#[tokio::test]
async fn get_timeout_cancels_pending_attempt_and_releases_capacity() {
    for retry in [false, true] {
        let (pool, mut rx, sink) = setup(retry, 1, 150, None);
        let task = start(&pool).await;
        let mut pending = attempt(&mut rx).await;
        if retry {
            pending.fail(false);
            pending = attempt(&mut rx).await;
        }
        assert!(matches!(task.await.unwrap(), Err(RunError::TimedOut)));
        assert!(pending.finish.is_closed());
        let expected_errors = if retry { vec![1] } else { vec![] };
        assert_eq!(*sink.errors.lock().unwrap(), expected_errors);

        let next = start(&pool).await;
        let new = attempt(&mut rx).await;
        assert_eq!(new.id, pending.id + 1);
        new.succeed();
        assert_eq!(next.await.unwrap().unwrap().id, pending.id + 1);
    }
}

#[tokio::test]
async fn concurrent_errors_go_to_their_own_attempts() {
    let (pool, mut rx, sink) = setup(false, 2, 500, None);
    let first = start(&pool).await;
    let a = attempt(&mut rx).await;
    let second = start(&pool).await;
    let b = attempt(&mut rx).await;
    b.fail(false);
    user_error(second, 2).await;
    a.succeed();
    assert_eq!(first.await.unwrap().unwrap().id, 1);
    assert!(sink.errors.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_stale_error_after_recovery_or_in_full_pool() {
    let (pool, mut rx, _) = setup(false, 1, 150, None);
    let first = start(&pool).await;
    attempt(&mut rx).await.fail(false);
    user_error(first, 1).await;
    let second = start(&pool).await;
    attempt(&mut rx).await.succeed();
    let held = second.await.unwrap().unwrap();
    assert!(matches!(pool.get().await, Err(RunError::TimedOut)));
    drop(held);
    assert_eq!(pool.get().await.unwrap().id, 2);
}

#[tokio::test]
async fn background_error_is_logged_and_capacity_notifies_waiter() {
    let (pool, mut rx, sink) = setup(false, 1, 500, Some(1));
    let background = attempt(&mut rx).await;
    let task = start(&pool).await;
    background.fail(false);
    let demand = attempt(&mut rx).await;
    assert_eq!(demand.id, 2);
    demand.succeed();
    assert_eq!(task.await.unwrap().unwrap().id, 2);
    assert_eq!(*sink.errors.lock().unwrap(), vec![1]);
}

#[tokio::test]
async fn idle_wins_cancels_owned_attempt_and_preserves_min_idle() {
    let (pool, mut rx, _) = setup(false, 2, 1000, Some(1));
    let background = attempt(&mut rx).await;
    let first = start(&pool).await;
    let second = start(&pool).await;
    let demand = attempt(&mut rx).await;
    assert_eq!(demand.id, 2);
    background.succeed();
    let held = first.await.unwrap().unwrap();
    assert_eq!(held.id, 1);
    drop(held);
    let second = timeout(Duration::from_millis(100), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.id, 1);
    assert!(demand.finish.is_closed());
    let refill = attempt(&mut rx).await;
    assert_eq!(refill.id, 3);
    refill.succeed();
    tokio::task::yield_now().await;
    assert_eq!(pool.state().idle_connections, 1);
    assert_eq!(pool.state().connections, 2);
}

#[tokio::test]
async fn cancelling_owned_attempt_notifies_an_existing_capacity_waiter() {
    let (pool, mut rx, _) = setup(false, 1, 1000, None);
    let first = start(&pool).await;
    let old = attempt(&mut rx).await;
    let waiting = start(&pool).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(old.finish.is_closed());
    attempt(&mut rx).await.succeed();
    assert_eq!(waiting.await.unwrap().unwrap().id, 2);
}

#[tokio::test]
async fn pending_background_attempt_survives_get_timeout() {
    let (pool, mut rx, _) = setup(false, 1, 70, Some(1));
    let background = attempt(&mut rx).await;
    assert!(matches!(pool.get().await, Err(RunError::TimedOut)));
    assert!(!background.finish.is_closed());
    assert!(matches!(pool.get().await, Err(RunError::TimedOut)));
    assert!(rx.try_recv().is_err());
    background.succeed();
    tokio::task::yield_now().await;
    assert_eq!(pool.get().await.unwrap().id, 1);
}

#[tokio::test]
async fn build_keeps_its_existing_connection_error_and_timeout_behavior() {
    let (attempts, mut rx) = mpsc::unbounded_channel();
    let manager = Manager {
        attempts,
        count: AtomicUsize::new(0),
    };
    let mut build = tokio::spawn(
        Pool::builder()
            .max_size(1)
            .min_idle(1)
            .retry_connection(false)
            .connection_timeout(Duration::from_millis(20))
            .build(manager),
    );
    let pending = attempt(&mut rx).await;
    assert!(timeout(Duration::from_millis(50), &mut build)
        .await
        .is_err());
    assert!(!pending.finish.is_closed());
    pending.fail(false);
    assert_eq!(build.await.unwrap().unwrap_err().id, 1);
}

#[derive(Debug)]
struct Customizer {
    requests: mpsc::UnboundedSender<oneshot::Sender<Result<(), Failure>>>,
}
impl bb8::CustomizeConnection<Connection, Failure> for Customizer {
    fn on_acquire<'a>(
        &'a self,
        _: &'a mut Connection,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), Failure>> + Send + 'a>> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            self.requests.send(tx).unwrap();
            rx.await.unwrap()
        })
    }
}

#[tokio::test]
async fn customizer_errors_are_owned_and_pending_customizer_can_be_cancelled() {
    let (attempts, mut rx) = mpsc::unbounded_channel();
    let (requests, mut custom) = mpsc::unbounded_channel();
    let manager = Manager {
        attempts,
        count: AtomicUsize::new(0),
    };
    let pool = Pool::builder()
        .retry_connection(true)
        .max_size(1)
        .connection_timeout(Duration::from_millis(300))
        .test_on_check_out(false)
        .connection_customizer(Box::new(Customizer { requests }))
        .build_unchecked(manager);
    let first = start(&pool).await;
    attempt(&mut rx).await.succeed();
    custom
        .recv()
        .await
        .unwrap()
        .send(Err(Failure::new(123, true)))
        .unwrap();
    user_error(first, 123).await;
    let pending = start(&pool).await;
    attempt(&mut rx).await.succeed();
    let custom_pending = custom.recv().await.unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(custom_pending.is_closed());
    let next = start(&pool).await;
    attempt(&mut rx).await.succeed();
    custom.recv().await.unwrap().send(Ok(())).unwrap();
    assert_eq!(next.await.unwrap().unwrap().id, 3);
}

#[tokio::test]
async fn fatal_demand_min_idle_refill_fails_once_without_hot_loop() {
    let (pool, mut rx, sink) = setup(true, 1, 1000, Some(1));
    attempt(&mut rx).await.fail(true);
    tokio::task::yield_now().await;
    let task = start(&pool).await;
    attempt(&mut rx).await.fail(true);
    user_error(task, 2).await;
    let refill = attempt(&mut rx).await;
    assert_eq!(refill.id, 3);
    refill.fail(true);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());
    assert_eq!(*sink.errors.lock().unwrap(), vec![1, 3]);
}

#[test]
fn cancelling_demand_outside_runtime_replenishes_min_idle() {
    use std::task::Poll;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (pool, mut rx, _) = runtime.block_on(async { setup(false, 2, 1000, Some(1)) });
    let mut demand = Box::pin(pool.get());
    let (held, pending) = runtime.block_on(async {
        let background = attempt(&mut rx).await;
        let first = start(&pool).await;
        poll_fn(|cx| {
            assert!(demand.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let pending = attempt(&mut rx).await;
        background.succeed();
        (first.await.unwrap().unwrap(), pending)
    });

    drop(demand);
    assert!(pending.finish.is_closed());
    runtime.block_on(async {
        let refill = attempt(&mut rx).await;
        assert_eq!(refill.id, 3);
        refill.succeed();
        tokio::task::yield_now().await;
        assert_eq!(pool.state().idle_connections, 1);
        assert_eq!(pool.state().connections, 2);
    });
    drop(held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_capacity_stress_no_lost_wakeups_or_deadlocks() {
    // Exceed Tokio's 32-waker batch size while mixing failures and idle returns.
    let (pool, mut rx, _) = setup(false, 4, 2000, None);
    let controller = tokio::spawn(async move {
        while let Some(a) = rx.recv().await {
            let result = if a.id % 5 == 0 {
                Err(Failure::new(a.id, false))
            } else {
                Ok(Connection {
                    id: a.id,
                    broken: false,
                })
            };
            // An idle return can legitimately cancel this demand attempt.
            let _ = a.finish.send(result);
        }
    });
    let mut tasks = Vec::new();
    for n in 0..48 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..80 {
                match pool.get().await {
                    Ok(mut conn) => {
                        tokio::task::yield_now().await;
                        conn.broken = (n + i) % 11 == 0;
                    }
                    Err(RunError::User(_)) => {}
                    Err(RunError::TimedOut) => panic!("capacity got stranded"),
                }
            }
        }));
    }
    timeout(Duration::from_secs(5), async {
        for t in tasks {
            t.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(pool.state().connections <= 4);
    drop(pool);
    controller.abort();
}
