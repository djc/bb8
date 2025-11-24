use bb8::*;

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use std::{error, fmt};

use futures_util::future::{err, lazy, ok, pending, ready, try_join_all, FutureExt};
use futures_util::stream::{FuturesUnordered, TryStreamExt};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

#[derive(Debug, PartialEq, Eq)]
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("blammo")
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "Error"
    }
}

#[derive(Debug, Default)]
struct FakeConnection;

struct OkManager<C> {
    _c: PhantomData<C>,
}

impl<C> OkManager<C> {
    fn new() -> Self {
        OkManager { _c: PhantomData }
    }
}

impl<C: Default + Send + Sync + 'static> ManageConnection for OkManager<C> {
    type Connection = C;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(Default::default())
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

struct NthConnectionFailManager<C> {
    n: Mutex<u32>,
    _c: PhantomData<C>,
}

impl<C> NthConnectionFailManager<C> {
    fn new(n: u32) -> Self {
        NthConnectionFailManager {
            n: Mutex::new(n),
            _c: PhantomData,
        }
    }
}

impl<C: Default + Send + Sync + 'static> ManageConnection for NthConnectionFailManager<C> {
    type Connection = C;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut n = self.n.lock().unwrap();
        if *n > 0 {
            *n -= 1;
            Ok(Default::default())
        } else {
            Err(Error)
        }
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[tokio::test]
async fn test_max_size_ok() {
    let manager = NthConnectionFailManager::<FakeConnection>::new(5);
    let pool = Pool::builder().max_size(5).build(manager).await.unwrap();
    let mut channels = Vec::with_capacity(5);
    let mut ignored = Vec::with_capacity(5);
    for _ in 0..5 {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel::<()>();
        let pool = pool.clone();
        tokio::spawn(async move {
            let conn = pool.get().await.unwrap();
            tx1.send(()).unwrap();
            let _ = rx2
                .map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
                .await;
        });
        channels.push(rx1);
        ignored.push(tx2);
    }
    assert_eq!(channels.len(), 5);
    assert_eq!(try_join_all(channels).await.unwrap().len(), 5);
}

#[tokio::test]
async fn test_acquire_release() {
    let pool = Pool::builder()
        .max_size(2)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        let _ = rx2
            .then(|r| match r {
                Ok(v) => ok((v, conn)),
                Err(_) => err((Error, conn)),
            })
            .await;
    });

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get();
        tx3.send(()).unwrap();
        let _ = rx4
            .then(|r| match r {
                Ok(v) => ok((v, conn)),
                Err(_) => err((Error, conn)),
            })
            .await;
    });

    // Get the first connection.
    rx1.await.unwrap();
    // Get the second connection.
    rx3.await.unwrap();

    let (tx5, mut rx5) = oneshot::channel();
    let (tx6, rx6) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx5.send(()).unwrap();
        let _ = rx6
            .then(|r| match r {
                Ok(v) => ok((v, conn)),
                Err(_) => err((Error, conn)),
            })
            .await;
    });

    {
        let rx5_ref = &mut rx5;
        // NB: The channel needs to run on a Task, so shove it onto
        // the tokio event loop with a lazy.
        assert_eq!(lazy(|cx| Pin::new(rx5_ref).poll(cx)).await, Poll::Pending);
    }

    // Release the first connection.
    tx2.send(()).unwrap();

    rx5.await.unwrap();
    tx4.send(()).unwrap();
    tx6.send(()).unwrap();
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<Pool<OkManager<FakeConnection>>>();
}

// A connection manager that always returns `true` for `has_broken()`
#[derive(Default)]
struct BrokenConnectionManager<C> {
    _c: PhantomData<C>,
}

impl<C: Default + Send + Sync + 'static> ManageConnection for BrokenConnectionManager<C> {
    type Connection = C;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(C::default())
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        true
    }
}

#[tokio::test]
async fn test_drop_on_broken() {
    static DROPPED: AtomicBool = AtomicBool::new(false);

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    let pool = Pool::builder()
        .build(BrokenConnectionManager::<Connection>::default())
        .await
        .unwrap();

    {
        let _ = pool.get().await.unwrap();
    }

    assert!(DROPPED.load(Ordering::SeqCst));
    assert_eq!(pool.state().statistics.connections_closed_broken, 1);
}

#[tokio::test]
async fn test_initialization_failure() {
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let res = Pool::builder()
        .connection_timeout(Duration::from_secs(5))
        .max_size(1)
        .min_idle(Some(1))
        .build(manager)
        .await;
    assert_eq!(res.unwrap_err(), Error);
}

#[tokio::test]
async fn test_lazy_initialization_failure() {
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let pool = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build_unchecked(manager);

    let res = pool.get().await;
    assert_eq!(res.unwrap_err(), RunError::TimedOut);
}

#[tokio::test]
async fn test_lazy_initialization_failure_no_retry() {
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let pool = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .retry_connection(false)
        .build_unchecked(manager);

    let res = pool.get().await;
    assert_eq!(res.unwrap_err(), RunError::TimedOut);
}

#[tokio::test]
async fn test_get_timeout() {
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(100))
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        let _ = rx2
            .map(|r| match r {
                Ok(v) => Ok((v, conn)),
                Err(_) => Err((Error, conn)),
            })
            .await;
    });

    // Get the first connection.
    assert!(rx1.await.is_ok());
    pool.get().await.unwrap_err();

    tx2.send(()).unwrap();
    let r: Result<(), ()> = Ok(());
    ready(r).await.unwrap();
}

#[tokio::test]
async fn test_lots_of_waiters() {
    let pool = Pool::builder()
        .max_size(3)
        .connection_timeout(Duration::from_millis(5_000))
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let mut waiters: Vec<oneshot::Receiver<()>> = Vec::new();

    for _ in 0..25000 {
        let pool = pool.clone();
        let (tx, rx) = oneshot::channel();
        waiters.push(rx);
        tokio::spawn(async move {
            let _conn = pool.get().await.unwrap();
            tx.send(()).unwrap();
        });
    }

    let results = futures_util::future::join_all(&mut waiters).await;

    for result in results {
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_timeout_caller() {
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(5_000))
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let one = pool.get().await;
    assert!(one.is_ok());

    let res = tokio::time::timeout(Duration::from_millis(100), pool.get()).await;
    assert!(res.is_err());

    drop(one);

    let two = pool.get().await;
    assert!(two.is_ok());
}

#[tokio::test]
async fn test_now_invalid() {
    static INVALID: AtomicBool = AtomicBool::new(false);

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = FakeConnection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            if INVALID.load(Ordering::SeqCst) {
                Err(Error)
            } else {
                Ok(FakeConnection)
            }
        }

        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
            println!("Called is_valid");
            if INVALID.load(Ordering::SeqCst) {
                println!("Not");
                Err(Error)
            } else {
                println!("Is");
                Ok(())
            }
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_size(2)
        .min_idle(Some(2))
        .connection_timeout(Duration::from_secs(1))
        .build(Handler)
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        let _ = rx2
            .map(|r| match r {
                Ok(v) => Ok((v, conn)),
                Err(_) => Err((Error, conn)),
            })
            .await;
    });

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx3.send(()).unwrap();
        let _ = rx4
            .map(|r| match r {
                Ok(v) => Ok((v, conn)),
                Err(_) => Err((Error, conn)),
            })
            .await;
    });

    // Get the first connection.
    rx1.await.unwrap();
    // Get the second connection.
    rx3.await.unwrap();

    INVALID.store(true, Ordering::SeqCst);

    tx2.send(()).unwrap();
    tx4.send(()).unwrap();

    // Go idle for a bit
    assert!(timeout(Duration::from_secs(3), pending::<()>())
        .await
        .is_err());

    // Now try to get a new connection.
    let r = pool.get().await;
    assert!(r.is_err());

    // both connections in the pool were considered invalid
    assert_eq!(pool.state().statistics.connections_closed_invalid, 2);
}

#[tokio::test]
async fn test_idle_timeout() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct Connection;
    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let manager = NthConnectionFailManager::<Connection>::new(5);
    let pool = Pool::builder()
        .idle_timeout(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .reaper_rate(Duration::from_secs(1))
        .max_size(5)
        .min_idle(Some(5))
        .build(manager)
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        // NB: If we sleep here we'll block this thread's event loop, and the
        // reaper can't run.
        let _ = rx2
            .map(|r| match r {
                Ok(v) => Ok((v, conn)),
                Err(_) => Err((Error, conn)),
            })
            .await;
    });

    rx1.await.unwrap();

    // And wait.
    assert!(timeout(Duration::from_secs(2), pending::<()>())
        .await
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 4);

    tx2.send(()).unwrap();

    // And wait some more.
    assert!(timeout(Duration::from_secs(3), pending::<()>())
        .await
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 5);

    // all 5 idle connections were closed due to max idle time
    assert_eq!(pool.state().statistics.connections_closed_idle_timeout, 5);
}

#[tokio::test]
async fn test_max_lifetime() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let manager = NthConnectionFailManager::<Connection>::new(5);
    let pool = Pool::builder()
        .max_lifetime(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .reaper_rate(Duration::from_secs(1))
        .max_size(5)
        .min_idle(Some(5))
        .build(manager)
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        // NB: If we sleep here we'll block this thread's event loop, and the
        // reaper can't run.
        let _ = rx2
            .map(|r| match r {
                Ok(v) => Ok((v, conn)),
                Err(_) => Err((Error, conn)),
            })
            .await;
    });

    rx1.await.unwrap();

    // And wait.
    assert!(timeout(Duration::from_secs(2), pending::<()>())
        .await
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 4);
    tx2.send(()).unwrap();

    // And wait some more.
    assert!(timeout(Duration::from_secs(2), pending::<()>())
        .await
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 5);

    // all 5 connections were closed due to max lifetime
    assert_eq!(pool.state().statistics.connections_closed_max_lifetime, 5);
}

#[tokio::test]
async fn test_max_lifetime_reap_on_drop() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let manager = OkManager::<Connection>::new();
    let pool = Pool::builder()
        .max_lifetime(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .reaper_rate(Duration::from_secs(999))
        .build(manager)
        .await
        .unwrap();

    let conn = pool.get().await;

    // And wait.
    sleep(Duration::from_secs(2)).await;
    assert_eq!(DROPPED.load(Ordering::SeqCst), 0);

    // Connection is reaped on drop.
    drop(conn);
    assert_eq!(DROPPED.load(Ordering::SeqCst), 1);
    assert_eq!(pool.state().statistics.connections_closed_max_lifetime, 1);
}

#[tokio::test]
async fn test_min_idle() {
    let pool = Pool::builder()
        .max_size(5)
        .min_idle(Some(2))
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(2, state.connections);

    let mut rx = Vec::with_capacity(3);
    let mut tx = Vec::with_capacity(3);
    for _ in 0..3 {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        let clone = pool.clone();
        tokio::spawn(async move {
            let conn = clone.get().await.unwrap();
            tx1.send(()).unwrap();
            let _ = rx2
                .map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
                .await;
        });
        rx.push(rx1);
        tx.push(tx2);
    }

    FuturesUnordered::from_iter(rx)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(5, state.connections);

    for tx in tx.into_iter() {
        tx.send(()).unwrap();
    }

    // And wait for the connections to return.
    assert!(timeout(Duration::from_secs(1), pending::<()>())
        .await
        .is_err());

    let state = pool.state();
    assert_eq!(5, state.idle_connections);
    assert_eq!(5, state.connections);
}

#[tokio::test]
async fn test_conns_drop_on_pool_drop() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Connection)
        }

        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_lifetime(Some(Duration::from_secs(10)))
        .max_size(10)
        .min_idle(Some(10))
        .build(Handler)
        .await
        .unwrap();

    drop(pool);

    for _ in 0u8..10 {
        if DROPPED.load(Ordering::SeqCst) == 10 {
            return;
        }

        assert!(timeout(Duration::from_secs(1), pending::<()>())
            .await
            .is_err());
    }

    panic!(
        "Timed out waiting for connections to drop, {} dropped",
        DROPPED.load(Ordering::SeqCst)
    );
}

// make sure that bb8 retries after is_valid fails once
#[tokio::test]
async fn test_retry() {
    static FAILED_ONCE: AtomicBool = AtomicBool::new(false);

    struct Connection;
    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Connection)
        }

        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
            // only fail once so the retry should work
            if FAILED_ONCE.load(Ordering::SeqCst) {
                Ok(())
            } else {
                FAILED_ONCE.store(true, Ordering::SeqCst);
                Err(Error)
            }
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder().max_size(1).build(Handler).await.unwrap();

    // is_valid() will be called between the 2 iterations
    for _ in 0..2 {
        let _ = pool.get().await.unwrap();
    }
}

#[tokio::test]
async fn test_conn_fail_once() {
    static FAILED_ONCE: AtomicBool = AtomicBool::new(false);
    static NB_CALL: AtomicUsize = AtomicUsize::new(0);

    struct Connection;
    struct Handler;

    impl Connection {
        fn inc(&mut self) {
            NB_CALL.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            // only fail once so the retry should work
            if FAILED_ONCE.load(Ordering::SeqCst) {
                Ok(Connection)
            } else {
                FAILED_ONCE.store(true, Ordering::SeqCst);
                Err(Error)
            }
        }

        async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder().max_size(1).build(Handler).await.unwrap();

    for _ in 0..2 {
        pool.get().await.unwrap().inc();
    }

    assert_eq!(NB_CALL.load(Ordering::SeqCst), 2);
}

// This mimics the test_acquire_release test, but using the `get()` API.
#[tokio::test]
async fn test_guard() {
    let pool = Pool::builder()
        .max_size(2)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await;
        tx1.send(()).unwrap();
        let res = rx2
            .then(|r| match r {
                Ok(()) => ok(()),
                Err(_) => err(Error),
            })
            .await
            .map(|_| ());
        drop(conn);
        res
    });

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await;
        tx3.send(()).unwrap();
        let res = rx4
            .then(|r| match r {
                Ok(()) => ok(()),
                Err(_) => err(Error),
            })
            .await
            .map(|_| ());
        drop(conn);
        res
    });

    // Get the first connection.
    rx1.await.unwrap();
    // Get the second connection.
    rx3.await.unwrap();

    let (tx5, mut rx5) = oneshot::channel();
    let (tx6, rx6) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await;
        tx5.send(()).unwrap();
        let res = rx6
            .then(|r| match r {
                Ok(()) => ok(()),
                Err(_) => err(Error),
            })
            .await
            .map(|_| ());
        drop(conn);
        res
    });

    {
        let rx5_ref = &mut rx5;
        // NB: The channel needs to run on a Task, so shove it onto
        // the tokio event loop with a lazy.
        assert_eq!(lazy(|cx| Pin::new(rx5_ref).poll(cx)).await, Poll::Pending);
    }

    // Release the first connection.
    tx2.send(()).unwrap();

    rx5.await.unwrap();
    tx4.send(()).unwrap();
    tx6.send(()).unwrap();
}

#[tokio::test]
async fn test_customize_connection_acquire() {
    #[derive(Debug, Default)]
    struct Connection {
        custom_field: usize,
    }

    #[derive(Debug, Default)]
    struct CountingCustomizer {
        count: AtomicUsize,
    }

    impl<E: 'static> CustomizeConnection<Connection, E> for CountingCustomizer {
        fn on_acquire<'a>(
            &'a self,
            connection: &'a mut Connection,
        ) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>> {
            Box::pin(async move {
                connection.custom_field = 1 + self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    let pool = Pool::builder()
        .max_size(2)
        .connection_customizer(Box::<CountingCustomizer>::default())
        .build(OkManager::<Connection>::new())
        .await
        .unwrap();

    // Each connection gets customized
    {
        let connection_1 = pool.get().await.unwrap();
        assert_eq!(connection_1.custom_field, 1);
        let connection_2 = pool.get().await.unwrap();
        assert_eq!(connection_2.custom_field, 2);
    }

    // Connections don't get customized again on re-use
    let connection_1_or_2 = pool.get().await.unwrap();
    assert!(connection_1_or_2.custom_field == 1 || connection_1_or_2.custom_field == 2);
}

#[tokio::test]
async fn test_broken_connections_dont_starve_pool() {
    use std::sync::RwLock;
    use std::{convert::Infallible, time::Duration};

    #[derive(Default)]
    struct ConnectionManager {
        counter: RwLock<u16>,
    }
    #[derive(Debug)]
    struct Connection;

    impl bb8::ManageConnection for ConnectionManager {
        type Connection = Connection;
        type Error = Infallible;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Connection)
        }

        async fn is_valid(&self, _: &mut Self::Connection) -> Result<(), Self::Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            let mut counter = self.counter.write().unwrap();
            let res = *counter < 5;
            *counter += 1;
            res
        }
    }

    let pool = bb8::Pool::builder()
        .max_size(5)
        .connection_timeout(Duration::from_secs(10))
        .build(ConnectionManager::default())
        .await
        .unwrap();

    let mut futures = Vec::new();

    for _ in 0..10 {
        let pool = pool.clone();
        futures.push(tokio::spawn(async move {
            let conn = pool.get().await.unwrap();
            drop(conn);
        }));
    }

    for future in futures {
        future.await.unwrap();
    }
}

#[tokio::test]
async fn test_state_get_contention() {
    let pool = Pool::builder()
        .max_size(1)
        .min_idle(1)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let conn = clone.get().await.unwrap();
        tx1.send(()).unwrap();
        let _ = rx2
            .then(|r| match r {
                Ok(v) => ok((v, conn)),
                Err(_) => err((Error, conn)),
            })
            .await;
    });

    // Get the first connection.
    rx1.await.unwrap();

    // Now try to get a new connection without waiting.
    let f = pool.get();

    // Release the first connection.
    tx2.send(()).unwrap();

    // Wait for the second attempt to get a connection.
    f.await.unwrap();

    let statistics = pool.state().statistics;
    assert_eq!(statistics.get_direct, 1);
    assert_eq!(statistics.get_waited, 1);
    assert!(statistics.get_wait_time > Duration::from_micros(0));
}

#[tokio::test]
async fn test_statistics_connections_created() {
    let pool = Pool::builder()
        .max_size(1)
        .min_idle(1)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();
    let (tx1, rx1) = oneshot::channel();
    let clone = pool.clone();
    tokio::spawn(async move {
        let _ = clone.get().await.unwrap();
        tx1.send(()).unwrap();
    });
    // wait until finished.
    rx1.await.unwrap();

    assert_eq!(pool.state().statistics.connections_created, 1);
}

#[tokio::test]
async fn test_can_use_added_connections() {
    let pool = Pool::builder()
        .connection_timeout(Duration::from_millis(1))
        .build_unchecked(NthConnectionFailManager::<FakeConnection>::new(0));

    // Assert pool can't replenish connections on its own
    let res = pool.get().await;
    assert_eq!(res.unwrap_err(), RunError::TimedOut);

    pool.add(FakeConnection).unwrap();
    let res = pool.get().await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_add_ok_until_max_size() {
    let pool = Pool::builder()
        .min_idle(1)
        .max_size(3)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    for _ in 0..2 {
        let conn = pool.dedicated_connection().await.unwrap();
        pool.add(conn).unwrap();
    }

    let conn = pool.dedicated_connection().await.unwrap();
    let res = pool.add(conn);
    assert!(matches!(res, Err(AddError::NoCapacity(_))));
}

#[tokio::test]
async fn test_add_checks_broken_connections() {
    let pool = Pool::builder()
        .min_idle(1)
        .max_size(3)
        .build(BrokenConnectionManager::<FakeConnection>::default())
        .await
        .unwrap();

    let conn = pool.dedicated_connection().await.unwrap();
    let res = pool.add(conn);
    assert!(matches!(res, Err(AddError::Broken(_))));
}

#[tokio::test]
async fn test_reuse_on_drop() {
    let pool = Pool::builder()
        .min_idle(0)
        .max_size(100)
        .queue_strategy(QueueStrategy::Lifo)
        .build(OkManager::<FakeConnection>::new())
        .await
        .unwrap();

    // The first get should
    // 1) see nothing in the pool,
    // 2) spawn a single replenishing approval,
    // 3) get notified of the new connection and grab it from the pool
    let conn_0 = pool.get().await.expect("should connect");
    // Dropping the connection queues up a notify
    drop(conn_0);

    // The second get should
    // 1) see the first connection in the pool and grab it
    let _conn_1 = pool.get().await.expect("should connect");

    // The third get will
    // 1) see nothing in the pool,
    // 2) spawn a single replenishing approval,
    // 3) get notified of the new connection,
    // 4) see nothing in the pool,
    // 5) _not_ spawn a single replenishing approval,
    // 6) get notified of the new connection and grab it from the pool
    let _conn_2 = pool.get().await.expect("should connect");

    assert_eq!(pool.state().connections, 2);
}
