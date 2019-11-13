use bb8::*;

use std::iter::FromIterator;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use std::{error, fmt, mem};

use async_trait::async_trait;
use futures::channel::oneshot;
use futures::future::{err, lazy, ok, pending, ready, try_join_all};
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Timeout;

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

#[async_trait]
impl<C> ManageConnection for OkManager<C>
where
    C: Default + Send + Sync + 'static,
{
    type Connection = C;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(Default::default())
    }

    async fn is_valid(
        &self,
        conn: Self::Connection,
    ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        Ok(conn)
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

#[async_trait]
impl<C> ManageConnection for NthConnectionFailManager<C>
where
    C: Default + Send + Sync + 'static,
{
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

    async fn is_valid(
        &self,
        conn: Self::Connection,
    ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        Ok(conn)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[test]
fn test_max_size_ok() {
    let mut event_loop = Runtime::new().unwrap();
    let manager = NthConnectionFailManager::<FakeConnection>::new(5);
    let pool = event_loop
        .block_on(async { Pool::builder().max_size(5).build(manager).await })
        .unwrap();
    let mut channels = Vec::with_capacity(5);
    let mut ignored = Vec::with_capacity(5);
    for _ in 0..5 {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel::<()>();
        let pool = pool.clone();
        event_loop.spawn(async move {
            pool.run(move |conn| {
                tx1.send(()).unwrap();
                rx2.map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
        });
        channels.push(rx1);
        ignored.push(tx2);
    }
    assert_eq!(channels.len(), 5);
    assert_eq!(
        event_loop.block_on(try_join_all(channels)).unwrap().len(),
        5
    );
}

#[test]
fn test_acquire_release() {
    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_size(2)
                .build(OkManager::<FakeConnection>::new())
                .await
        })
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx1.send(()).unwrap();
                rx2.then(|r| match r {
                    Ok(v) => ok((v, conn)),
                    Err(_) => err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx3.send(()).unwrap();
                rx4.then(|r| match r {
                    Ok(v) => ok((v, conn)),
                    Err(_) => err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    // Get the first connection.
    event_loop.block_on(rx1).unwrap();
    // Get the second connection.
    event_loop.block_on(rx3).unwrap();

    let (tx5, mut rx5) = oneshot::channel();
    let (tx6, rx6) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx5.send(()).unwrap();
                rx6.then(|r| match r {
                    Ok(v) => ok((v, conn)),
                    Err(_) => err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    {
        let rx5_ref = &mut rx5;
        // NB: The channel needs to run on a Task, so shove it onto
        // the tokio event loop with a lazy.
        assert_eq!(
            event_loop.block_on(lazy(|cx| Pin::new(rx5_ref).poll(cx))),
            Poll::Pending
        );
    }

    // Release the first connection.
    tx2.send(()).unwrap();

    event_loop.block_on(rx5).unwrap();
    tx4.send(()).unwrap();
    tx6.send(()).unwrap();
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<Pool<OkManager<FakeConnection>>>();
}

#[test]
fn test_drop_on_broken() {
    static DROPPED: AtomicBool = AtomicBool::new(false);

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;

    #[async_trait]
    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Default::default())
        }

        async fn is_valid(
            &self,
            conn: Self::Connection,
        ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
            Ok(conn)
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            true
        }
    }

    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async { Pool::builder().build(Handler).await })
        .unwrap();

    event_loop
        .block_on(pool.run(move |conn| {
            let r: Result<_, (Error, _)> = Ok(((), conn));
            ready(r)
        }))
        .unwrap();

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let mut event_loop = Runtime::new().unwrap();
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let e = event_loop.block_on(async {
        Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
    });
    assert!(e.is_err());
    assert_eq!(e.unwrap_err(), Error);
}

#[test]
fn test_lazy_initialization_failure() {
    let mut event_loop = Runtime::new().unwrap();
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let pool = event_loop.block_on(async {
        Pool::builder()
            .connection_timeout(Duration::from_secs(1))
            .build_unchecked(manager)
    });

    let e = event_loop.block_on(pool.run(move |conn| {
        let r: Result<_, (Error, _)> = Ok(((), conn));
        ready(r)
    }));
    assert!(e.is_err());
    assert_eq!(e.unwrap_err(), bb8::RunError::TimedOut);
}

#[test]
fn test_get_timeout() {
    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_size(1)
                .connection_timeout(Duration::from_millis(100))
                .build(OkManager::<FakeConnection>::new())
                .await
        })
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx1.send(()).unwrap();
                rx2.map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    // Get the first connection.
    assert!(event_loop.block_on(rx1).is_ok());

    let e = event_loop.block_on(
        pool.run(move |conn| {
            let r: Result<_, (Error, _)> = Ok(((), conn));
            ready(r)
        })
        .map_err(|_| ()),
    );
    assert!(e.is_err());

    tx2.send(()).unwrap();
    let r: Result<(), ()> = Ok(());
    event_loop.block_on(ready(r)).unwrap();
}

#[test]
fn test_now_invalid() {
    static INVALID: AtomicBool = AtomicBool::new(false);

    struct Handler;

    #[async_trait]
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

        async fn is_valid(
            &self,
            conn: Self::Connection,
        ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
            println!("Called is_valid");
            if INVALID.load(Ordering::SeqCst) {
                println!("Not");
                Err((Error, conn))
            } else {
                println!("Is");
                Ok(conn)
            }
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_size(2)
                .min_idle(Some(2))
                .connection_timeout(Duration::from_secs(1))
                .build(Handler)
                .await
        })
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx1.send(()).unwrap();
                rx2.map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx3.send(()).unwrap();
                rx4.map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    // Get the first connection.
    event_loop.block_on(rx1).unwrap();
    // Get the second connection.
    event_loop.block_on(rx3).unwrap();

    INVALID.store(true, Ordering::SeqCst);

    tx2.send(()).unwrap();
    tx4.send(()).unwrap();

    // Go idle for a bit
    assert!(event_loop
        .block_on(Timeout::new(pending::<()>(), Duration::from_secs(3)))
        .is_err());

    // Now try to get a new connection.
    let r = event_loop.block_on(
        pool.run(move |conn| {
            let r: Result<_, (Error, _)> = Ok(((), conn));
            ready(r)
        })
        .map_err(|_| ()),
    );
    assert!(r.is_err());
}

#[test]
fn test_max_lifetime() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut event_loop = Runtime::new().unwrap();
    let manager = NthConnectionFailManager::<Connection>::new(5);
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_lifetime(Some(Duration::from_secs(1)))
                .connection_timeout(Duration::from_secs(1))
                .reaper_rate(Duration::from_secs(1))
                .max_size(5)
                .min_idle(Some(5))
                .build(manager)
                .await
        })
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    let clone = pool.clone();
    event_loop.spawn(async move {
        clone
            .run(move |conn| {
                tx1.send(()).unwrap();
                // NB: If we sleep here we'll block this thread's event loop, and the
                // reaper can't run.
                rx2.map(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map(|_| ())
            .await
    });

    event_loop.block_on(rx1).unwrap();

    // And wait.
    assert!(event_loop
        .block_on(Timeout::new(pending::<()>(), Duration::from_secs(2)))
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 4);
    tx2.send(()).unwrap();

    // And wait some more.
    assert!(event_loop
        .block_on(Timeout::new(pending::<()>(), Duration::from_secs(2)))
        .is_err());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 5);
}

#[test]
fn test_min_idle() {
    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_size(5)
                .min_idle(Some(2))
                .build(OkManager::<FakeConnection>::new())
                .await
        })
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
        event_loop.spawn(async move {
            clone
                .run(|conn| {
                    tx1.send(()).unwrap();
                    rx2.map(|r| match r {
                        Ok(v) => Ok((v, conn)),
                        Err(_) => Err((Error, conn)),
                    })
                })
                .map(|_| ())
                .await
        });
        rx.push(rx1);
        tx.push(tx2);
    }

    event_loop
        .block_on(FuturesUnordered::from_iter(rx).try_collect::<Vec<_>>())
        .unwrap();
    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(5, state.connections);

    for tx in tx.into_iter() {
        tx.send(()).unwrap();
    }

    // And wait for the connections to return.
    assert!(event_loop
        .block_on(Timeout::new(pending::<()>(), Duration::from_secs(1)))
        .is_err());

    let state = pool.state();
    assert_eq!(5, state.idle_connections);
    assert_eq!(5, state.connections);
}

#[test]
fn test_conns_drop_on_pool_drop() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler;

    #[async_trait]
    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Connection)
        }

        async fn is_valid(
            &self,
            conn: Self::Connection,
        ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
            Ok(conn)
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Runtime::new().unwrap();
    let pool = event_loop
        .block_on(async {
            Pool::builder()
                .max_lifetime(Some(Duration::from_secs(10)))
                .max_size(10)
                .min_idle(Some(10))
                .build(Handler)
                .await
        })
        .unwrap();

    mem::drop(pool);

    for _ in 0..10 {
        if DROPPED.load(Ordering::SeqCst) == 10 {
            return;
        }

        assert!(event_loop
            .block_on(Timeout::new(pending::<()>(), Duration::from_secs(1)))
            .is_err());
    }

    panic!(
        "Timed out waiting for connections to drop, {} dropped",
        DROPPED.load(Ordering::SeqCst)
    );
}

// make sure that bb8 retries after is_valid fails once
#[test]
fn test_retry() {
    static FAILED_ONCE: AtomicBool = AtomicBool::new(false);

    struct Connection;
    struct Handler;

    #[async_trait]
    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        async fn connect(&self) -> Result<Self::Connection, Self::Error> {
            Ok(Connection)
        }

        async fn is_valid(
            &self,
            conn: Self::Connection,
        ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
            // only fail once so the retry should work
            if FAILED_ONCE.load(Ordering::SeqCst) {
                Ok(conn)
            } else {
                FAILED_ONCE.store(true, Ordering::SeqCst);
                Err((Error, conn))
            }
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Runtime::new().unwrap();

    let pool = event_loop
        .block_on(async { Pool::builder().max_size(1).build(Handler).await })
        .unwrap();

    // is_valid() will be called between the 2 iterations
    for _ in 0..2 {
        event_loop.block_on(async {
            pool.run(|c: Connection| {
                async { Ok::<((), Connection), (Error, Connection)>(((), c)) }
            })
            .await
            .unwrap();
        });
    }
}

#[test]
fn test_conn_fail_once() {
    static FAILED_ONCE: AtomicBool = AtomicBool::new(false);
    static NB_CALL: AtomicUsize = AtomicUsize::new(0);

    struct Connection;
    struct Handler;

    impl Connection {
        fn inc(&mut self) {
            NB_CALL.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
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

        async fn is_valid(
            &self,
            conn: Self::Connection,
        ) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
            Ok(conn)
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Runtime::new().unwrap();

    let pool = event_loop
        .block_on(async { Pool::builder().max_size(1).build(Handler).await })
        .unwrap();

    for _ in 0..2 {
        event_loop.block_on(async {
            pool.run(|mut c: Connection| {
                c.inc();
                async { Ok::<((), Connection), (Error, Connection)>(((), c)) }
            })
            .await
            .unwrap();
        });
    }

    assert_eq!(NB_CALL.load(Ordering::SeqCst), 2);
}
