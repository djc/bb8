use super::*;

use std::error;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT};

use futures::Async;
use futures::future::{err, join_all, Executor};
use tokio_core::reactor::Core;

#[derive(Debug)]
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

impl Bb8Error for Error {
    fn timed_out() -> Self {
        Error
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

impl<C> ManageConnection for OkManager<C>
    where C: Default + Send + Sync + 'static
{
    type Connection = C;
    type Error = Error;

    fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send> {
        Box::new(ok(Default::default()))
    }

    fn is_valid(&self, _: &mut Self::Connection) -> Box<Future<Item = (), Error = Self::Error>> {
        Box::new(ok(()))
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

impl<C> ManageConnection for NthConnectionFailManager<C>
    where C: Default + Send + Sync + 'static
{
    type Connection = C;
    type Error = Error;

    fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send> {
        let mut n = self.n.lock().unwrap();
        if *n > 0 {
            *n -= 1;
            Box::new(ok(Default::default()))
        } else {
            Box::new(err(Error))
        }
    }

    fn is_valid(&self, _: &mut Self::Connection) -> Box<Future<Item = (), Error = Self::Error>> {
        Box::new(ok(()))
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[test]
fn test_max_size_ok() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let manager = NthConnectionFailManager::<FakeConnection>::new(5);
    let pool = event_loop.run(Pool::builder()
            .max_size(5)
            .build(manager, remote))
        .unwrap();
    let mut channels = Vec::with_capacity(5);
    let mut ignored = Vec::with_capacity(5);
    for _ in 0..5 {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        event_loop.execute(pool.run(move |conn| {
                    tx1.send(()).unwrap();
                    rx2.then(|r| match r {
                        Ok(v) => Ok((v, conn)),
                        Err(_) => Err((Error, conn)),
                    })
                })
                .map_err(|_| ()))
            .unwrap();
        channels.push(rx1);
        ignored.push(tx2);
    }
    assert_eq!(channels.len(), 5);
    assert_eq!(event_loop.run(join_all(channels))
                   .unwrap()
                   .len(),
               5);
}

#[test]
fn test_acquire_release() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder()
            .max_size(2)
            .build(OkManager::<FakeConnection>::new(), remote))
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx1.send(()).unwrap();
                rx2.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx3.send(()).unwrap();
                rx4.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    // Get the first connection.
    event_loop.run(rx1).unwrap();
    // Get the second connection.
    event_loop.run(rx3).unwrap();

    let (tx5, mut rx5) = oneshot::channel();
    let (tx6, rx6) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx5.send(()).unwrap();
                rx6.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    event_loop.turn(Some(Duration::from_millis(100)));
    {
        let rx5_ref = &mut rx5;
        // NB: The channel needs to run on a Task, so shove it onto
        // the tokio event loop with a lazy.
        assert_eq!(event_loop.run(lazy(|| rx5_ref.poll())), Ok(Async::NotReady));
    }

    // Release the first connection.
    tx2.send(()).unwrap();

    event_loop.run(rx5).unwrap();
    tx4.send(()).unwrap();
    tx6.send(()).unwrap();

    let r: Result<(), ()> = Ok(());
    event_loop.run(r.into_future()).unwrap();
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<Pool<OkManager<FakeConnection>>>();
}

#[test]
fn test_drop_on_broken() {
    static DROPPED: AtomicBool = ATOMIC_BOOL_INIT;

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;


    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send> {
            Box::new(ok(Default::default()))
        }

        fn is_valid(&self,
                    _: &mut Self::Connection)
                    -> Box<Future<Item = (), Error = Self::Error>> {
            Box::new(ok(()))
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            true
        }
    }

    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder().build(Handler, remote)).unwrap();

    event_loop.run(pool.run(move |conn| {
                let r: Result<_, (Error, _)> = Ok(((), conn));
                r.into_future()
            })
            .map_err(|_| ()))
        .unwrap();

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let e = event_loop.run(Pool::builder()
        .max_size(1)
        .min_idle(Some(1))
        .build(manager, remote)
        .map(|_| ()));
    assert!(e.is_err());
    assert!(e.unwrap_err().to_string().contains("blammo"));
}

#[test]
fn test_lazy_initialization_failure() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let manager = NthConnectionFailManager::<FakeConnection>::new(0);
    let pool = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build_unchecked(manager, remote);

    let e = event_loop.run(pool.run(move |conn| {
        let r: Result<_, (Error, _)> = Ok(((), conn));
        r.into_future()
    }));
    assert!(e.is_err());
    assert!(e.unwrap_err().to_string().contains("blammo"));
}

#[test]
fn test_get_timeout() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_millis(100))
            .build(OkManager::<FakeConnection>::new(), remote))
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx1.send(()).unwrap();
                rx2.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    // Get the first connection.
    assert!(event_loop.run(rx1).is_ok());

    let e = event_loop.run(pool.run(move |conn| {
            let r: Result<_, (Error, _)> = Ok(((), conn));
            r.into_future()
        })
        .map_err(|_| ()));
    assert!(e.is_err());

    tx2.send(()).unwrap();
    let r: Result<(), ()> = Ok(());
    event_loop.run(r.into_future()).unwrap();
}

#[test]
fn test_now_invalid() {
    static INVALID: AtomicBool = ATOMIC_BOOL_INIT;

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = FakeConnection;
        type Error = Error;

        fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send> {
            let r = if INVALID.load(Ordering::SeqCst) {
                Err(Error)
            } else {
                Ok(FakeConnection)
            };
            Box::new(r.into_future())
        }

        fn is_valid(&self,
                    _: &mut Self::Connection)
                    -> Box<Future<Item = (), Error = Self::Error>> {
            let r = if INVALID.load(Ordering::SeqCst) {
                Err(Error)
            } else {
                Ok(())
            };
            Box::new(r.into_future())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder()
            .max_size(2)
            .min_idle(Some(2))
            .connection_timeout(Duration::from_secs(1))
            .build(Handler, remote))
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx1.send(()).unwrap();
                rx2.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    let (tx3, rx3) = oneshot::channel();
    let (tx4, rx4) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx3.send(()).unwrap();
                rx4.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    // Get the first connection.
    event_loop.run(rx1).unwrap();
    // Get the second connection.
    event_loop.run(rx3).unwrap();

    INVALID.store(true, Ordering::SeqCst);

    tx2.send(()).unwrap();
    tx4.send(()).unwrap();

    // Go idle for a bit
    let timeout = Timeout::new(Duration::from_secs(2), &event_loop.handle()).unwrap();
    event_loop.run(timeout).unwrap();

    // Now try to get a new connection.
    let r = event_loop.run(pool.run(move |conn| {
            let r: Result<_, (Error, _)> = Ok(((), conn));
            r
        })
        .map_err(|_| ()));
    assert!(r.is_err());
}

#[test]
fn test_max_lifetime() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

    #[derive(Default)]
    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let manager = NthConnectionFailManager::<Connection>::new(5);
    let pool = event_loop.run(Pool::builder()
            .max_lifetime(Some(Duration::from_secs(1)))
            .connection_timeout(Duration::from_secs(1))
            .reaper_rate(Duration::from_secs(1))
            .max_size(5)
            .min_idle(Some(5))
            .build(manager, remote))
        .unwrap();

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();
    event_loop.execute(pool.run(move |conn| {
                tx1.send(()).unwrap();
                // NB: If we sleep here we'll block this thread's event loop, and the
                // reaper can't run.
                rx2.then(|r| match r {
                    Ok(v) => Ok((v, conn)),
                    Err(_) => Err((Error, conn)),
                })
            })
            .map_err(|_| ()))
        .unwrap();

    event_loop.run(rx1).unwrap();

    // And wait.
    let timeout = Timeout::new(Duration::from_secs(2), &event_loop.handle()).unwrap();
    event_loop.run(timeout).unwrap();
    assert_eq!(DROPPED.load(Ordering::SeqCst), 4);
    tx2.send(()).unwrap();

    // And wait some more.
    let timeout = Timeout::new(Duration::from_secs(2), &event_loop.handle()).unwrap();
    event_loop.run(timeout).unwrap();
    assert_eq!(DROPPED.load(Ordering::SeqCst), 5);
}

#[test]
fn test_min_idle() {
    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder()
            .max_size(5)
            .min_idle(Some(2))
            .build(OkManager::<FakeConnection>::new(), remote))
        .unwrap();

    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(2, state.connections);

    let mut rx = Vec::with_capacity(3);
    let mut tx = Vec::with_capacity(3);
    for _ in 0..3 {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        event_loop.execute(pool.run(|conn| {
                    tx1.send(()).unwrap();
                    rx2.then(|r| match r {
                        Ok(v) => Ok((v, conn)),
                        Err(_) => Err((Error, conn)),
                    })
                })
                .map_err(|_| ()))
            .unwrap();
        rx.push(rx1);
        tx.push(tx2);
    }

    event_loop.run(FuturesUnordered::from_iter(rx).collect()).unwrap();
    let state = pool.state();
    assert_eq!(2, state.idle_connections);
    assert_eq!(5, state.connections);

    for tx in tx.into_iter() {
        tx.send(()).unwrap();
    }

    // And wait for the connections to return.
    let timeout = Timeout::new(Duration::from_secs(1), &event_loop.handle()).unwrap();
    event_loop.run(timeout).unwrap();

    let state = pool.state();
    assert_eq!(5, state.idle_connections);
    assert_eq!(5, state.connections);
}

#[test]
fn test_conns_drop_on_pool_drop() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

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

        fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send> {
            Box::new(ok(Connection))
        }

        fn is_valid(&self,
                    _: &mut Self::Connection)
                    -> Box<Future<Item = (), Error = Self::Error>> {
            Box::new(ok(()))
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    let mut event_loop = Core::new().unwrap();
    let remote = event_loop.remote();
    let pool = event_loop.run(Pool::builder()
            .max_lifetime(Some(Duration::from_secs(10)))
            .max_size(10)
            .min_idle(Some(10))
            .build(Handler, remote))
        .unwrap();

    mem::drop(pool);

    for _ in 0..10 {
        if DROPPED.load(Ordering::SeqCst) == 10 {
            return;
        }

        event_loop.turn(Some(Duration::from_secs(1)));
    }

    panic!("Timed out waiting for connections to drop, {} dropped",
           DROPPED.load(Ordering::SeqCst));
}
