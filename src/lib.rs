#[deny(missing_docs)]

extern crate futures;
extern crate tokio_core;

use std::cmp::{max, min};
use std::collections::VecDeque;
use std::fmt;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use futures::{Future, IntoFuture, Stream};
use futures::future::{lazy, ok, Either};
use futures::stream::FuturesUnordered;
use futures::sync::oneshot;
use tokio_core::reactor::{Interval, Handle, Remote, Timeout};

mod util;
use util::*;

#[cfg(test)]
mod test;

pub trait Bb8Error {
    fn timed_out() -> Self;
}

/// A trait which provides connection-specific functionality.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;
    /// The error type returned by `Connection`s.
    type Error: Bb8Error + Send + 'static;

    /// Attempts to create a new connection.
    fn connect(&self) -> Box<Future<Item = Self::Connection, Error = Self::Error> + Send>;
    /// Determines if the connection is still connected to the database.
    fn is_valid(&self) -> Box<Future<Item = (), Error = Self::Error>>;
    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
}

/// Information about the state of a `Pool`.
pub struct State {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
    _p: (),
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .finish()
    }
}

#[derive(Debug)]
struct Conn<C> {
    conn: C,
    birth: Instant,
}

struct IdleConn<C> {
    conn: Conn<C>,
    idle_start: Instant,
}

impl<C> IdleConn<C> {
    fn make_idle(conn: Conn<C>) -> IdleConn<C> {
        let now = Instant::now();
        IdleConn {
            conn: conn,
            idle_start: now,
        }
    }
}

/// A builder for a connection pool.
#[derive(Debug)]
pub struct Builder<M: ManageConnection> {
    /// The maximum number of connections allowed.
    max_size: u32,
    /// The minimum idle connection count the pool will attempt to maintain.
    min_idle: Option<u32>,
    /// The maximum lifetime, if any, that a connection is allowed.
    max_lifetime: Option<Duration>,
    /// The duration, if any, after which idle_connections in excess of `min_idle` are closed.
    idle_timeout: Option<Duration>,
    /// The duration to wait to start a connection before giving up.
    connection_timeout: Duration,
    /// The time interval used to wake up and reap connections.
    reaper_rate: Duration,
    _p: PhantomData<M>,
}

impl<M: ManageConnection> Default for Builder<M> {
    fn default() -> Self {
        Builder {
            max_size: 10,
            min_idle: None,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(30),
            reaper_rate: Duration::from_secs(30),
            _p: PhantomData,
        }
    }
}

impl<M: ManageConnection> Builder<M> {
    /// Constructs a new `Builder`.
    ///
    /// Parameters are initialized with their default values.
    pub fn new() -> Builder<M> {
        Default::default()
    }

    /// Sets the maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    pub fn max_size(mut self, max_size: u32) -> Builder<M> {
        assert!(max_size > 0, "max_size must be greater than zero!");
        self.max_size = max_size;
        self
    }

    /// Sets the minimum idle connection count maintained by the pool.
    ///
    /// If set, the pool will try to maintain at least this many idle
    /// connections at all times, while respecting the value of `max_size`.
    ///
    /// Defaults to None.
    pub fn min_idle(mut self, min_idle: Option<u32>) -> Builder<M> {
        self.min_idle = min_idle;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed at the next reaping after surviving
    /// past this duration.
    ///
    /// If a connection reachs its maximum lifetime while checked out it will be
    /// closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Builder<M> {
        assert!(max_lifetime != Some(Duration::from_secs(0)),
                "max_lifetime must be greater than zero!");
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout used by the pool.
    ///
    /// If set, idle connections in excess of `min_idle` will be closed at the
    /// next reaping after remaining idle past this duration.
    ///
    /// Defaults to 10 minutes.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder<M> {
        assert!(idle_timeout != Some(Duration::from_secs(0)),
                "idle_timeout must be greater than zero!");
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// Futures returned by `Pool::get` will wait this long before giving up and
    /// resolving with an error.
    ///
    /// Defaults to 30 seconds.
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder<M> {
        assert!(connection_timeout > Duration::from_secs(0),
                "connection_timeout must be non-zero");
        self.connection_timeout = connection_timeout;
        self
    }

    /// Used by tests
    #[allow(dead_code)]
    pub(crate) fn reaper_rate(mut self, reaper_rate: Duration) -> Builder<M> {
        self.reaper_rate = reaper_rate;
        self
    }

    fn build_inner(self,
                   manager: M,
                   event_loop: Remote)
                   -> (Pool<M>, Box<Future<Item = (), Error = M::Error> + Send>) {
        if let Some(min_idle) = self.min_idle {
            assert!(self.max_size >= min_idle,
                    "min_idle must be no larger than max_size");
        }

        let p = Pool::new_inner(self, manager, event_loop);
        let f = p.replenish_idle_connections();
        (p, f)
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    ///
    /// The `Pool` will not be returned until it has established its configured
    /// minimum number of connections, or it times out.
    pub fn build(self,
                 manager: M,
                 event_loop: Remote)
                 -> Box<Future<Item = Pool<M>, Error = M::Error>> {
        let (p, f) = self.build_inner(manager, event_loop);
        Box::new(f.map(|_| p))
    }

    /// Consumes the builder, returning a new, initialized `Pool`.
    ///
    /// Unlike `build`, this does not wait for any connections to be established
    /// before returning.
    pub fn build_unchecked(self, manager: M, event_loop: Remote) -> Pool<M> {
        let (p, f) = self.build_inner(manager, event_loop);
        p.proxy_or_dispatch(f.map_err(|_| ()));
        p
    }
}

/// The pool data that must be protected by a lock.
struct PoolInternals<C> {
    waiters: VecDeque<oneshot::Sender<Conn<C>>>,
    conns: VecDeque<IdleConn<C>>,
    num_conns: u32,
    pending_conns: u32,
}

impl<C> PoolInternals<C> {
    fn put_idle_conn(&mut self, mut conn: IdleConn<C>) {
        loop {
            if let Some(waiter) = self.waiters.pop_front() {
                // This connection is no longer idle, send it back out.
                match waiter.send(conn.conn) {
                    Ok(_) => break,
                    // Oops, that receiver was gone. Loop and try again.
                    Err(c) => conn.conn = c,
                }
            } else {
                // Queue it in the idle queue.
                self.conns.push_back(conn);
                break;
            }
        }
        println!("put_idle_conn: {} conns, {} pending, {} idle",
                 self.num_conns,
                 self.pending_conns,
                 self.conns.len());
    }
}

/// The guts of a `Pool`.
struct SharedPool<M: ManageConnection> {
    statics: Builder<M>,
    manager: M,
    event_loop: Remote,
    internals: Mutex<PoolInternals<M::Connection>>,
}

impl<M: ManageConnection> SharedPool<M> {
    fn proxy_or_dispatch<R>(&self, runnable: R)
        where R: IntoFuture<Item = (), Error = ()>,
              R::Future: Send + 'static
    {
        let runnable = runnable.into_future();
        match self.event_loop.handle() {
            // We're being called on the event loop.
            Some(handle) => handle.spawn(runnable),
            // We're being called from somewhere else.
            None => self.event_loop.spawn(move |_| runnable),
        }
    }


    fn or_timeout<'a, F>(&self, f: F) -> Box<Future<Item = Option<F::Item>, Error = F::Error> + 'a>
        where F: IntoFuture + Send,
              F::Future: 'a,
              F::Item: 'a,
              F::Error: 'a
    {
        let runnable = f.into_future();
        let event_loop = &self.event_loop;
        let f: Box<Future<Item = Option<F::Item>, Error = F::Error>> = match event_loop.handle() {
            // We're being called on the event loop. We can set up a timeout directly.
            Some(handle) => {
                let timeout = Timeout::new(self.statics.connection_timeout, &handle).unwrap();
                Box::new(runnable.select2(timeout)
                    .then(|r| match r {
                        Ok(Either::A((item, _))) => Ok(Some(item)),
                        Err(Either::A((error, _))) => Err(error),
                        Ok(Either::B(_)) |
                        Err(Either::B(_)) => Ok(None),
                    }))
            }
            // We're being called from somewhere else.
            None => {
                let (tx, rx) = oneshot::channel();
                let timeout = self.statics.connection_timeout;
                event_loop.spawn(move |handle| {
                    let timeout = Timeout::new(timeout, handle).unwrap();
                    timeout.then(|_| tx.send(()))
                });
                Box::new(runnable.select2(rx)
                    .then(|r| match r {
                        Ok(Either::A((item, _))) => Ok(Some(item)),
                        Err(Either::A((error, _))) => Err(error),
                        Ok(Either::B(_)) |
                        Err(Either::B(_)) => Ok(None),
                    }))
            }
        };
        f
    }
}

/// A generic connection pool.
#[derive(Clone)]
pub struct Pool<M: ManageConnection> {
    inner: Arc<SharedPool<M>>,
}

// Outside of Pool to avoid borrow splitting issues on self
// NB: This is called with the pool lock held.
fn add_connection<M>(pool: &Arc<SharedPool<M>>,
                     internals: &mut PoolInternals<M::Connection>)
                     -> Box<Future<Item = (), Error = M::Error> + Send>
    where M: ManageConnection
{
    println!("{} conns, {} pending",
             internals.num_conns,
             internals.pending_conns);
    assert!(internals.num_conns + internals.pending_conns < pool.statics.max_size);
    internals.pending_conns += 1;
    fn do_it<M>(pool: &Arc<SharedPool<M>>) -> Box<Future<Item = (), Error = M::Error> + Send>
        where M: ManageConnection
    {
        let new_shared = Arc::downgrade(pool);
        let (tx, rx) = oneshot::channel();
        pool.proxy_or_dispatch(ok(new_shared).and_then(|new_shared| {
            let f: Box<Future<Item = (), Error = ()> + Send> = match new_shared.upgrade() {
                None => Box::new(ok(())),
                Some(shared) => {
                    Box::new(shared.manager
                        .connect()
                        .then(move |result| {
                            let mut locked = shared.internals.lock().unwrap();
                            match result {
                                Ok(conn) => {
                                    let now = Instant::now();
                                    let conn = IdleConn {
                                        conn: Conn {
                                            conn: conn,
                                            birth: now,
                                        },
                                        idle_start: now,
                                    };
                                    locked.pending_conns -= 1;
                                    locked.num_conns += 1;
                                    locked.put_idle_conn(conn);
                                    tx.send(Ok(())).map_err(|_| ()).unwrap();
                                    Ok(())
                                }
                                Err(err) => {
                                    locked.pending_conns -= 1;
                                    // TODO: retry?
                                    tx.send(Err(err)).map_err(|_| ()).unwrap();
                                    Err(())
                                }
                            }
                        }))
                }
            };
            f
        }));
        Box::new(rx.then(|v| match v {
            Ok(o) => o,
            Err(_) => panic!(),
        }))
    }

    do_it(pool)
}

fn get_idle_connection<'a, M>
    (pool: &Arc<SharedPool<M>>,
     mut internals: MutexGuard<'a, PoolInternals<M::Connection>>)
     -> Result<Conn<M::Connection>, MutexGuard<'a, PoolInternals<M::Connection>>>
    where M: ManageConnection
{
    if let Some(conn) = internals.conns.pop_front() {
        // Spin up a new connection if necessary to retain our minimum idle count
        if internals.num_conns + internals.pending_conns < pool.statics.max_size {
            let f = Pool::replenish_idle_connections_locked(pool, &mut internals);
            pool.proxy_or_dispatch(f.map_err(|_| ()));
        }

        // Go ahead and release the lock here.
        mem::drop(internals);

        // XXXkhuey todo test_on_check_out
        Ok(conn.conn)
    } else {
        Err(internals)
    }
}

// Drop connections
// NB: This is called with the pool lock held.
fn drop_connections<'a, M>(pool: &Arc<SharedPool<M>>,
                           mut internals: MutexGuard<'a, PoolInternals<M::Connection>>,
                           to_drop: Vec<M::Connection>)
                           -> Box<Future<Item = (), Error = M::Error> + Send>
    where M: ManageConnection
{
    internals.num_conns -= to_drop.len() as u32;
    // We might need to spin up more connections to maintain the idle limit, e.g.
    // if we hit connection lifetime limits
    let f = if internals.num_conns + internals.pending_conns < pool.statics.max_size {
        Pool::replenish_idle_connections_locked(pool, &mut *internals)
    } else {
        Box::new(ok(()))
    };

    // Unlock
    mem::drop(internals);
    // And drop the connections
    // TODO: connection_customizer::on_release!
    f
}

fn drop_idle_connections<'a, M>(pool: &Arc<SharedPool<M>>,
                                internals: MutexGuard<'a, PoolInternals<M::Connection>>,
                                to_drop: Vec<IdleConn<M::Connection>>)
                                -> Box<Future<Item = (), Error = M::Error> + Send>
    where M: ManageConnection
{
    let to_drop = to_drop.into_iter()
        .map(|c| c.conn.conn)
        .collect();
    drop_connections(pool, internals, to_drop)
}

// Reap connections if necessary.
// NB: This is called with the pool lock held.
fn reap_connections<'a, M>(pool: &Arc<SharedPool<M>>,
                           mut internals: MutexGuard<'a, PoolInternals<M::Connection>>)
                           -> Box<Future<Item = (), Error = M::Error> + Send>
    where M: ManageConnection
{
    println!("reaping");
    let now = Instant::now();
    let (to_drop, preserve) = internals.conns
        .drain(..)
        .partition2(|conn| {
            let mut reap = false;
            if let Some(timeout) = pool.statics.idle_timeout {
                println!("idle for {:?}, timeout {:?}",
                         now - conn.idle_start,
                         timeout);
                reap |= now - conn.idle_start >= timeout;
            }
            if let Some(lifetime) = pool.statics.max_lifetime {
                println!("alive for {:?}, max {:?}", now - conn.conn.birth, lifetime);
                reap |= now - conn.conn.birth >= lifetime;
            }
            println!("reaping {}", reap);
            reap
        });
    internals.conns = preserve;
    drop_idle_connections(pool, internals, to_drop)
}

fn schedule_one_reaping<M>(handle: Handle, interval: Interval, weak_shared: Weak<SharedPool<M>>)
    where M: ManageConnection
{
    println!("schedule_one_reaping");
    handle.clone()
        .spawn(interval.into_future()
            .map_err(|_| ())
            .and_then(move |(_, interval)| {
                let f: Box<Future<Item = (), Error = ()>> = match weak_shared.upgrade() {
                    None => Box::new(ok(())),
                    Some(shared) => {
                        let locked = shared.internals.lock().unwrap();
                        Box::new(reap_connections(&shared, locked)
                            .map_err(|_| ())
                            .then(|r| {
                                schedule_one_reaping(handle, interval, weak_shared);
                                r
                            }))
                    }
                };
                f
            }))
}

impl<M: ManageConnection> Pool<M> {
    fn new_inner(builder: Builder<M>, manager: M, event_loop: Remote) -> Pool<M> {
        let internals = PoolInternals {
            waiters: VecDeque::new(),
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
        };

        let shared = Arc::new(SharedPool {
            statics: builder,
            manager: manager,
            event_loop: event_loop,
            internals: Mutex::new(internals),
        });

        if shared.statics.max_lifetime.is_some() || shared.statics.idle_timeout.is_some() {
            let s = Arc::downgrade(&shared);
            shared.event_loop.spawn(move |handle| {
                s.upgrade().ok_or(()).map(|shared| {
                    let interval = Interval::new(shared.statics.reaper_rate, handle).unwrap();
                    println!("Scheduling reapings at {:?}", shared.statics.reaper_rate);
                    schedule_one_reaping(handle.clone(), interval, s);
                })
            })
        }

        Pool { inner: shared }
    }

    fn proxy_or_dispatch<R>(&self, runnable: R)
        where R: IntoFuture<Item = (), Error = ()>,
              R::Future: Send + 'static
    {
        self.inner.proxy_or_dispatch(runnable);
    }

    fn replenish_idle_connections_locked(pool: &Arc<SharedPool<M>>,
                                         internals: &mut PoolInternals<M::Connection>)
                                         -> Box<Future<Item = (), Error = M::Error> + Send> {
        let slots_available = pool.statics.max_size - internals.num_conns - internals.pending_conns;
        let idle = internals.conns.len() as u32;
        let desired = pool.statics.min_idle.unwrap_or(0);
        let f = FuturesUnordered::from_iter((idle..
                                             max(idle, min(desired, idle + slots_available)))
            .map(|_| add_connection(pool, internals)));
        Box::new(f.fold((), |_, _| Ok(())))
    }

    fn replenish_idle_connections(&self) -> Box<Future<Item = (), Error = M::Error> + Send> {
        let mut locked = self.inner.internals.lock().unwrap();
        Pool::replenish_idle_connections_locked(&self.inner, &mut locked)
    }

    /// Returns a `Builder` instance to configure a new pool.
    pub fn builder() -> Builder<M> {
        Builder::new()
    }

    /// Returns information about the current state of the pool.
    pub fn state(&self) -> State {
        let locked = self.inner.internals.lock().unwrap();
        State {
            connections: locked.num_conns,
            idle_connections: locked.conns.len() as u32,
            _p: (),
        }
    }

    /// Run a closure with a `Connection`. Returns a future that must be polled
    /// to drive the process.
    pub fn run<'a, T, E, U, F>(&self, f: F) -> Box<Future<Item = T, Error = E> + 'a>
        where F: FnOnce(M::Connection) -> U + Send + 'a,
              U: IntoFuture<Item = (T, M::Connection), Error = (E, M::Connection)> + 'a,
              E: From<M::Error> + 'a,
              T: 'a
    {
        let inner = self.inner.clone();
        let inner2 = inner.clone();
        Box::new(lazy(move || {
                let f: Box<Future<Item = Conn<M::Connection>, Error = M::Error>> =
                    match get_idle_connection(&inner, inner.internals.lock().unwrap()) {
                        Ok(conn) => Box::new(ok(conn)),
                        Err(mut locked) => {
                            let (tx, rx) = oneshot::channel();
                            locked.waiters.push_back(tx);
                            if locked.num_conns + locked.pending_conns < inner.statics.max_size {
                                let f = add_connection(&inner, &mut locked);
                                inner.proxy_or_dispatch(f.map_err(|_| ()));
                            }

                            Box::new(inner.or_timeout(rx)
                                .then(|r| match r {
                                    Ok(Some(conn)) => Ok(conn),
                                    _ => Err(M::Error::timed_out()),
                                }))
                        }
                    };
                f
            })
            .map_err(|e| e.into())
            .and_then(|conn| {
                let inner = inner2;
                let birth = conn.birth;
                f(conn.conn)
                    .into_future()
                    .then(move |r| {
                        println!("done");
                        let (r, mut conn): (Result<_, E>, _) = match r {
                            Ok((t, conn)) => (Ok(t), conn),
                            Err((e, conn)) => (Err(e.into()), conn),
                        };
                        // Supposed to be fast, but do it before locking anyways.
                        let broken = inner.manager.has_broken(&mut conn);

                        let mut locked = inner.internals.lock().unwrap();
                        if broken {
                            drop_connections(&inner, locked, vec![conn]);
                        } else {
                            let conn = IdleConn::make_idle(Conn {
                                conn: conn,
                                birth: birth,
                            });
                            locked.put_idle_conn(conn);
                        }
                        r
                    })
            }))
    }
}
