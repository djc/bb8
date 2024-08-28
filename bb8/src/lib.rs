//! A full-featured connection pool, designed for asynchronous connections
//! (using tokio). Originally based on [r2d2](https://github.com/sfackler/r2d2).
//!
//! Opening a new database connection every time one is needed is both
//! inefficient and can lead to resource exhaustion under high traffic
//! conditions. A connection pool maintains a set of open connections to a
//! database, handing them out for repeated use.
//!
//! bb8 is agnostic to the connection type it is managing. Implementors of the
//! `ManageConnection` trait provide the database-specific logic to create and
//! check the health of connections.
//!
//! # Example
//!
//! Using an imaginary "foodb" database.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() {
//!     let manager = bb8_foodb::FooConnectionManager::new("localhost:1234");
//!     let pool = bb8::Pool::builder().build(manager).await.unwrap();
//!
//!     for _ in 0..20 {
//!         let pool = pool.clone();
//!         tokio::spawn(async move {
//!             let conn = pool.get().await.unwrap();
//!             // use the connection
//!             // it will be returned to the pool when it falls out of scope.
//!         });
//!     }
//! }
//! ```
#![allow(clippy::needless_doctest_main)]
#![deny(missing_docs, missing_debug_implementations)]

mod api;
pub use api::{
    AddError, Builder, CustomizeConnection, ErrorSink, ManageConnection, NopErrorSink, Pool,
    PooledConnection, QueueStrategy, RunError, State, Statistics,
};

mod inner;
mod internals;
mod lock {
    #[cfg(feature = "parking_lot")]
    use parking_lot::Mutex as MutexImpl;
    #[cfg(feature = "parking_lot")]
    use parking_lot::MutexGuard;

    #[cfg(not(feature = "parking_lot"))]
    use std::sync::Mutex as MutexImpl;
    #[cfg(not(feature = "parking_lot"))]
    use std::sync::MutexGuard;

    pub(crate) struct Mutex<T>(MutexImpl<T>);

    impl<T> Mutex<T> {
        pub(crate) fn new(val: T) -> Self {
            Self(MutexImpl::new(val))
        }

        pub(crate) fn lock(&self) -> MutexGuard<'_, T> {
            #[cfg(feature = "parking_lot")]
            {
                self.0.lock()
            }
            #[cfg(not(feature = "parking_lot"))]
            {
                self.0.lock().unwrap()
            }
        }
    }
}
