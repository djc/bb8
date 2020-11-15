# bb8

[![Documentation](https://docs.rs/bb8/badge.svg)](https://docs.rs/bb8/)
[![Crates.io](https://img.shields.io/crates/v/bb8.svg)](https://crates.io/crates/bb8)
[![Build status](https://github.com/djc/bb8/workflows/CI/badge.svg)](https://github.com/djc/bb8/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/djc/bb8/branch/main/graph/badge.svg)](https://codecov.io/gh/djc/bb8)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE-MIT)

A full-featured connection pool, designed for asynchronous connections (using
tokio). Originally based on [r2d2](https://github.com/sfackler/r2d2).

Opening a new database connection every time one is needed is both inefficient
and can lead to resource exhaustion under high traffic conditions. A connection
pool maintains a set of open connections to a database, handing them out for
repeated use.

bb8 is agnostic to the connection type it is managing. Implementors of the
`ManageConnection` trait provide the database-specific logic to create and
check the health of connections.

A (possibly not exhaustive) list of adapters for different backends:

Backend | Adapter Crate
------- | -------------
[tokio-postgres](https://github.com/sfackler/rust-postgres) | [bb8-postgres](https://crates.io/crates/bb8-postgres) (in-tree)
[redis](https://github.com/mitsuhiko/redis-rs) | [bb8-redis](https://crates.io/crates/bb8-redis) (in-tree)
[rsmq](https://github.com/smrchy/rsmq) | [rsmq_async](https://crates.io/crates/rsmq_async)
[bolt-client](https://crates.io/crates/bolt-client) | [bb8-bolt](https://crates.io/crates/bb8-bolt)
[diesel](https://crates.io/crates/diesel) | [bb8-diesel](https://crates.io/crates/bb8-diesel)
[tiberius](https://crates.io/crates/tiberius) | [bb8-tiberius](https://crates.io/crates/bb8-tiberius)
[nebula-graph-client](https://crates.io/crates/nebula-graph-client) | [bb8-nebula-graph](https://crates.io/crates/bb8-nebula-graph)
[memcache-async](https://github.com/vavrusa/memcache-async) | [bb8-memcached](https://crates.io/crates/bb8-memcached)
[lapin](https://crates.io/crates/lapin) | [bb8-lapin](https://crates.io/crates/bb8-lapin)

## Example

Using an imaginary "foodb" database.

```rust
fn main() {
    let manager = bb8_foodb::FooConnectionManager::new("localhost:1234");
    let pool = bb8::Pool::builder()
        .max_size(15)
        .build(manager)
        .unwrap();

    for _ in 0..20 {
        let pool = pool.clone();
        tokio::spawn(move || {
            let conn = pool.get().await.unwrap();
            // use the connection
            // it will be returned to the pool when it falls out of scope.
        })
    }
}
```

## License

Licensed under the MIT license ([LICENSE](LICENSE)).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be licensed as above, without any
additional terms or conditions.
