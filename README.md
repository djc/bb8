# bb8

A full-featured connection pool, designed for asynchronous connections (using
tokio). Originally based on [r2d2](https://github.com/sfackler/r2d2).

[Documentation](https://docs.rs/bb8)

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
[tokio-postgres](https://github.com/sfackler/rust-postgres) | [bb8-postgres](https://crates.io/crates/bb8-postgres)
[redis](https://github.com/mitsuhiko/redis-rs) | [bb8-redis](https://crates.io/crates/bb8-redis)

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
