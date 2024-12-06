use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::str::FromStr;

use bb8::{CustomizeConnection, Pool};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::config::Config;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Client, Error, Socket, Statement};

// Select some static data from a Postgres DB
//
// The simplest way to start the db is using Docker:
// docker run --name gotham-middleware-postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
#[tokio::main]
async fn main() {
    let config = Config::from_str("postgresql://postgres:docker@localhost:5432").unwrap();
    let pg_mgr = CustomPostgresConnectionManager::new(config, tokio_postgres::NoTls);

    let pool = Pool::builder()
        .connection_customizer(Box::new(Customizer))
        .build(pg_mgr)
        .await
        .expect("build error");

    let connection = pool.get().await.expect("pool error");

    let row = connection
        .query_one(
            connection
                .custom_state
                .get(&QueryName::Addition)
                .expect("statement not predefined"),
            &[],
        )
        .await
        .expect("query failed");

    println!("result: {}", row.get::<usize, i32>(0));
}

#[derive(Debug)]
struct Customizer;

impl CustomizeConnection<CustomPostgresConnection, Error> for Customizer {
    fn on_acquire<'a>(
        &'a self,
        conn: &'a mut CustomPostgresConnection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async {
            conn.custom_state
                .insert(QueryName::BasicSelect, conn.prepare("SELECT 1").await?);

            conn.custom_state
                .insert(QueryName::Addition, conn.prepare("SELECT 1 + 1 + 1").await?);

            Ok(())
        })
    }
}

struct CustomPostgresConnection {
    inner: Client,
    custom_state: BTreeMap<QueryName, Statement>,
}

impl CustomPostgresConnection {
    fn new(inner: Client) -> Self {
        Self {
            inner,
            custom_state: Default::default(),
        }
    }
}

impl Deref for CustomPostgresConnection {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

struct CustomPostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    inner: PostgresConnectionManager<Tls>,
}

impl<Tls> CustomPostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket>,
{
    pub fn new(config: Config, tls: Tls) -> Self {
        Self {
            inner: PostgresConnectionManager::new(config, tls),
        }
    }
}

impl<Tls> bb8::ManageConnection for CustomPostgresConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = CustomPostgresConnection;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = self.inner.connect().await?;
        Ok(CustomPostgresConnection::new(conn))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("").await.map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.inner.has_broken(&mut conn.inner)
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
enum QueryName {
    BasicSelect,
    Addition,
}
