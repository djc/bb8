//! Faktory support for the `bb8` connection pool.
#![deny(missing_docs, missing_debug_implementations)]

pub use bb8;
pub use faktory;

use faktory::{Client, Error};

#[cfg(any(feature = "native-tls", feature = "rustls"))]
use url::Url;

#[cfg(feature = "rustls")]
use std::sync::Arc;

/// TLS connector configuration.
#[derive(Clone, Default)]
pub enum TlsConnector {
    /// Plain TCP connection.
    #[default]
    NoTls,
    /// TLS using native-tls.
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls::TlsConnector),
    /// TLS using rustls.
    #[cfg(feature = "rustls")]
    Rustls(tokio_rustls::TlsConnector),
}

impl std::fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoTls => f.debug_tuple("NoTls").finish(),
            #[cfg(feature = "native-tls")]
            Self::NativeTls(connector) => f.debug_tuple("NativeTls").field(connector).finish(),
            #[cfg(feature = "rustls")]
            Self::Rustls(_) => f.debug_tuple("Rustls").field(&"..").finish(),
        }
    }
}

impl TlsConnector {
    /// Create a native-tls connector with default settings.
    #[cfg(feature = "native-tls")]
    pub fn native_tls_default() -> Result<Self, native_tls::Error> {
        let connector = native_tls::TlsConnector::builder().build()?;
        Ok(Self::NativeTls(connector))
    }

    /// Create a rustls connector with default settings (empty root store).
    ///
    /// Note: This uses an empty certificate store. For production use,
    /// consider using `rustls_default_with_native_certs()` or providing
    /// your own `ClientConfig`.
    #[cfg(feature = "rustls")]
    pub fn rustls_default() -> Self {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        Self::Rustls(tokio_rustls::TlsConnector::from(Arc::new(config)))
    }

    /// Create a rustls connector from a custom ClientConfig.
    #[cfg(feature = "rustls")]
    pub fn rustls_with_config(config: rustls::ClientConfig) -> Self {
        Self::Rustls(tokio_rustls::TlsConnector::from(Arc::new(config)))
    }
}

/// A `bb8::ManageConnection` for `faktory::Client`.
#[derive(Debug, Clone)]
pub struct FaktoryConnectionManager {
    url: String,
    tls: TlsConnector,
}

impl FaktoryConnectionManager {
    /// Create a new `FaktoryConnectionManager` with the specified URL.
    ///
    /// The URL should be in the format: `protocol://[:password@]hostname[:port]`
    ///
    /// # Examples
    ///
    /// ```
    /// use bb8_faktory::FaktoryConnectionManager;
    ///
    /// let manager = FaktoryConnectionManager::new("tcp://localhost:7419");
    /// ```
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            tls: TlsConnector::default(),
        }
    }

    /// Create a new `FaktoryConnectionManager` using environment variables.
    ///
    /// This reads `FAKTORY_PROVIDER` to get the env var name (defaults to `FAKTORY_URL`),
    /// then reads that env var for the URL (defaults to `tcp://localhost:7419`).
    ///
    /// If a TLS feature is enabled, it will automatically use that TLS implementation
    /// with default settings.
    ///
    /// # Example
    ///
    /// ```
    /// use bb8_faktory::FaktoryConnectionManager;
    ///
    /// let manager = FaktoryConnectionManager::from_env();
    /// ```
    pub fn from_env() -> Self {
        let url = std::env::var("FAKTORY_PROVIDER")
            .ok()
            .and_then(|provider| std::env::var(provider).ok())
            .or_else(|| std::env::var("FAKTORY_URL").ok())
            .unwrap_or_else(|| "tcp://localhost:7419".to_string());

        #[cfg(feature = "rustls")]
        let tls = TlsConnector::rustls_default();

        #[cfg(all(feature = "native-tls", not(feature = "rustls")))]
        let tls = TlsConnector::native_tls_default().expect("Failed to build native-tls connector");

        #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
        let tls = TlsConnector::NoTls;

        Self { url, tls }
    }

    /// Set the TLS connector for the connection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use bb8_faktory::{FaktoryConnectionManager, TlsConnector};
    ///
    /// let manager = FaktoryConnectionManager::new("tcp+tls://localhost:7419")
    ///     .with_tls(TlsConnector::native_tls_default().unwrap());
    /// ```
    pub fn with_tls(mut self, tls: TlsConnector) -> Self {
        self.tls = tls;
        self
    }
}

impl bb8::ManageConnection for FaktoryConnectionManager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match &self.tls {
            TlsConnector::NoTls => Client::connect_to(&self.url).await,
            #[cfg(feature = "native-tls")]
            TlsConnector::NativeTls(connector) => {
                let password = Url::parse(&self.url)
                    .ok()
                    .and_then(|url| url.password().map(|p| p.to_string()));
                let stream = faktory::native_tls::TlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str()),
                )
                .await?;
                let buffered = tokio::io::BufStream::new(stream);
                Client::connect_with(buffered, password).await
            }
            #[cfg(feature = "rustls")]
            TlsConnector::Rustls(connector) => {
                let password = Url::parse(&self.url)
                    .ok()
                    .and_then(|url| url.password().map(|p| p.to_string()));
                let stream = faktory::rustls::TlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str()),
                )
                .await?;
                let buffered = tokio::io::BufStream::new(stream);
                Client::connect_with(buffered, password).await
            }
        }
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.current_info().await.map(|_| ())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
