//! vtgate gRPC connection handling.

use std::time::Duration;

use base64::Engine as _;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::error::VStreamError;
use crate::proto::vtgateservice::vitess_client::VitessClient;

/// Connection settings for a vtgate (or a PlanetScale edge, which speaks the same protocol).
#[derive(Debug, Clone)]
pub struct VitessEndpoint {
    /// `https://aws.connect.psdb.cloud:443` or `http://127.0.0.1:33575`.
    pub uri: String,
    /// Optional HTTP basic credentials, required by PlanetScale.
    pub basic_auth: Option<(String, String)>,
    pub connect_timeout: Duration,
    /// Upper bound for a single gRPC message. Large transactions arrive as one message unless
    /// `transaction_chunk_size` is set, so keep this generous.
    pub max_message_bytes: usize,
}

impl VitessEndpoint {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            basic_auth: None,
            connect_timeout: Duration::from_secs(15),
            max_message_bytes: 256 * 1024 * 1024,
        }
    }

    pub fn with_basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.basic_auth = Some((username.into(), password.into()));
        self
    }

    pub fn is_tls(&self) -> bool {
        self.uri.starts_with("https://")
    }

    pub async fn connect(
        &self,
    ) -> Result<VitessClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>, VStreamError>
    {
        let mut endpoint = Endpoint::from_shared(self.uri.clone())
            .map_err(VStreamError::Transport)?
            .connect_timeout(self.connect_timeout)
            .http2_keep_alive_interval(Duration::from_secs(20))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .tcp_nodelay(true);
        if self.is_tls() {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let channel = endpoint.connect().await?;
        let interceptor = AuthInterceptor::new(self.basic_auth.as_ref());
        let client = VitessClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        Ok(client)
    }
}

/// Adds `authorization: Basic ...` to every request when credentials are configured.
#[derive(Clone)]
pub struct AuthInterceptor {
    header: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl AuthInterceptor {
    fn new(auth: Option<&(String, String)>) -> Self {
        let header = auth.map(|(u, p)| {
            let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
            MetadataValue::try_from(format!("Basic {encoded}")).expect("ascii header")
        });
        Self { header }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(h) = &self.header {
            request.metadata_mut().insert("authorization", h.clone());
        }
        Ok(request)
    }
}

impl std::fmt::Debug for AuthInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthInterceptor")
            .field("configured", &self.header.is_some())
            .finish()
    }
}

pub type Client = VitessClient<tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>>;
