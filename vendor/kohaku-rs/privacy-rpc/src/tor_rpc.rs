use alloy::{
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::TransportError,
};
use async_trait::async_trait;
use kohaku_tor_rpc::TorRpc;
use url::Url;

/// Planner-facing Tor egress (JSON-RPC + generic HTTP).
///
/// Production uses [`LiveTorRpc`]; tests inject recording doubles.
#[async_trait]
pub trait TorRpcBackend: Send + Sync {
    /// JSON-RPC over the shared Tor client.
    async fn send_shared(
        &self,
        url: Url,
        packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError>;

    /// JSON-RPC on a fresh isolated Tor client.
    async fn send_isolated(
        &self,
        url: Url,
        packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError>;

    /// HTTP POST over shared Tor (PIR server traffic).
    async fn http_post(
        &self,
        url: &Url,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<Vec<u8>, String>;
}

/// Tor backend that refuses every call. Clearnet Helios mode installs this so
/// the builder can omit Arti while fallback stays on localhost.
#[derive(Debug)]
pub struct DisabledTor;

#[derive(Debug)]
struct DisabledTorError;

impl std::fmt::Display for DisabledTorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Tor is disabled")
    }
}

impl std::error::Error for DisabledTorError {}

fn tor_disabled() -> TransportError {
    alloy::transports::TransportErrorKind::custom(DisabledTorError).into()
}

#[async_trait]
impl TorRpcBackend for DisabledTor {
    async fn send_shared(
        &self,
        _url: Url,
        _packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError> {
        Err(tor_disabled())
    }

    async fn send_isolated(
        &self,
        _url: Url,
        _packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError> {
        Err(tor_disabled())
    }

    async fn http_post(
        &self,
        _url: &Url,
        _headers: &[(String, String)],
        _body: &[u8],
    ) -> Result<Vec<u8>, String> {
        Err("Tor is disabled".into())
    }
}

/// Thin wrapper around [`TorRpc`].
#[derive(Clone)]
pub struct LiveTorRpc {
    inner: TorRpc,
}

impl LiveTorRpc {
    /// Wrap a bootstrapped client.
    #[must_use]
    pub const fn new(inner: TorRpc) -> Self {
        Self { inner }
    }

    /// Access the underlying client (e.g. for PIR lookup adapters).
    #[must_use]
    pub const fn inner(&self) -> &TorRpc {
        &self.inner
    }
}

#[async_trait]
impl TorRpcBackend for LiveTorRpc {
    async fn send_shared(
        &self,
        url: Url,
        packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError> {
        self.inner.send(url, packet).await
    }

    async fn send_isolated(
        &self,
        url: Url,
        packet: RequestPacket,
    ) -> Result<ResponsePacket, TransportError> {
        self.inner.isolated().send(url, packet).await
    }

    async fn http_post(
        &self,
        url: &Url,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        self.inner
            .post(url, headers, body)
            .await
            .map_err(|e| e.to_string())
    }
}
