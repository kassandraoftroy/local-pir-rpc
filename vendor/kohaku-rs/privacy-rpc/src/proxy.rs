//! Localhost HTTP proxies in front of the public RPC rings.
//!
//! Helios dials these directly. The proxy is what egresses, over Tor or in
//! clearnet, and retries a different provider when a call fails.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use http_body_util::BodyExt;
use kohaku_tor_rpc::TorRpc;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::warn;
use url::Url;

use crate::PrivacyError;
use crate::isolation::{CircuitChoice, DefaultIsolationPolicy, IsolationPolicy};
use crate::ring::RpcRing;

const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(45);

/// Where a proxy sends the bytes it receives from Helios.
#[derive(Clone)]
pub enum RpcOutbound {
    /// Dial the public URL directly.
    Clearnet,
    /// Dial the public URL through Arti.
    Tor(Box<TorRpc>),
}

/// A bound localhost forwarder. Dropping the handle aborts the accept loop.
pub struct LocalProxy {
    /// Address Helios should use.
    pub addr: std::net::SocketAddr,
    task: JoinHandle<()>,
}

impl LocalProxy {
    /// `http://127.0.0.1:<port>` with no path.
    #[must_use]
    pub fn url(&self) -> Url {
        Url::parse(&format!("http://{}", self.addr)).expect("localhost proxy URL")
    }
}

impl Drop for LocalProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct ProxyState {
    urls: Arc<Vec<Url>>,
    outbound: RpcOutbound,
    /// Execution JSON-RPC posts go to the provider URL as-is. Beacon requests
    /// append the incoming path.
    execution: bool,
    http: reqwest::Client,
}

/// Bind a rotating proxy for `urls`.
///
/// # Errors
///
/// Returns when the loopback listener cannot bind.
pub async fn spawn_proxy(
    urls: Vec<Url>,
    outbound: RpcOutbound,
    execution: bool,
) -> Result<LocalProxy, PrivacyError> {
    let http = reqwest::Client::builder()
        .timeout(ATTEMPT_TIMEOUT)
        .build()
        .map_err(|e| PrivacyError::Transport(e.to_string()))?;
    let state = ProxyState {
        urls: Arc::new(urls),
        outbound,
        execution,
        http,
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| PrivacyError::Transport(format!("bind proxy: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| PrivacyError::Transport(e.to_string()))?;
    let app = axum::Router::new().fallback(forward).with_state(state);
    let task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            warn!(error = %e, "rpc proxy stopped");
        }
    });
    Ok(LocalProxy { addr, task })
}

/// Execution and consensus proxies for `ring`.
///
/// # Errors
///
/// Returns when either listener fails to bind.
pub async fn spawn_ring_proxies(
    ring: &RpcRing,
    outbound: RpcOutbound,
) -> Result<(LocalProxy, LocalProxy), PrivacyError> {
    let execution = spawn_proxy(ring.execution.clone(), outbound.clone(), true).await?;
    let consensus = spawn_proxy(ring.consensus.clone(), outbound, false).await?;
    Ok((execution, consensus))
}

async fn forward(State(state): State<ProxyState>, req: axum::extract::Request) -> Response<Body> {
    let path = req
        .uri()
        .path_and_query()
        .map_or_else(|| "/".to_string(), |p| p.as_str().to_string());
    let method = req.method().clone();
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            if name.eq_ignore_ascii_case("host") || name.eq_ignore_ascii_case("content-length") {
                return None;
            }
            Some((name.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    let body = req
        .into_body()
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
        .unwrap_or_default();
    let isolated = state.execution && wants_isolated(&body);
    let attempts = crate::ring::distinct_attempts(state.urls.as_slice(), crate::ring::MAX_ATTEMPTS);
    let mut last = "no providers configured".to_string();
    for url in attempts {
        let target = target_url(&url, &path, state.execution);
        match tokio::time::timeout(
            ATTEMPT_TIMEOUT,
            state.outbound.exchange(
                &state.http,
                &target,
                method.as_str(),
                &headers,
                &body,
                isolated,
            ),
        )
        .await
        {
            Ok(Ok((status, _bytes))) if should_retry(status) => {
                last = format!("{target} HTTP {status}");
                warn!(%target, %status, "public rpc failed; trying another provider");
            }
            Ok(Ok((status, bytes))) => {
                return Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from(bytes))
                    .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response());
            }
            Ok(Err(err)) => {
                last = format!("{target} {err}");
                warn!(%target, error = %err, "public rpc failed; trying another provider");
            }
            Err(_) => {
                last = format!("{target} timed out");
                warn!(%target, "public rpc timed out; trying another provider");
            }
        }
    }
    (
        StatusCode::BAD_GATEWAY,
        format!("public rpc ring exhausted: {last}"),
    )
        .into_response()
}

fn wants_isolated(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return true;
    };
    let items = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    let policy = DefaultIsolationPolicy;
    items.iter().any(|item| {
        let method = item.get("method").and_then(Value::as_str).unwrap_or("");
        let params = item.get("params").cloned().unwrap_or(Value::Null);
        !matches!(policy.circuit_for(method, &params), CircuitChoice::Shared)
    })
}

fn should_retry(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// Execution posts use `base` unchanged. Beacon calls append `incoming`.
#[must_use]
pub fn target_url(base: &Url, incoming: &str, execution: bool) -> Url {
    if execution {
        return base.clone();
    }
    let mut url = base.clone();
    let (path, query) = incoming
        .split_once('?')
        .map_or((incoming, None), |(path, query)| (path, Some(query)));
    let base_path = url.path().trim_end_matches('/');
    let path = path.trim_start_matches('/');
    url.set_path(&format!("{base_path}/{path}"));
    url.set_query(query);
    url
}

impl RpcOutbound {
    /// Tor egress. The client is boxed because an Arti handle is large.
    #[must_use]
    pub fn tor(tor: TorRpc) -> Self {
        Self::Tor(Box::new(tor))
    }

    /// `GET url` and parse a JSON body. Used for beacon checkpoint agreement,
    /// which must name the endpoint instead of going through the rotating proxy.
    pub(crate) async fn get_json(&self, url: &Url) -> Result<serde_json::Value, String> {
        let http = reqwest::Client::builder()
            .timeout(ATTEMPT_TIMEOUT)
            .build()
            .map_err(|e| e.to_string())?;
        let (status, bytes) = self.exchange(&http, url, "GET", &[], &[], false).await?;
        if !status.is_success() {
            return Err(format!("{url} HTTP {status}"));
        }
        serde_json::from_slice(&bytes).map_err(|e| e.to_string())
    }

    async fn exchange(
        &self,
        http: &reqwest::Client,
        url: &Url,
        method: &str,
        headers: &[(String, String)],
        body: &[u8],
        isolated: bool,
    ) -> Result<(StatusCode, Vec<u8>), String> {
        match self {
            Self::Clearnet => {
                let method =
                    reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
                let mut req = http.request(method, url.clone());
                for (name, value) in headers {
                    req = req.header(name, value);
                }
                if !body.is_empty() {
                    req = req.body(body.to_vec());
                }
                let resp = req.send().await.map_err(|e| e.to_string())?;
                let status = resp.status();
                let bytes = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
                Ok((status, bytes))
            }
            Self::Tor(tor) => {
                let tor = if isolated {
                    tor.isolated()
                } else {
                    tor.as_ref().clone()
                };
                let resp = tor
                    .http(url, method, headers, body)
                    .await
                    .map_err(|e| e.to_string())?;
                let status = resp.status();
                let bytes = resp.into_body().to_vec();
                Ok((status, bytes))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_target_ignores_the_incoming_path() {
        let base = Url::parse("https://ethereum.blockpi.network/v1/rpc/public").unwrap();
        let got = target_url(&base, "/eth/v1/beacon/headers/head", true);
        assert_eq!(got.as_str(), base.as_str());
    }

    #[test]
    fn consensus_target_appends_the_beacon_path() {
        let base = Url::parse("https://lodestar-mainnet.chainsafe.io").unwrap();
        let got = target_url(&base, "/eth/v1/beacon/blocks/1/root", false);
        assert_eq!(
            got.as_str(),
            "https://lodestar-mainnet.chainsafe.io/eth/v1/beacon/blocks/1/root"
        );
    }
}
