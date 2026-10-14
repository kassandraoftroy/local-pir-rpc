//! In-process Helios. Fallback JSON-RPC is served on localhost; Helios fetches
//! proofs through the rotating proxies.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::B256;
use helios_ethereum::config::networks::Network;
use helios_ethereum::{EthereumClient, EthereumClientBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{info, warn};
use url::Url;

use crate::PrivacyError;
use crate::proxy::{LocalProxy, RpcOutbound, spawn_ring_proxies};
use crate::ring::RpcRing;

/// Mainnet genesis unix time. Slot = (now - genesis) / 12.
const GENESIS_TIME: u64 = 1_606_824_023;
const SLOT_SECONDS: u64 = 12;
const SLOTS_PER_EPOCH: u64 = 32;
/// About twelve days, inside Helios's fourteen-day strict checkpoint limit.
const HISTORY_SLOTS: u64 = 12 * 24 * 60 * 60 / SLOT_SECONDS;
/// Roll the pin before it crosses the strict limit.
const ROLL_AFTER_SECS: u64 = 13 * 24 * 60 * 60;
const SYNC_TIMEOUT: Duration = Duration::from_mins(15);

/// Helios plus the proxies that feed it. Keep this value alive for as long as
/// clients use [`rpc_url`](Self::rpc_url).
pub struct VerifiedRpc {
    rpc_url: Url,
    oldest_block: u64,
    head_block: Option<u64>,
    _client: EthereumClient,
    _execution: LocalProxy,
    _consensus: LocalProxy,
}

impl VerifiedRpc {
    /// Local Helios JSON-RPC URL. Fallback traffic posts here, not through Tor.
    #[must_use]
    pub fn rpc_url(&self) -> &Url {
        &self.rpc_url
    }

    /// Oldest execution block this process will serve.
    #[must_use]
    pub const fn oldest_block(&self) -> u64 {
        self.oldest_block
    }

    /// Helios head after sync, when the localhost RPC answered `eth_blockNumber`.
    #[must_use]
    pub const fn head_block(&self) -> Option<u64> {
        self.head_block
    }

    /// Start proxies, pin a checkpoint about twelve days back, and wait until
    /// Helios reports synced.
    ///
    /// # Errors
    ///
    /// Returns when the proxies fail to bind, two beacon endpoints disagree on
    /// the checkpoint, or Helios does not sync in time.
    pub async fn start(
        outbound: RpcOutbound,
        extra_execution: &[String],
        data_dir: PathBuf,
    ) -> Result<Self, PrivacyError> {
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| PrivacyError::Transport(format!("helios data dir: {e}")))?;
        let ring = RpcRing::mainnet(extra_execution)?;
        let (execution, consensus) = spawn_ring_proxies(&ring, outbound.clone()).await?;
        let pinned = load_or_fetch_window(&outbound, &ring, &data_dir).await?;
        info!(
            checkpoint = %pinned.checkpoint,
            slot = pinned.slot,
            oldest_block = pinned.execution_block,
            "pinned light-client window"
        );
        let checkpoint: B256 = pinned
            .checkpoint
            .parse()
            .map_err(|e| PrivacyError::Transport(format!("checkpoint: {e}")))?;
        let rpc_addr = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = tokio::net::TcpListener::bind(rpc_addr)
            .await
            .map_err(|e| PrivacyError::Transport(format!("bind helios: {e}")))?;
        let rpc_addr = listener
            .local_addr()
            .map_err(|e| PrivacyError::Transport(e.to_string()))?;
        drop(listener);

        let client = EthereumClientBuilder::new()
            .network(Network::Mainnet)
            .consensus_rpc(consensus.url())
            .map_err(|e| PrivacyError::Transport(e.to_string()))?
            .execution_rpc(execution.url())
            .map_err(|e| PrivacyError::Transport(e.to_string()))?
            .checkpoint(checkpoint)
            .strict_checkpoint_age()
            .data_dir(data_dir)
            .rpc_address(rpc_addr)
            .with_file_db()
            .build()
            .map_err(|e| PrivacyError::Transport(format!("helios: {e}")))?;

        let rpc_url = Url::parse(&format!("http://{rpc_addr}"))
            .map_err(|e| PrivacyError::InvalidUrl(e.to_string()))?;
        wait_until_synced(&rpc_url).await?;
        let head_block = fetch_head(&rpc_url).await;
        if let Some(head) = head_block {
            info!(head, oldest = pinned.execution_block, "helios synced");
        }
        Ok(Self {
            rpc_url,
            oldest_block: pinned.execution_block,
            head_block,
            _client: client,
            _execution: execution,
            _consensus: consensus,
        })
    }
}

/// `~/.local-pir-rpc` , or `$LOCAL_PIR_RPC_DIR` when that is set.
#[must_use]
pub fn default_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LOCAL_PIR_RPC_DIR") {
        let dir = dir.trim();
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".local-pir-rpc");
    }
    std::env::temp_dir().join("local-pir-rpc")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PinnedWindow {
    checkpoint: String,
    execution_block: u64,
    slot: u64,
}

async fn load_or_fetch_window(
    outbound: &RpcOutbound,
    ring: &RpcRing,
    data_dir: &std::path::Path,
) -> Result<PinnedWindow, PrivacyError> {
    let path = data_dir.join("light-window.json");
    if let Ok(text) = std::fs::read_to_string(&path)
        && let Ok(pinned) = serde_json::from_str::<PinnedWindow>(&text)
        && slot_age_secs(pinned.slot) < ROLL_AFTER_SECS
    {
        info!(slot = pinned.slot, "reusing pinned light-client checkpoint");
        return Ok(pinned);
    }
    let pinned = fetch_window(outbound, ring).await?;
    let text = serde_json::to_string_pretty(&pinned)
        .map_err(|e| PrivacyError::Transport(e.to_string()))?;
    std::fs::write(&path, text)
        .map_err(|e| PrivacyError::Transport(format!("save window: {e}")))?;
    Ok(pinned)
}

async fn fetch_window(
    outbound: &RpcOutbound,
    ring: &RpcRing,
) -> Result<PinnedWindow, PrivacyError> {
    let endpoints = ring.consensus_attempts(3);
    if endpoints.len() < 2 {
        return Err(PrivacyError::Transport(
            "need two beacon endpoints to agree on a checkpoint".into(),
        ));
    }
    let head = head_slot(outbound, &endpoints).await?;
    let slot = epoch_aligned(head.saturating_sub(HISTORY_SLOTS));
    info!(head, slot, "fetching light-client checkpoint");

    let mut roots: Vec<(Url, String)> = Vec::new();
    for url in &endpoints {
        let root_url = join_beacon(url, &format!("/eth/v1/beacon/blocks/{slot}/root"));
        match outbound.get_json(&root_url).await {
            Ok(body) => {
                if let Some(root) = body
                    .get("data")
                    .and_then(|d| d.get("root"))
                    .and_then(Value::as_str)
                {
                    roots.push((url.clone(), root.to_string()));
                }
            }
            Err(err) => warn!(%url, error = %err, "beacon root fetch failed"),
        }
    }
    let checkpoint = agreed_root(&roots).ok_or_else(|| {
        PrivacyError::Transport("beacon endpoints did not agree on a checkpoint".into())
    })?;
    let source = roots
        .iter()
        .find(|(_, root)| root == &checkpoint)
        .map(|(url, _)| url.clone())
        .ok_or_else(|| PrivacyError::Transport("checkpoint source missing".into()))?;
    let block_url = join_beacon(&source, &format!("/eth/v2/beacon/blocks/{slot}"));
    let body = outbound
        .get_json(&block_url)
        .await
        .map_err(PrivacyError::Transport)?;
    let execution_block = execution_block_number(&body).ok_or_else(|| {
        PrivacyError::Transport("beacon block has no execution payload block number".into())
    })?;
    Ok(PinnedWindow {
        checkpoint,
        execution_block,
        slot,
    })
}

async fn head_slot(outbound: &RpcOutbound, endpoints: &[Url]) -> Result<u64, PrivacyError> {
    let mut last = "no beacon head".to_string();
    for url in endpoints {
        let head_url = join_beacon(url, "/eth/v1/beacon/headers/head");
        match outbound.get_json(&head_url).await {
            Ok(body) => {
                if let Some(slot) = body
                    .pointer("/data/header/message/slot")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                {
                    return Ok(slot);
                }
                last = format!("{url} head had no slot");
            }
            Err(err) => last = format!("{url} {err}"),
        }
    }
    Err(PrivacyError::Transport(last))
}

fn agreed_root(roots: &[(Url, String)]) -> Option<String> {
    for (i, (_, a)) in roots.iter().enumerate() {
        if roots.iter().skip(i + 1).any(|(_, b)| a == b) {
            return Some(a.clone());
        }
    }
    None
}

fn execution_block_number(body: &Value) -> Option<u64> {
    let payload = body.pointer("/data/message/body/execution_payload")?;
    match payload.get("block_number")? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn join_beacon(base: &Url, path: &str) -> Url {
    crate::proxy::target_url(base, path, false)
}

fn epoch_aligned(slot: u64) -> u64 {
    slot - (slot % SLOTS_PER_EPOCH)
}

fn slot_age_secs(slot: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let slot_time = GENESIS_TIME + slot.saturating_mul(SLOT_SECONDS);
    now.saturating_sub(slot_time)
}

async fn wait_until_synced(url: &Url) -> Result<(), PrivacyError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| PrivacyError::Transport(e.to_string()))?;
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_syncing",
        "params": []
    });
    let deadline = tokio::time::Instant::now() + SYNC_TIMEOUT;
    let mut logged = false;
    loop {
        if tokio::time::Instant::now() > deadline {
            return Err(PrivacyError::Transport(
                "helios did not sync within 15 minutes".into(),
            ));
        }
        match client.post(url.clone()).json(&body).send().await {
            Ok(resp) => {
                let value: Value = resp.json().await.unwrap_or(Value::Null);
                if value.get("result").is_some_and(|r| r == false) {
                    return Ok(());
                }
                if !logged {
                    info!("waiting for helios to sync");
                    logged = true;
                }
            }
            Err(err) => {
                if !logged {
                    info!(error = %err, "waiting for helios rpc");
                    logged = true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn fetch_head(url: &Url) -> Option<u64> {
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": []
    });
    let resp = client.post(url.clone()).json(&body).send().await.ok()?;
    let value: Value = resp.json().await.ok()?;
    let hex = value.get("result")?.as_str()?;
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    u64::from_str_radix(hex, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_slot_is_epoch_aligned_about_twelve_days_back() {
        let head = 15_428_561_u64;
        let slot = epoch_aligned(head.saturating_sub(HISTORY_SLOTS));
        assert_eq!(slot % SLOTS_PER_EPOCH, 0);
        assert!(head - slot >= HISTORY_SLOTS);
        assert!(head - slot < HISTORY_SLOTS + SLOTS_PER_EPOCH);
    }

    #[test]
    fn two_matching_roots_win() {
        let a = Url::parse("https://a.example").unwrap();
        let b = Url::parse("https://b.example").unwrap();
        let c = Url::parse("https://c.example").unwrap();
        let roots = vec![
            (a, "0x1".into()),
            (b, "0x2".into()),
            (c.clone(), "0x2".into()),
        ];
        assert_eq!(agreed_root(&roots).as_deref(), Some("0x2"));
        let _ = c;
    }
}
