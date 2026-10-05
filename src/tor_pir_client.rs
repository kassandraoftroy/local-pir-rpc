//! inspire-gpu-serving PIR client logic over [`TorRpc`] (shared circuits, no isolation).
//!
//! Adapted from `pir-client`; HTTP uses Tor instead of clearnet `ureq`.
//!
//! Concurrent lookups use a pool of clients (see
//! [kassandraoftroy/local-pir-rpc#1](https://github.com/kassandraoftroy/local-pir-rpc/issues/1)).

use std::collections::VecDeque;
use std::sync::Arc;

use kohaku_tor_rpc::TorRpc;
use pir_backend_ffi::{pack_query, ClientQuery, Params};
use pir_keyword::cuckoo::CuckooHash;
use pir_keyword::manifest::{Manifest, SidecarBroadcast};
use pir_keyword::slots::unpack_bytes;
use tokio::sync::{Mutex, Semaphore};
use url::Url;

const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;

/// Default pool size: matches inspire-gpu-serving parallel client capacity used in benches.
pub const DEFAULT_POOL_SIZE: usize = 16;

/// Tor-backed PIR HTTP client (manifest + encrypted `/lookup` POSTs).
#[derive(Clone)]
pub struct TorPirClient {
    tor: TorRpc,
    base: String,
    manifest: Manifest,
    params: Params,
    hasher: CuckooHash,
}

impl TorPirClient {
    /// Fetch `/manifest` over Tor and initialize crypto state.
    pub async fn connect(tor: TorRpc, base: &str) -> Result<Self, String> {
        let base = base.trim_end_matches('/').to_string();
        let manifest_url = Url::parse(&format!("{base}/manifest")).map_err(|e| e.to_string())?;
        let body = tor
            .get(&manifest_url)
            .await
            .map_err(|e| format!("manifest over Tor: {e}"))?;
        if body.len() > MAX_RESPONSE_BYTES {
            return Err("manifest response too large".into());
        }
        let manifest = Manifest::from_json(std::str::from_utf8(&body).map_err(|e| e.to_string())?)?;
        let params = Params::from_manifest(&manifest.pir)?;
        let hasher = manifest.cuckoo.hasher()?;
        Ok(Self {
            tor,
            base,
            manifest,
            params,
            hasher,
        })
    }

    pub async fn lookup_address(&mut self, address: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let cuckoo = &self.manifest.cuckoo;
        let key = cuckoo.key_derivation.key(address, cuckoo.key_size);
        self.lookup(&key).await
    }

    pub async fn lookup(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        match self.lookup_once(key).await? {
            LookupOutcome::Done(v) => Ok(v),
            LookupOutcome::Reconfigured => {
                self.refresh().await?;
                match self.lookup_once(key).await? {
                    LookupOutcome::Done(v) => Ok(v),
                    LookupOutcome::Reconfigured => {
                        Err("server configuration kept changing".into())
                    }
                }
            }
        }
    }

    async fn refresh(&mut self) -> Result<(), String> {
        let fresh = Self::connect(self.tor.clone(), &self.base).await?;
        self.manifest = fresh.manifest;
        self.params = fresh.params;
        self.hasher = fresh.hasher;
        Ok(())
    }

    fn match_cell(&self, bucket: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let ks = self.manifest.cuckoo.key_size;
        let cs = ks + self.manifest.cuckoo.value_size;
        for c in 0..self.manifest.cuckoo.bucket_capacity {
            let cell = &bucket[c * cs..(c + 1) * cs];
            if &cell[..ks] == key {
                return Some(cell[ks..cs].to_vec());
            }
        }
        None
    }

    async fn lookup_once(&mut self, key: &[u8]) -> Result<LookupOutcome, String> {
        let [p0, p1] = self.hasher.positions_2(key);
        let q0 = ClientQuery::build(&self.params, p0 as u64)?;
        let q1 = ClientQuery::build(&self.params, p1 as u64)?;

        let mut body = pack_query(&self.params, &q0.flat)?;
        body.extend_from_slice(&pack_query(&self.params, &q1.flat)?);

        let lookup_url =
            Url::parse(&format!("{}/lookup", self.base)).map_err(|e| e.to_string())?;
        let resp = self
            .tor
            .http_shared(&lookup_url, "POST", &[], &body)
            .await
            .map_err(|e| format!("lookup over Tor: {e}"))?;
        if resp.status != 200 {
            return Err(format!("lookup HTTP {}", resp.status));
        }
        if resp.body.len() > MAX_RESPONSE_BYTES {
            return Err("lookup response too large".into());
        }
        let stamp = resp.header("x-snapshot").unwrap_or("");
        if stamp.split(':').nth(1) != Some(self.manifest.config_fp().as_str()) {
            return Ok(LookupOutcome::Reconfigured);
        }
        let bytes = &resp.body;

        let rb = self.params.response_compressed_bytes();
        if bytes.len() < 4 {
            return Err("lookup response too short".into());
        }
        let json_len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if bytes.len() != 4 + json_len + 2 * rb {
            return Err(format!(
                "lookup response is {} bytes, expected 4 + {json_len} + 2x{rb}",
                bytes.len()
            ));
        }
        let sc = SidecarBroadcast::from_json(
            std::str::from_utf8(&bytes[4..4 + json_len]).map_err(|e| e.to_string())?,
        )?;

        let r0 = &bytes[4 + json_len..4 + json_len + rb];
        let r1 = &bytes[4 + json_len + rb..];

        let cs = self.manifest.cuckoo.key_size + self.manifest.cuckoo.value_size;
        let bucket_bytes = self.manifest.cuckoo.bucket_capacity * cs;
        let b0 = unpack_bytes(&q0.extract_compressed(&self.params, r0)?, bucket_bytes);
        let b1 = unpack_bytes(&q1.extract_compressed(&self.params, r1)?, bucket_bytes);

        if let Some(e) = sc
            .entries
            .iter()
            .rev()
            .find(|e| hex::decode(&e.address_hex).as_deref() == Ok(key))
        {
            return Ok(LookupOutcome::Done(Some(
                hex::decode(&e.value_hex).map_err(|x| x.to_string())?,
            )));
        }
        let mut found = self
            .match_cell(&b0, key)
            .or_else(|| self.match_cell(&b1, key));
        if found.is_none() {
            if let Some(e) = sc
                .stash
                .iter()
                .find(|e| hex::decode(&e.address_hex).as_deref() == Ok(key))
            {
                found = Some(hex::decode(&e.value_hex).map_err(|x| x.to_string())?);
            }
        }
        Ok(LookupOutcome::Done(found))
    }
}

enum LookupOutcome {
    Done(Option<Vec<u8>>),
    Reconfigured,
}

/// Fixed-size pool of [`TorPirClient`]s for concurrent lookups.
///
/// One mutex-wrapped client serializes every PIR call (~N× lookup latency under
/// load). A pool of size S lets up to S lookups progress at once; extras wait
/// for a free client.
struct ClientPool {
    idle: Mutex<VecDeque<TorPirClient>>,
    permits: Semaphore,
}

impl ClientPool {
    fn new(clients: Vec<TorPirClient>) -> Self {
        let n = clients.len();
        Self {
            idle: Mutex::new(VecDeque::from(clients)),
            permits: Semaphore::new(n),
        }
    }

    async fn checkout(&self) -> (tokio::sync::SemaphorePermit<'_>, TorPirClient) {
        let permit = self.permits.acquire().await.expect("semaphore closed");
        let client = self
            .idle
            .lock()
            .await
            .pop_front()
            .expect("permit implies an idle client");
        (permit, client)
    }

    async fn checkin(&self, client: TorPirClient) {
        self.idle.lock().await.push_back(client);
    }
}

/// [`kohaku_privacy_rpc::AsyncLookupBackend`] over a pool of inspire PIR clients + Tor HTTP.
pub struct TorPirClientLookup {
    pool: ClientPool,
    pool_size: usize,
}

impl TorPirClientLookup {
    /// Bootstrap one manifest fetch, then clone into a pool of `pool_size` clients.
    ///
    /// # Errors
    ///
    /// Returns when Tor/manifest setup fails, or `pool_size` is zero.
    pub async fn connect(tor: TorRpc, base: &str, pool_size: usize) -> Result<Arc<Self>, String> {
        if pool_size == 0 {
            return Err("pir pool size must be >= 1".into());
        }
        let prototype = TorPirClient::connect(tor, base).await?;
        let clients = (0..pool_size).map(|_| prototype.clone()).collect();
        Ok(Arc::new(Self {
            pool: ClientPool::new(clients),
            pool_size,
        }))
    }

    /// Number of concurrent PIR clients in the pool.
    #[must_use]
    pub const fn pool_size(&self) -> usize {
        self.pool_size
    }

    async fn lookup_one(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let (permit, mut client) = self.pool.checkout().await;
        let result = if key.len() == 20 {
            client.lookup_address(key).await
        } else {
            client.lookup(key).await
        };
        self.pool.checkin(client).await;
        drop(permit);
        result
    }
}

#[async_trait::async_trait]
impl kohaku_privacy_rpc::AsyncLookupBackend for TorPirClientLookup {
    async fn lookup_batch(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, kohaku_pir_rpc::PirProviderError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        // Run lookups concurrently; pool size caps in-flight Tor/PIR work.
        let futs = keys.iter().map(|key| {
            let key = key.clone();
            async move {
                self.lookup_one(&key)
                    .await
                    .map_err(kohaku_pir_rpc::PirProviderError::Client)
            }
        });
        futures::future::try_join_all(futs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn pool_allows_concurrent_checkouts() {
        let idle = Mutex::new(VecDeque::from((0..4_u8).collect::<Vec<_>>()));
        let permits = Semaphore::new(4);
        let peak = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));

        let futs: Vec<_> = (0..8)
            .map(|_| {
                let peak = Arc::clone(&peak);
                let in_flight = Arc::clone(&in_flight);
                let idle = &idle;
                let permits = &permits;
                async move {
                    let permit = permits.acquire().await.unwrap();
                    let idx = idle.lock().await.pop_front().unwrap();
                    let n = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    idle.lock().await.push_back(idx);
                    drop(permit);
                }
            })
            .collect();
        futures::future::join_all(futs).await;
        assert!(
            peak.load(Ordering::SeqCst) >= 4,
            "expected at least 4 concurrent checkouts, got {}",
            peak.load(Ordering::SeqCst)
        );
    }
}
