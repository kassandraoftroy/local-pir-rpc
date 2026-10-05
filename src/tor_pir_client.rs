//! inspire-gpu-serving PIR client logic over [`TorRpc`] (shared circuits, no isolation).
//!
//! Adapted from `pir-client`; HTTP uses Tor instead of clearnet `ureq`.

use std::sync::Arc;

use kohaku_tor_rpc::TorRpc;
use pir_backend_ffi::{pack_query, ClientQuery, Params};
use pir_keyword::cuckoo::CuckooHash;
use pir_keyword::manifest::{Manifest, SidecarBroadcast};
use pir_keyword::slots::unpack_bytes;
use tokio::sync::Mutex;
use url::Url;

const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;

/// Tor-backed PIR HTTP client (manifest + encrypted `/lookup` POSTs).
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

/// [`kohaku_privacy_rpc::AsyncLookupBackend`] over real inspire PIR + Tor HTTP.
pub struct TorPirClientLookup {
    client: Mutex<TorPirClient>,
}

impl TorPirClientLookup {
    /// Bootstrap manifest and crypto over Tor.
    pub async fn connect(tor: TorRpc, base: &str) -> Result<Arc<Self>, String> {
        let client = TorPirClient::connect(tor, base).await?;
        Ok(Arc::new(Self {
            client: Mutex::new(client),
        }))
    }
}

#[async_trait::async_trait]
impl kohaku_privacy_rpc::AsyncLookupBackend for TorPirClientLookup {
    async fn lookup_batch(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, kohaku_pir_rpc::PirProviderError> {
        let mut client = self.client.lock().await;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let result = if key.len() == 20 {
                client.lookup_address(key).await
            } else {
                client.lookup(key).await
            };
            out.push(result.map_err(kohaku_pir_rpc::PirProviderError::Client)?);
        }
        Ok(out)
    }
}
