use std::path::PathBuf;
use std::sync::Arc;

use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use kohaku_pir_rpc::{DatasetManifest, MapFallback, MapLookup, PirRouter};
use kohaku_tor_rpc::TorRpc;
use url::Url;

use crate::proxy::RpcOutbound;
use crate::verified::{VerifiedRpc, default_data_dir};
use crate::{
    AsyncLookupBackend, DefaultIsolationPolicy, IsolationPolicy, LiveTorRpc, PrivacyConnect,
    PrivacyError, PrivacyTransport, StateVerifier, SyncLookupAdapter, TorPirLookup, TorRpcBackend,
};

/// Mix-and-match builder for the privacy orchestrator.
///
/// `TorPIR` preset: `.tor(...).pir_over_tor(accounts_url, token_url, datasets).connect()`.
/// Future Helios+Tor: `.tor(...).verifier(helios).connect()` without PIR.
pub struct PrivacyBuilder {
    rpc_url: Option<Url>,
    /// Extra execution URLs appended to the baked-in public ring.
    extra_rpc_urls: Vec<String>,
    local_fallback: bool,
    oldest_block: Option<u64>,
    head_block: Option<u64>,
    verified: Option<Arc<VerifiedRpc>>,
    tor: Option<Arc<dyn TorRpcBackend>>,
    accounts_lookup: Option<Arc<dyn AsyncLookupBackend>>,
    tokens_lookup: Option<Arc<dyn AsyncLookupBackend>>,
    names_lookup: Option<Arc<dyn AsyncLookupBackend>>,
    /// When set, build [`TorPirLookup`] for accounts at `build_transport` time.
    pir_accounts_url: Option<String>,
    /// Optional token-storage PIR URL.
    pir_tokens_url: Option<String>,
    datasets: Vec<DatasetManifest>,
    isolation: Arc<dyn IsolationPolicy>,
    verifier: Option<Arc<dyn StateVerifier>>,
}

impl Default for PrivacyBuilder {
    fn default() -> Self {
        Self {
            rpc_url: None,
            extra_rpc_urls: Vec::new(),
            local_fallback: false,
            oldest_block: None,
            head_block: None,
            verified: None,
            tor: None,
            accounts_lookup: None,
            tokens_lookup: None,
            names_lookup: None,
            pir_accounts_url: None,
            pir_tokens_url: None,
            datasets: Vec::new(),
            isolation: Arc::new(DefaultIsolationPolicy),
            verifier: None,
        }
    }
}

impl PrivacyBuilder {
    /// Start a builder. Fallback goes to Helios after [`open_verified`](Self::open_verified),
    /// or to an explicit URL from [`fallback_url`](Self::fallback_url).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Direct fallback URL. Tests use this. Production uses [`open_verified`](Self::open_verified).
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyError::InvalidUrl`] when `rpc_url` does not parse.
    pub fn fallback_url(mut self, rpc_url: &str) -> Result<Self, PrivacyError> {
        self.rpc_url =
            Some(Url::parse(rpc_url).map_err(|e| PrivacyError::InvalidUrl(e.to_string()))?);
        Ok(self)
    }

    /// Append execution endpoints to the baked-in public ring.
    #[must_use]
    pub fn extra_rpc_urls(mut self, urls: Vec<String>) -> Self {
        self.extra_rpc_urls.extend(urls);
        self
    }

    /// Post fallback JSON-RPC to the fallback URL on localhost, not through Tor.
    #[must_use]
    pub fn local_fallback(mut self) -> Self {
        self.local_fallback = true;
        self
    }

    /// Reject calls older than this execution block.
    #[must_use]
    pub fn oldest_block(mut self, block: u64) -> Self {
        self.oldest_block = Some(block);
        self
    }

    /// Chain head used to warn when a PIR snapshot lags.
    #[must_use]
    pub fn head_block(mut self, block: u64) -> Self {
        self.head_block = Some(block);
        self
    }

    /// Use an already-started Helios stack as the fallback.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyError::InvalidUrl`] when the Helios URL cannot be stored.
    pub fn verified(self, verified: Arc<VerifiedRpc>) -> Result<Self, PrivacyError> {
        let rpc_url = verified.rpc_url().as_str().to_string();
        let oldest = verified.oldest_block();
        let head = verified.head_block();
        let mut builder = self
            .fallback_url(&rpc_url)?
            .local_fallback()
            .oldest_block(oldest);
        if let Some(head) = head {
            builder = builder.head_block(head);
        }
        builder.verified = Some(verified);
        Ok(builder)
    }

    /// Start Helios against the public ring (plus [`extra_rpc_urls`](Self::extra_rpc_urls))
    /// and point fallback at it.
    ///
    /// # Errors
    ///
    /// Returns when Helios or the beacon checkpoint cannot be started.
    pub async fn open_verified(
        self,
        outbound: RpcOutbound,
        data_dir: PathBuf,
    ) -> Result<Self, PrivacyError> {
        let verified = VerifiedRpc::start(outbound, &self.extra_rpc_urls, data_dir).await?;
        self.verified(Arc::new(verified))
    }

    /// Use a live [`TorRpc`] for all egress.
    #[must_use]
    pub fn tor(mut self, tor: TorRpc) -> Self {
        self.tor = Some(Arc::new(LiveTorRpc::new(tor)));
        self
    }

    /// Clearnet mode. Fallback must be localhost Helios ([`local_fallback`](Self::local_fallback)
    /// or [`open_verified`](Self::open_verified)); this backend errors if a call tries Tor.
    #[must_use]
    pub fn without_tor(mut self) -> Self {
        self.tor = Some(Arc::new(crate::tor_rpc::DisabledTor));
        self
    }

    /// Inject a custom Tor backend (tests, alternate transports).
    #[must_use]
    pub fn tor_backend(mut self, tor: Arc<dyn TorRpcBackend>) -> Self {
        self.tor = Some(tor);
        self
    }

    /// Enable PIR over **shared Tor** to account and optional token PIR bases.
    ///
    /// `token_pir_url`: `None` keeps ERC-20 `balanceOf` on fallback RPC (still
    /// Multicall-packed). Pass `Some(url)` for the inspire storage table (`:18091`).
    #[must_use]
    pub fn pir_over_tor(
        mut self,
        accounts_pir_url: &str,
        token_pir_url: Option<&str>,
        datasets: Vec<DatasetManifest>,
    ) -> Self {
        self.pir_accounts_url = Some(accounts_pir_url.to_string());
        self.pir_tokens_url = token_pir_url.map(str::to_string);
        self.accounts_lookup = None;
        self.tokens_lookup = None;
        self.datasets = datasets;
        self
    }

    /// Enable PIR with custom async lookups (e.g. inspire crypto pools).
    #[must_use]
    pub fn pir_lookup(
        mut self,
        accounts: Arc<dyn AsyncLookupBackend>,
        tokens: Option<Arc<dyn AsyncLookupBackend>>,
        names: Option<Arc<dyn AsyncLookupBackend>>,
        datasets: Vec<DatasetManifest>,
    ) -> Self {
        self.accounts_lookup = Some(accounts);
        self.tokens_lookup = tokens;
        self.names_lookup = names;
        self.pir_accounts_url = None;
        self.pir_tokens_url = None;
        self.datasets = datasets;
        self
    }

    /// Override the default shared / isolated-by-EOA policy.
    #[must_use]
    pub fn isolation_policy(mut self, policy: Arc<dyn IsolationPolicy>) -> Self {
        self.isolation = policy;
        self
    }

    /// Attach an optional lightclient verifier (Helios / Colibri later).
    #[must_use]
    pub fn verifier(mut self, verifier: Arc<dyn StateVerifier>) -> Self {
        self.verifier = Some(verifier);
        self
    }

    /// Build the [`PrivacyTransport`].
    ///
    /// # Errors
    ///
    /// Returns when Tor was not configured, or a PIR URL is invalid.
    pub fn build_transport(self) -> Result<PrivacyTransport, PrivacyError> {
        let tor = self
            .tor
            .ok_or_else(|| PrivacyError::Builder("tor backend is required".into()))?;

        let (accounts_lookup, tokens_lookup, names_lookup) =
            if let Some(accounts_url) = self.pir_accounts_url {
                let accounts = Arc::new(TorPirLookup::new(Arc::clone(&tor), &accounts_url)?)
                    as Arc<dyn AsyncLookupBackend>;
                let tokens = self
                    .pir_tokens_url
                    .map(|u| {
                        TorPirLookup::new(Arc::clone(&tor), &u)
                            .map(|l| Arc::new(l) as Arc<dyn AsyncLookupBackend>)
                    })
                    .transpose()?;
                (Some(accounts), tokens, self.names_lookup)
            } else {
                (self.accounts_lookup, self.tokens_lookup, self.names_lookup)
            };

        let rpc_url = self.rpc_url.ok_or_else(|| {
            PrivacyError::Builder(
                "fallback URL is required (call .fallback_url or .open_verified)".into(),
            )
        })?;
        // Router is used for plan/classify only; its sync LookupBackend is inert.
        let router = Arc::new(PirRouter::from_parts(
            Arc::new(MapLookup::default()),
            Arc::new(MapFallback::default()),
            self.datasets,
        ));
        if let Some(block) = self.oldest_block {
            router.set_oldest_block(block);
        }
        if let Some(block) = self.head_block {
            router.set_head_block(block);
        }
        let mut transport = PrivacyTransport::new(
            router,
            accounts_lookup,
            tokens_lookup,
            names_lookup,
            tor,
            rpc_url,
            self.isolation,
            self.verifier,
        );
        if self.local_fallback {
            transport = transport.with_local_fallback();
        }
        if let Some(verified) = self.verified {
            transport = transport.hold_verified(verified);
        }
        Ok(transport)
    }

    /// Build a type-erased Alloy provider.
    ///
    /// # Errors
    ///
    /// Returns when the transport cannot be built or Alloy connect fails.
    pub async fn connect(self) -> Result<DynProvider, PrivacyError> {
        let transport = self.build_transport()?;
        ProviderBuilder::default()
            .connect_with(&PrivacyConnect::new(transport))
            .await
            .map(Provider::erased)
            .map_err(|e| PrivacyError::Transport(e.to_string()))
    }
}

/// Convenience: Tor + optional PIR, with Helios verifying the public-RPC ring.
///
/// # Errors
///
/// Returns when Helios cannot sync or Alloy connect fails.
pub async fn connect_tor_pir(
    tor: TorRpc,
    accounts_pir_url: &str,
    token_pir_url: Option<&str>,
    datasets: Vec<DatasetManifest>,
) -> Result<DynProvider, PrivacyError> {
    let transport = PrivacyBuilder::new()
        .tor(tor.clone())
        .pir_over_tor(accounts_pir_url, token_pir_url, datasets)
        .open_verified(RpcOutbound::tor(tor), default_data_dir())
        .await?
        .build_transport()?;
    ProviderBuilder::default()
        .connect_with(&PrivacyConnect::new(transport))
        .await
        .map(Provider::erased)
        .map_err(|e| PrivacyError::Transport(e.to_string()))
}

/// Test helper: sync [`MapLookup`](kohaku_pir_rpc::MapLookup) as async PIR (no Tor HTTP).
#[must_use]
pub fn sync_pir_lookup(
    lookup: Arc<dyn kohaku_pir_rpc::LookupBackend>,
) -> Arc<dyn AsyncLookupBackend> {
    Arc::new(SyncLookupAdapter::new(lookup))
}
