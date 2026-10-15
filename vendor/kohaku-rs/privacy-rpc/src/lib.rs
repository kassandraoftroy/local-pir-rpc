//! Batch-aware Tor + PIR privacy orchestrator for Alloy JSON-RPC.
//!
//! # TorPIRProvider
//!
//! There is no combinatorial `HeliosTorPIRProvider` type matrix. Compose via
//! [`PrivacyBuilder`]:
//!
//! ```rust,ignore
//! let provider = PrivacyBuilder::new()
//!     .tor(tor.clone())
//!     .pir_over_tor(accounts_pir_url, Some(token_pir_url), datasets)
//!     .open_verified(RpcOutbound::tor(tor), data_dir)
//!     .await?
//!     .connect()
//!     .await?;
//! ```
//!
//! PIR HTTP egresses through Tor. Fallback JSON-RPC is answered by an in-process
//! Helios light client. Helios fetches execution proofs and beacon data through
//! localhost proxies that rotate a baked-in public endpoint ring over Tor.
//! Calls older than the pinned checkpoint fail. Fallback `eth_call` /
//! `eth_getBalance` are still packed into Multicall3; Helios verifies that call.
//!
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

mod async_lookup;
mod builder;
mod error;
mod isolation;
mod multicall;
mod proxy;
mod ring;
mod tor_rpc;
mod transport;
mod verified;
mod verifier;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;

pub use async_lookup::{
    AsyncLookupBackend, CachingAsyncLookup, DEFAULT_PIR_CACHE_TTL, SyncLookupAdapter, TorPirLookup,
};
pub use builder::{PrivacyBuilder, connect_tor_pir, sync_pir_lookup};
pub use error::PrivacyError;
pub use isolation::{CircuitChoice, DefaultIsolationPolicy, IsolationPolicy};
pub use proxy::RpcOutbound;
pub use ring::RpcRing;
pub use tor_rpc::{DisabledTor, LiveTorRpc, TorRpcBackend};
pub use transport::{PrivacyConnect, PrivacyTransport};
pub use verified::{VerifiedRpc, default_data_dir};
pub use verifier::StateVerifier;
