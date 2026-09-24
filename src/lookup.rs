use std::sync::Mutex;

use kohaku_pir_provider::{LookupBackend, PirProviderError};
use pir_client::PirClient;

/// Adapts `pir_client::PirClient` to [`LookupBackend`].
///
/// Account routes pass a raw 20-byte address; use [`PirClient::lookup_address`]
/// so keccak (or other) key derivation from the remote manifest applies.
/// Longer keys (e.g. derived `eth_call` keys) go through [`PirClient::lookup`].
pub struct PirLookup(pub Mutex<PirClient>);

impl LookupBackend for PirLookup {
    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PirProviderError> {
        let mut client = self
            .0
            .lock()
            .map_err(|_| PirProviderError::Client("PirClient mutex poisoned".into()))?;
        let result = if key.len() == 20 {
            client.lookup_address(key)
        } else {
            client.lookup(key)
        };
        result
            .map(|found| found.map(|l| l.value))
            .map_err(PirProviderError::Client)
    }
}
