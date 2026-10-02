use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use kohaku_pir_provider::{LookupBackend, PirProviderError};
use pir_client::PirClient;

/// Adapts `pir_client::PirClient` to [`LookupBackend`].
///
/// Account routes pass a raw 20-byte address; use [`PirClient::lookup_address`]
/// so keccak (or other) key derivation from the remote manifest applies.
/// Longer keys (e.g. derived `eth_call` keys) go through [`PirClient::lookup`].
///
/// Holds several clients so concurrent requests do not queue behind one
/// lookup: a request takes an idle client, or waits on the next one in turn.
pub struct PirLookup {
    clients: Vec<Mutex<PirClient>>,
    next: AtomicUsize,
}

impl PirLookup {
    pub fn new(clients: Vec<PirClient>) -> Self {
        Self {
            clients: clients.into_iter().map(Mutex::new).collect(),
            next: AtomicUsize::new(0),
        }
    }
}

impl LookupBackend for PirLookup {
    fn lookup(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PirProviderError> {
        let mut client = match self.clients.iter().find_map(|c| c.try_lock().ok()) {
            Some(idle) => idle,
            None => {
                let i = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
                self.clients[i]
                    .lock()
                    .map_err(|_| PirProviderError::Client("PirClient mutex poisoned".into()))?
            }
        };
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
