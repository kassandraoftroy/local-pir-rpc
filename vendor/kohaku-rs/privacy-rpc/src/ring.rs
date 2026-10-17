//! Baked-in public Ethereum execution and beacon endpoints.
//!
//! Each fallback attempt picks a random provider and, on failure, retries a
//! different registrable domain until one answers.

use url::Url;

use crate::PrivacyError;

/// Public execution JSON-RPC endpoints. Each host is a different provider, so a
/// retry leaves the one that just failed.
pub const EXECUTION_URLS: &[&str] = &[
    "https://ethereum.publicnode.com",
    "https://eth.drpc.org",
    "https://eth.llamarpc.com",
];

/// Public beacon APIs that serve the light-client routes Helios needs.
pub const CONSENSUS_URLS: &[&str] = &[
    "https://ethereum-beacon-api.publicnode.com",
    "https://lodestar-mainnet.chainsafe.io",
    "https://docs-demo.quiknode.pro",
];

/// How many distinct providers to try before giving up.
pub const MAX_ATTEMPTS: usize = 3;

/// Mainnet execution and consensus URL rings.
#[derive(Clone, Debug)]
pub struct RpcRing {
    /// Execution JSON-RPC URLs, including any user extras.
    pub execution: Vec<Url>,
    /// Beacon API bases.
    pub consensus: Vec<Url>,
}

impl RpcRing {
    /// Baked-in mainnet rings plus `extra_execution` URLs appended after them.
    ///
    /// # Errors
    ///
    /// Returns [`PrivacyError::InvalidUrl`] when an extra URL does not parse.
    pub fn mainnet(extra_execution: &[String]) -> Result<Self, PrivacyError> {
        let mut execution = parse_all(EXECUTION_URLS)?;
        for extra in extra_execution {
            let trimmed = extra.trim();
            if trimmed.is_empty() {
                continue;
            }
            execution
                .push(Url::parse(trimmed).map_err(|e| PrivacyError::InvalidUrl(e.to_string()))?);
        }
        Ok(Self {
            execution,
            consensus: parse_all(CONSENSUS_URLS)?,
        })
    }

    /// Up to [`MAX_ATTEMPTS`] execution URLs on distinct domains, shuffled.
    #[must_use]
    pub fn execution_attempts(&self) -> Vec<Url> {
        distinct_attempts(&self.execution, MAX_ATTEMPTS)
    }

    /// Up to `n` consensus URLs on distinct domains, shuffled.
    #[must_use]
    pub fn consensus_attempts(&self, n: usize) -> Vec<Url> {
        distinct_attempts(&self.consensus, n)
    }
}

fn parse_all(urls: &[&str]) -> Result<Vec<Url>, PrivacyError> {
    urls.iter()
        .map(|u| Url::parse(u).map_err(|e| PrivacyError::InvalidUrl(e.to_string())))
        .collect()
}

/// Last two DNS labels, so `ethereum-rpc.publicnode.com` and
/// `ethereum.publicnode.com` count as one provider.
#[must_use]
pub fn provider_key(url: &Url) -> String {
    let host = url.host_str().unwrap_or("").to_ascii_lowercase();
    let parts: Vec<&str> = host.split('.').filter(|p| !p.is_empty()).collect();
    if parts.len() >= 2 {
        let n = parts.len();
        format!("{}.{}", parts[n - 2], parts[n - 1])
    } else {
        host
    }
}

pub(crate) fn distinct_attempts(urls: &[Url], n: usize) -> Vec<Url> {
    if urls.is_empty() || n == 0 {
        return Vec::new();
    }
    let mut order: Vec<usize> = (0..urls.len()).collect();
    shuffle(&mut order);
    let mut seen = Vec::new();
    let mut out = Vec::new();
    for idx in order {
        let key = provider_key(&urls[idx]);
        if seen.iter().any(|k| k == &key) {
            continue;
        }
        seen.push(key);
        out.push(urls[idx].clone());
        if out.len() == n {
            break;
        }
    }
    out
}

fn shuffle(items: &mut [usize]) {
    let mut state = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0x9E37_79B9_7F4A_7C15, |d| d.as_nanos() as u64);
    for i in (1..items.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        items.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publicnode_hosts_share_a_provider() {
        let a = Url::parse("https://ethereum-rpc.publicnode.com").unwrap();
        let b = Url::parse("https://ethereum.publicnode.com").unwrap();
        assert_eq!(provider_key(&a), provider_key(&b));
        assert_ne!(
            provider_key(&a),
            provider_key(&Url::parse("https://rpc.ankr.com/eth").unwrap())
        );
    }

    #[test]
    fn attempts_are_distinct_domains_and_capped() {
        let ring = RpcRing::mainnet(&["https://eth.llamarpc.com".into()]).unwrap();
        let attempts = ring.execution_attempts();
        assert!(attempts.len() <= MAX_ATTEMPTS);
        assert!(attempts.len() >= 2);
        let keys: Vec<_> = attempts.iter().map(provider_key).collect();
        let mut uniq = keys.clone();
        uniq.sort();
        uniq.dedup();
        assert_eq!(uniq.len(), keys.len());
    }

    #[test]
    fn extra_urls_are_appended() {
        let ring = RpcRing::mainnet(&["https://eth.example/rpc".into()]).unwrap();
        assert!(
            ring.execution
                .iter()
                .any(|u| u.as_str().starts_with("https://eth.example"))
        );
    }
}
