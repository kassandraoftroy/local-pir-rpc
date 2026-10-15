//! Light-client history window. Calls that name a block older than the pinned
//! checkpoint's execution block are rejected instead of being clipped.

use serde_json::Value;

use crate::PirProviderError;

/// `true` when `method` asks for a block older than `oldest` (inclusive floor).
///
/// `latest`, `safe`, `finalized`, `pending`, and block hashes are not rejected
/// here. A hash is left to Helios. A log filter that starts below `oldest` and
/// ends at `latest` is rejected as a whole.
#[must_use]
pub fn outside_window(method: &str, params: &Value, oldest: u64) -> bool {
    if method == "eth_getLogs" {
        return logs_outside(params, oldest);
    }
    if method == "eth_feeHistory" {
        return fee_history_outside(params, oldest);
    }
    let Some(index) = block_param_index(method) else {
        return false;
    };
    let Some(tag) = params.as_array().and_then(|arr| arr.get(index)) else {
        return false;
    };
    tag_outside(tag, oldest)
}

fn block_param_index(method: &str) -> Option<usize> {
    match method {
        "eth_getBlockByNumber"
        | "eth_getBlockTransactionCountByNumber"
        | "eth_getTransactionByBlockNumberAndIndex"
        | "eth_getUncleCountByBlockNumber"
        | "eth_getUncleByBlockNumberAndIndex" => Some(0),
        "eth_getBalance"
        | "eth_getCode"
        | "eth_getTransactionCount"
        | "eth_call"
        | "eth_estimateGas"
        | "eth_createAccessList" => Some(1),
        "eth_getStorageAt" | "eth_getProof" => Some(2),
        _ => None,
    }
}

fn logs_outside(params: &Value, oldest: u64) -> bool {
    let Some(filter) = params.as_array().and_then(|arr| arr.first()) else {
        return false;
    };
    if filter.get("blockHash").is_some()
        && filter.get("fromBlock").is_none()
        && filter.get("toBlock").is_none()
    {
        return false;
    }
    tag_outside(filter.get("fromBlock").unwrap_or(&Value::Null), oldest)
        || tag_outside(filter.get("toBlock").unwrap_or(&Value::Null), oldest)
}

fn fee_history_outside(params: &Value, oldest: u64) -> bool {
    let Some(arr) = params.as_array() else {
        return false;
    };
    let Some(newest) = arr.get(1) else {
        return false;
    };
    if let Some(number) = tag_number(newest) {
        let count = arr.first().and_then(tag_number).unwrap_or(1).max(1);
        let start = number.saturating_sub(count.saturating_sub(1));
        return start < oldest;
    }
    tag_outside(newest, oldest)
}

fn tag_outside(tag: &Value, oldest: u64) -> bool {
    match tag {
        Value::Null => false,
        Value::String(s) if s.is_empty() || is_head_tag(s) => false,
        Value::String(s) if s.eq_ignore_ascii_case("earliest") => oldest > 0,
        Value::String(s) if is_block_hash(s) => false,
        other => tag_number(other).is_some_and(|n| n < oldest),
    }
}

fn is_head_tag(s: &str) -> bool {
    s.eq_ignore_ascii_case("latest")
        || s.eq_ignore_ascii_case("safe")
        || s.eq_ignore_ascii_case("finalized")
        || s.eq_ignore_ascii_case("pending")
}

fn is_block_hash(s: &str) -> bool {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit())
}

fn tag_number(tag: &Value) -> Option<u64> {
    match tag {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => parse_qty(s),
        _ => None,
    }
}

fn parse_qty(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        if hex.is_empty() || hex.len() == 64 {
            return None;
        }
        return u64::from_str_radix(hex, 16).ok();
    }
    if s.chars().all(|c| c.is_ascii_digit()) && !s.is_empty() {
        s.parse().ok()
    } else {
        None
    }
}

/// Error returned when [`outside_window`] is true.
#[must_use]
pub fn outside_window_error() -> PirProviderError {
    PirProviderError::OutsideWindow
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const OLDEST: u64 = 1000;

    #[test]
    fn latest_balance_is_inside() {
        assert!(!outside_window(
            "eth_getBalance",
            &json!(["0x01", "latest"]),
            OLDEST
        ));
    }

    #[test]
    fn old_balance_is_outside() {
        assert!(outside_window(
            "eth_getBalance",
            &json!(["0x01", "0x10"]),
            OLDEST
        ));
    }

    #[test]
    fn logs_that_start_below_the_window_are_rejected_whole() {
        assert!(outside_window(
            "eth_getLogs",
            &json!([{ "fromBlock": "0x1", "toBlock": "latest" }]),
            OLDEST
        ));
    }

    #[test]
    fn recent_logs_are_inside() {
        assert!(!outside_window(
            "eth_getLogs",
            &json!([{ "fromBlock": "0x3e8", "toBlock": "latest" }]),
            OLDEST
        ));
    }

    #[test]
    fn a_block_hash_is_left_to_helios() {
        let hash = format!("0x{}", "ab".repeat(32));
        assert!(!outside_window(
            "eth_getBlockByHash",
            &json!([hash]),
            OLDEST
        ));
        assert!(!outside_window(
            "eth_getLogs",
            &json!([{ "blockHash": format!("0x{}", "cd".repeat(32)) }]),
            OLDEST
        ));
    }

    #[test]
    fn earliest_is_outside() {
        assert!(outside_window(
            "eth_call",
            &json!([{ "to": "0x01" }, "earliest"]),
            OLDEST
        ));
    }
}
