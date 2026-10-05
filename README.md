# local-pir-rpc

Thin `localhost:8545` Ethereum JSON-RPC facade over
[`kohaku-privacy-rpc`](https://github.com/ethereum/kohaku-rs) (`Tor` + PIR). All
remote traffic — PIR (`/manifest`, encrypted `/lookup`) and fallback Ethereum
JSON-RPC — egresses through Arti on **shared** circuits for PIR and
shared/isolated circuits for fallback RPC per the privacy orchestrator.

Hybrid routing (PIR allowlist vs fallback RPC) lives in
[`kohaku-pir-rpc`](https://github.com/ethereum/kohaku-rs/tree/experiments/pir-v1/crates/pir-rpc).
This binary wires inspire-gpu-serving crypto over Tor HTTP and serves HTTP for
process-isolated demos with
[kohaku-cli](https://github.com/kassandraoftroy/kohaku-cli).

## Two-terminal demo

**Terminal 1** — start the proxy (needs mainnet RPC + PIR URLs reachable via Tor
exits):

```bash
export ETH_RPC_URL="https://your-mainnet-rpc.example"
export PIR_URL="https://your-pir-endpoint.example"
cargo run --release -- \
  --listen 127.0.0.1:8545 \
  --pir-url "$PIR_URL" \
  --rpc-url "$ETH_RPC_URL"
```

First run bootstraps Tor; PIR manifest fetch and lookups use Tor HTTP.

**Terminal 2** — point kohaku-cli at the proxy:

```bash
kohaku <command> --rpc-url http://localhost:8545
```

Watch Terminal 1: `eth_getBalance` / `eth_getTransactionCount` (latest) log as
PIR routes; `eth_chainId`, `eth_getLogs`, etc. log as fallback (still over Tor).

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--listen` | | `127.0.0.1:8545` |
| `--pir-url` | `PIR_URL` | *(required)* |
| `--rpc-url` | `ETH_RPC_URL` | *(required)* |

## Dependencies

Path crates (sibling checkouts):

- `../kohaku-rs/crates/privacy-rpc`
- `../kohaku-rs/crates/tor-rpc`
- `../kohaku-rs/crates/pir-rpc`
- `../inspire-gpu-serving/crates/{backend-ffi,keyword}` (CPU-only PIR crypto)
