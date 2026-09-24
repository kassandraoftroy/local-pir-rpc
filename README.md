# local-pir-rpc

Thin `localhost:8545` Ethereum JSON-RPC facade over
[`kohaku-pir-provider`](../kohaku-rs/crates/pir-provider). Hybrid routing
(PIR allowlist vs fallback RPC) lives in that crate; this binary only wires
`pir-client` as the `LookupBackend` and serves HTTP for process-isolated demos
with [kohaku-cli](https://github.com/kassandraoftroy/kohaku-cli).

## Two-terminal demo

**Terminal 1** — start the proxy (needs a normal mainnet JSON-RPC URL):

```bash
export ETH_RPC_URL="https://your-mainnet-rpc.example"
export PIR_URL="https://your-pir-endpoint.example"
cargo run --release -- \
  --listen 127.0.0.1:8545 \
  --pir-url "$PIR_URL" \
  --rpc-url "$ETH_RPC_URL"
```

**Terminal 2** — point kohaku-cli at the proxy:

```bash
kohaku <command> --rpc-url http://localhost:8545
```

(kohaku-cli already defaults to `http://localhost:8545` when `--rpc-url` /
`RPC_URL` are unset.)

Watch Terminal 1: `eth_getBalance` / `eth_getTransactionCount` (latest) log as
PIR routes; `eth_chainId`, `eth_getLogs`, etc. log as fallback.

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--listen` | | `127.0.0.1:8545` |
| `--pir-url` | `PIR_URL` | *(required)* |
| `--rpc-url` | `ETH_RPC_URL` | *(required)* |

## Dependencies

Path crates (sibling checkouts):

- `../kohaku-rs/crates/pir-provider`
- `../inspire-gpu-serving/crates/client` (CPU-only; no GPU)
