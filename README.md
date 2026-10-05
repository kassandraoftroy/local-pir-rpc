# local-pir-rpc

Thin `localhost:8545` Ethereum JSON-RPC facade for
[kohaku-cli](https://github.com/kassandraoftroy/kohaku-cli) demos.

**Default:** Tor everywhere via
[`kohaku-privacy-rpc`](https://github.com/ethereum/kohaku-rs) — PIR and fallback
Ethereum RPC both egress through Arti.

**`--without-tor`:** clearnet only via
[`kohaku-pir-rpc`](https://github.com/ethereum/kohaku-rs/tree/experiments/pir-v1/crates/pir-rpc)
— no Tor on PIR or fallback (good for local/dev PIR servers).

## Tor mode (default)

Needs PIR + RPC URLs reachable via Tor exits:

```bash
export ETH_RPC_URL="https://your-mainnet-rpc.example"
export PIR_URL="https://your-pir-endpoint.example"
cargo run --release -- \
  --listen 127.0.0.1:8545 \
  --pir-url "$PIR_URL" \
  --rpc-url "$ETH_RPC_URL"
```

First run bootstraps Tor; then fetches the PIR manifest and serves lookups over Tor.

## Clearnet mode

```bash
cargo run --release -- \
  --without-tor \
  --listen 127.0.0.1:8545 \
  --pir-url "$PIR_URL" \
  --rpc-url "$ETH_RPC_URL"
```

## Client

```bash
kohaku <command> --rpc-url http://localhost:8545
```

Watch logs: `eth_getBalance` / `eth_getTransactionCount` (latest) as PIR;
`eth_chainId`, `eth_getLogs`, etc. as fallback.

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--listen` | | `127.0.0.1:8545` |
| `--pir-url` | `PIR_URL` | *(required)* |
| `--rpc-url` | `ETH_RPC_URL` | *(required)* |
| `--pir-pool-size` | `PIR_POOL_SIZE` | `16` |
| `--without-tor` | | off (Tor on) |

### Concurrent PIR lookups

A single `PirClient` behind a mutex serializes lookups (see
[issue #1](https://github.com/kassandraoftroy/local-pir-rpc/issues/1)). Both
modes keep a pool of clients (default 16) so concurrent lookups stay near one
lookup of latency.

## Dependencies

Path crates (sibling checkouts):

- `../kohaku-rs/crates/{privacy-rpc,tor-rpc,pir-rpc}`
- `../inspire-gpu-serving/crates/{client,backend-ffi,keyword}`
