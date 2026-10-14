# local-pir-rpc

A localhost Ethereum JSON-RPC for wallets. Point a wallet at `http://127.0.0.1:8545`.

This is not an Ethereum node. It answers JSON-RPC from a process on your machine.
Historical and non-PIR reads go to a baked-in ring of public endpoints and are
checked by an in-process [Helios](https://github.com/a16z/helios) light client.
Optional PIR answers account, token, and name reads without sending those keys
to a public node.

Public RPC traffic leaves through Tor by default. `--without-tor` dials the same
ring in clearnet.

## Run

```bash
cargo run --release
```

The first start fetches a beacon checkpoint about twelve days old and syncs
Helios from it. That can take several minutes, longer over Tor. The checkpoint
is stored in `~/.local-pir-rpc` (or `$LOCAL_PIR_RPC_DIR`) and reused until it
is about thirteen days old.

Wallets and scripts then use:

```text
http://127.0.0.1:8545
```

Calls whose block is older than the pinned checkpoint fail with JSON-RPC error
`-32000`, message `block is outside the light-client window`. `latest`, `safe`,
`finalized`, and `pending` stay inside the window. A log range that starts
below the window is rejected rather than clipped.

## Optional PIR

```bash
cargo run --release -- --pir-url "https://your-pir-endpoint.example"
```

The account server uses the port you pass. Tokens are that port plus one, names
plus two (`http://host:18090` → `18090` / `18091` / `18092`). PIR traffic uses
Tor unless you pass `--without-tor`. Without `--pir-url`, every call is a
Helios-verified public read.

## Extra public endpoints

The built-in execution ring is PublicNode, Ankr, Cloudflare, 1RPC, dRPC,
BlockPI, and Alchemy's public mainnet URL. Each fallback picks a provider at
random and retries a different provider up to three times. Add more with a
repeatable flag:

```bash
cargo run --release -- \
  --extra-rpc-url "https://your-mainnet-rpc.example" \
  --extra-rpc-url "https://another.example"
```

## Clearnet

```bash
cargo run --release -- --without-tor
```

## Logs

```text
eth_getBalance  pir  312ms
batch 8  1.4s
  eth_call  pir  balanceOf  0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
  eth_getLogs  helios
  eth_blockNumber  helios
eth_call  local  1ms  stealthMetaAddressOf  0x6538E6bf4B0eBd30A8Ea093027Ac2422ce5d6538
```

`pir` is an account, token, or names lookup. The first `stealthMetaAddressOf` or
`stealth-address-scheme-1` text read in a session is `pir` while it loads the
names sidecar; later ones are `local`. `helios` is a public-ring read checked by
the light client (including a name that was too long for the table, or a text
record other than `stealth-address-scheme-1`). `local` is `computeId` or a
stealth answer taken from the names sidecar, which never leaves the process.
A batch prints one elapsed time, then one line per call. An `eth_call` line
also names the function and the contract.

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--listen` | | `127.0.0.1:8545` |
| `--pir-url` | `PIR_URL` | unset (Helios only). Account server, explicit port. Tokens are port+1, names are port+2. |
| `--extra-rpc-url` | | none. Repeat to append execution endpoints to the public ring. |
| `--pir-pool-size` | `PIR_POOL_SIZE` | `2` |
| `--without-tor` | | off (Tor on) |

Checkpoint and Helios data live in `$LOCAL_PIR_RPC_DIR` or `~/.local-pir-rpc`.

### Concurrent PIR lookups

A single `PirClient` behind a mutex serializes lookups (see
[issue #1](https://github.com/kassandraoftroy/local-pir-rpc/issues/1)). Both
modes keep a pool of clients (default 2) so concurrent lookups stay near one
lookup of latency.

## Dependencies

The crates this binary links are vendored under `vendor/`:

- `vendor/kohaku-rs/{privacy-rpc,tor-rpc,pir-rpc}`
- `vendor/inspire/crates/{client,backend-ffi,keyword}` and `vendor/inspire/third_party/inspire-gpu`

`vendor/UPSTREAM.txt` records the upstream commits.
