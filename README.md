# local-pir-rpc

Privacy-conscious localhost Ethereum JSON-RPC middleware. Run it on your machine and point a wallet at `http://127.0.0.1:8545` instead of a traditional RPC endpoint.

A traditional endpoint sees your IP together with every address, balance, log, and contract you read - linking them all together. This process sits in between. It uses Tor, PIR, and a ring of public Ethereum RPC endpoints so that no single party gets that whole picture and your reads of ethereum chain are maximally obfuscated from any observer.

## What a wallet read goes through

**PIR** answers account, token, and name lookups. The PIR server sees that a lookup happened. The key stays on your machine. Those keys stay off the public ring as well: re-checking a PIR hit against a public node would reveal the key and undo the lookup.

**Tor** carries traffic that has to leave the machine: PIR queries, and the public RPC calls below. An exit node sees the destination. Your IP stays on your machine.

**A ring of public endpoints** serves everything PIR does not: blocks, logs, gas, and calls outside the private tables. Each request picks a provider at random (PublicNode, Ankr, Cloudflare, 1RPC, dRPC, BlockPI, and Alchemy's public mainnet URL). A failure retries a different provider, up to three times. One company does not hold a complete log of your reads.

**A Helios light client** makes those public reads verifiable. The endpoints are ordinary public RPCs and any of them can lie. Helios checks their answers against the beacon chain, so a malicious provider cannot hand the wallet a forged balance, log, or state root inside the window it certifies. The client is pinned to a checkpoint about twelve days old. Calls older than that execution block fail with JSON-RPC error `-32000` and the message `block is outside the light-client window`. `latest`, `safe`, `finalized`, and `pending` stay inside the window. This check is on by default. `--without-light-client` skips it: the same ring still answers, and a lying endpoint can go unnoticed. The history window stays open in that mode.

## Run

```bash
cargo run --release
```

The first start bootstraps Tor, fetches the checkpoint, and syncs Helios. That can take several minutes. The checkpoint is kept in `~/.local-pir-rpc` (or `$LOCAL_PIR_RPC_DIR`) and reused until it is about thirteen days old. `--without-light-client` skips that sync.

Then set the wallet RPC URL to `http://127.0.0.1:8545`.

PIR is on when you pass a server:

```bash
cargo run --release -- --pir-url "https://your-pir-endpoint.example"
```

The account server uses the port you pass. Tokens are that port plus one, names plus two (`http://host:18090` → `18090` / `18091` / `18092`).

## Turning pieces off

Tor, PIR, and the light client are how the middleware earns its privacy and its verifiability. Each one can be disabled.

`--without-tor` dials the public ring and the PIR server in clearnet. Your IP is visible to whoever you contact.

Omit `--pir-url` and every read goes to the public ring, still over Tor and still checked by Helios unless you also skip the light client. Providers see the addresses and contracts in those calls.

`--without-light-client` leaves Tor and PIR as they are and stops verifying public reads. The ring still rotates providers. Answers are whatever the chosen endpoint returns, including history older than twelve days.

Add your own execution endpoints with a repeatable flag. They join the same ring:

```bash
cargo run --release -- \
  --extra-rpc-url "https://your-mainnet-rpc.example"
```

## Flags

| Flag | Env | Default |
|------|-----|---------|
| `--listen` | | `127.0.0.1:8545` |
| `--pir-url` | `PIR_URL` | unset. Account server, explicit port. Tokens are port+1, names are port+2. |
| `--extra-rpc-url` | | none. Repeat to append execution endpoints to the ring. |
| `--pir-pool-size` | `PIR_POOL_SIZE` | `2` |
| `--without-tor` | | off (Tor on) |
| `--without-light-client` | | off (Helios on) |

## Logs

```text
eth_getBalance  pir  312ms
eth_getLogs  helios
eth_call  local  1ms  stealthMetaAddressOf  0x6538E6bf4B0eBd30A8Ea093027Ac2422ce5d6538
```

`pir` is an account, token, or names lookup. `helios` is a public-ring read the light client checked. `ring` is the same kind of read with `--without-light-client`, unverified. `local` was answered from data already in the process (a `computeId`, or a stealth record taken from the names sidecar) and did not leave the machine.

The crates this binary links are vendored under `vendor/`. `vendor/UPSTREAM.txt` records the upstream commits.
