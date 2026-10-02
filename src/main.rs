mod lookup;
mod server;

use std::sync::Arc;

use clap::Parser;
use kohaku_pir_provider::PirRouter;
use lookup::PirLookup;
use pir_client::PirClient;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "local-pir-rpc", about = "localhost JSON-RPC facade over kohaku-pir-provider")]
struct Args {
    /// Bind address for the Ethereum JSON-RPC HTTP server.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: String,

    /// inspire-gpu-serving PIR HTTP base URL.
    #[arg(long, env = "PIR_URL")]
    pir_url: String,

    /// Fallback Ethereum JSON-RPC URL (mainnet).
    #[arg(long, env = "ETH_RPC_URL")]
    rpc_url: String,

    /// PIR clients to keep, so this many lookups can run at the same time.
    #[arg(long, default_value_t = 4)]
    pir_clients: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if args.pir_url.trim().is_empty() {
        error!("--pir-url / PIR_URL is required");
        std::process::exit(1);
    }
    if args.rpc_url.trim().is_empty() {
        error!("--rpc-url / ETH_RPC_URL is required");
        std::process::exit(1);
    }

    info!(pir = %args.pir_url, clients = args.pir_clients, "connecting PirClient");
    let mut clients = Vec::new();
    for _ in 0..args.pir_clients.max(1) {
        match PirClient::connect(&args.pir_url) {
            Ok(c) => clients.push(c),
            Err(e) => {
                error!(error = %e, "PirClient::connect failed");
                std::process::exit(1);
            }
        }
    }
    info!(
        key_size = clients[0].manifest.cuckoo.key_size,
        value_size = clients[0].manifest.cuckoo.value_size,
        "PIR manifest loaded"
    );

    let lookup = Arc::new(PirLookup::new(clients));
    let router = match PirRouter::with_rpc(lookup, &args.rpc_url, Vec::new()) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            error!(error = %e, "PirRouter::with_rpc failed");
            std::process::exit(1);
        }
    };

    let app = axum::Router::new()
        .route("/", axum::routing::post(server::handle_rpc))
        .with_state(router);

    let listener = match tokio::net::TcpListener::bind(&args.listen).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, listen = %args.listen, "bind failed");
            std::process::exit(1);
        }
    };
    info!(
        listen = %args.listen,
        fallback = %args.rpc_url,
        "local-pir-rpc listening"
    );

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        error!(error = %e, "server exited with error");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
}
