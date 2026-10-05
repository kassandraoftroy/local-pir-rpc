mod server;
mod tor_pir_client;

use std::sync::Arc;

use clap::Parser;
use kohaku_privacy_rpc::PrivacyBuilder;
use tor_pir_client::TorPirClientLookup;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "local-pir-rpc",
    about = "localhost JSON-RPC facade over kohaku-privacy-rpc (Tor + PIR)"
)]
struct Args {
    /// Bind address for the Ethereum JSON-RPC HTTP server.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: String,

    /// inspire-gpu-serving PIR HTTP base URL (reached over Tor).
    #[arg(long, env = "PIR_URL")]
    pir_url: String,

    /// Fallback Ethereum JSON-RPC URL (reached over Tor).
    #[arg(long, env = "ETH_RPC_URL")]
    rpc_url: String,
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

    info!("bootstrapping Tor (Arti)…");
    let tor = match kohaku_tor_rpc::TorRpc::connect().await {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "Tor bootstrap failed");
            std::process::exit(1);
        }
    };
    info!("Tor ready");

    info!(pir = %args.pir_url, "connecting PIR over Tor");
    let lookup = match TorPirClientLookup::connect(tor.clone(), &args.pir_url).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "PIR manifest over Tor failed");
            std::process::exit(1);
        }
    };

    let transport = match PrivacyBuilder::new(&args.rpc_url) {
        Ok(builder) => builder
            .tor(tor)
            .pir_lookup(lookup, Vec::new())
            .build_transport(),
        Err(e) => Err(e),
    };
    let transport = match transport {
        Ok(t) => Arc::new(t),
        Err(e) => {
            error!(error = %e, "PrivacyBuilder failed");
            std::process::exit(1);
        }
    };

    let app = axum::Router::new()
        .route("/", axum::routing::post(server::handle_rpc))
        .with_state(transport);

    let listener = match tokio::net::TcpListener::bind(&args.listen).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, listen = %args.listen, "bind failed");
            std::process::exit(1);
        }
    };
    info!(
        listen = %args.listen,
        pir = %args.pir_url,
        fallback = %args.rpc_url,
        "local-pir-rpc listening (all egress via Tor)"
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
