mod clearnet_pir;
mod server;
mod tor_pir_client;

use std::sync::Arc;

use clap::Parser;
use clearnet_pir::ClearnetPirLookup;
use kohaku_pir_rpc::{PirRouter, PirTransport};
use kohaku_privacy_rpc::PrivacyBuilder;
use server::Backend;
use tor_pir_client::{DEFAULT_POOL_SIZE, TorPirClientLookup};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "local-pir-rpc",
    about = "localhost JSON-RPC facade: Tor+PIR (default) or clearnet pir-rpc"
)]
struct Args {
    /// Bind address for the Ethereum JSON-RPC HTTP server.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: String,

    /// inspire-gpu-serving PIR HTTP base URL.
    #[arg(long, env = "PIR_URL")]
    pir_url: String,

    /// Fallback Ethereum JSON-RPC URL.
    #[arg(long, env = "ETH_RPC_URL")]
    rpc_url: String,

    /// Concurrent PIR client pool size.
    #[arg(long, env = "PIR_POOL_SIZE", default_value_t = DEFAULT_POOL_SIZE)]
    pir_pool_size: usize,

    /// Disable Tor: PIR and fallback RPC go clearnet via `kohaku-pir-rpc` only.
    /// Default (flag unset): all egress via Tor (`kohaku-privacy-rpc`).
    #[arg(long, default_value_t = false)]
    without_tor: bool,
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

    let backend = if args.without_tor {
        build_clearnet(&args)
    } else {
        build_tor(&args).await
    };

    let app = axum::Router::new()
        .route("/", axum::routing::post(server::handle_rpc))
        .with_state(backend);

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
        without_tor = args.without_tor,
        pir_pool_size = args.pir_pool_size,
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

fn build_clearnet(args: &Args) -> Arc<Backend> {
    info!(
        pir = %args.pir_url,
        pool = args.pir_pool_size,
        "clearnet mode: connecting PirClient pool (no Tor)"
    );
    let lookup = match ClearnetPirLookup::connect(&args.pir_url, args.pir_pool_size) {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "PirClient::connect failed");
            std::process::exit(1);
        }
    };
    info!(pool = lookup.pool_size(), "clearnet PIR pool ready");
    let router = match PirRouter::with_rpc(lookup, &args.rpc_url, Vec::new()) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            error!(error = %e, "PirRouter::with_rpc failed");
            std::process::exit(1);
        }
    };
    Arc::new(Backend::Clearnet(PirTransport::new(router)))
}

async fn build_tor(args: &Args) -> Arc<Backend> {
    info!("bootstrapping Tor (Arti)…");
    let tor = match kohaku_tor_rpc::TorRpc::connect().await {
        Ok(t) => t,
        Err(e) => {
            error!(error = %e, "Tor bootstrap failed");
            std::process::exit(1);
        }
    };
    info!("Tor ready");

    info!(
        pir = %args.pir_url,
        pool = args.pir_pool_size,
        "connecting PIR over Tor"
    );
    let lookup =
        match TorPirClientLookup::connect(tor.clone(), &args.pir_url, args.pir_pool_size).await {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "PIR manifest over Tor failed");
                std::process::exit(1);
            }
        };
    info!(pool = lookup.pool_size(), "Tor PIR pool ready");

    let transport = match PrivacyBuilder::new(&args.rpc_url) {
        Ok(builder) => builder
            .tor(tor)
            .pir_lookup(lookup, Vec::new())
            .build_transport(),
        Err(e) => Err(e),
    };
    match transport {
        Ok(t) => Arc::new(Backend::Tor(t)),
        Err(e) => {
            error!(error = %e, "PrivacyBuilder failed");
            std::process::exit(1);
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
}
