mod clearnet_pir;
mod server;
mod tor_pir_client;

use std::sync::Arc;

use clap::Parser;
use clearnet_pir::{ClearnetPirLookup, KeyMode as ClearnetKeyMode};
use kohaku_pir_rpc::{PirRouter, PirTransport};
use kohaku_privacy_rpc::{CachingAsyncLookup, PrivacyBuilder};
use server::Backend;
use tor_pir_client::{DEFAULT_POOL_SIZE, KeyMode as TorKeyMode, TorPirClientLookup};
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

    /// inspire-gpu-serving account PIR HTTP base URL (e.g. :18090).
    #[arg(long, env = "PIR_URL")]
    pir_url: String,

    /// inspire-gpu-serving token/storage PIR HTTP base URL (e.g. :18091).
    /// When unset, ERC-20 balanceOf for the four default tokens falls back to RPC.
    #[arg(long, env = "TOKEN_PIR_URL")]
    token_pir_url: Option<String>,

    /// Fallback Ethereum JSON-RPC URL.
    #[arg(long, env = "ETH_RPC_URL")]
    rpc_url: String,

    /// Concurrent PIR client pool size (per PIR server).
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
        token_pir = args.token_pir_url.as_deref().unwrap_or("(none)"),
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
    let accounts = match ClearnetPirLookup::connect_with_mode(
        &args.pir_url,
        args.pir_pool_size,
        ClearnetKeyMode::Account,
    ) {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "account PirClient::connect failed");
            std::process::exit(1);
        }
    };
    info!(pool = accounts.pool_size(), "clearnet account PIR pool ready");
    let accounts: Arc<dyn kohaku_pir_rpc::LookupBackend> = accounts;

    let tokens = args.token_pir_url.as_ref().and_then(|url| {
        if url.trim().is_empty() {
            return None;
        }
        match ClearnetPirLookup::connect_with_mode(url, args.pir_pool_size, ClearnetKeyMode::Storage)
        {
            Ok(l) => {
                info!(pool = l.pool_size(), token_pir = %url, "clearnet token PIR pool ready");
                Some(l as Arc<dyn kohaku_pir_rpc::LookupBackend>)
            }
            Err(e) => {
                error!(error = %e, "token PirClient::connect failed");
                std::process::exit(1);
            }
        }
    });

    let router = match PirRouter::with_rpc_dual(accounts, tokens, &args.rpc_url, Vec::new()) {
        Ok(r) => Arc::new(r),
        Err(e) => {
            error!(error = %e, "PirRouter::with_rpc_dual failed");
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
        "connecting account PIR over Tor"
    );
    let accounts = match TorPirClientLookup::connect_with_mode(
        tor.clone(),
        &args.pir_url,
        args.pir_pool_size,
        TorKeyMode::Account,
    )
    .await
    {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "account PIR manifest over Tor failed");
            std::process::exit(1);
        }
    };
    info!(pool = accounts.pool_size(), "Tor account PIR pool ready");
    let accounts = CachingAsyncLookup::with_default_ttl(accounts);

    let tokens = if let Some(url) = args.token_pir_url.as_ref().filter(|u| !u.trim().is_empty()) {
        info!(token_pir = %url, pool = args.pir_pool_size, "connecting token PIR over Tor");
        match TorPirClientLookup::connect_with_mode(
            tor.clone(),
            url,
            args.pir_pool_size,
            TorKeyMode::Storage,
        )
        .await
        {
            Ok(l) => {
                info!(pool = l.pool_size(), "Tor token PIR pool ready");
                Some(CachingAsyncLookup::with_default_ttl(l)
                    as Arc<dyn kohaku_privacy_rpc::AsyncLookupBackend>)
            }
            Err(e) => {
                error!(error = %e, "token PIR manifest over Tor failed");
                std::process::exit(1);
            }
        }
    } else {
        info!("TOKEN_PIR_URL unset; USDC/USDT/DAI/WETH balanceOf will use fallback RPC");
        None
    };

    let transport = match PrivacyBuilder::new(&args.rpc_url) {
        Ok(builder) => builder
            .tor(tor)
            .pir_lookup(accounts, tokens, Vec::new())
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
