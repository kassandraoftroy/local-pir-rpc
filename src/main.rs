mod clearnet_pir;
mod ports;
mod server;
mod stealth;
mod tor_pir_client;

use std::sync::Arc;

use clap::Parser;
use clearnet_pir::{ClearnetPirLookup, KeyMode as ClearnetKeyMode};
use kohaku_privacy_rpc::{
    default_data_dir, sync_pir_lookup, CachingAsyncLookup, PrivacyBuilder, RpcOutbound,
};
use server::Backend;
use tor_pir_client::{KeyMode as TorKeyMode, TorPirClientLookup, DEFAULT_POOL_SIZE};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "local-pir-rpc",
    about = "localhost Ethereum JSON-RPC verified by a Helios light client"
)]
struct Args {
    /// Bind address for the Ethereum JSON-RPC HTTP server.
    #[arg(long, default_value = "127.0.0.1:8545")]
    listen: String,

    /// Account PIR HTTP base URL, with an explicit port (e.g. `http://host:18090`).
    /// Tokens are the same URL at port+1, primary names at port+2.
    /// Omit to answer every call through Helios.
    #[arg(long, env = "PIR_URL")]
    pir_url: Option<String>,

    /// Extra execution RPC URL appended to the baked-in public ring. Repeat the flag to add more.
    #[arg(long = "extra-rpc-url", action = clap::ArgAction::Append)]
    extra_rpc_urls: Vec<String>,

    /// Concurrent PIR client pool size (per PIR server).
    #[arg(long, env = "PIR_POOL_SIZE", default_value_t = DEFAULT_POOL_SIZE)]
    pir_pool_size: usize,

    /// Dial the public RPC ring in clearnet. Default: the ring is reached through Tor.
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
    let pir_urls = match args
        .pir_url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(url) => match ports::derive_pir_urls(url) {
            Ok(urls) => Some(urls),
            Err(e) => {
                error!(error = %e, "PIR URL");
                std::process::exit(1);
            }
        },
        None => None,
    };

    let backend = build(&args, pir_urls.as_ref()).await;
    let oldest = backend
        .transport()
        .verified()
        .map(kohaku_privacy_rpc::VerifiedRpc::oldest_block);

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
        oldest_block = oldest,
        pir = pir_urls.is_some(),
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

async fn build(args: &Args, urls: Option<&ports::PirUrls>) -> Arc<Backend> {
    info!("starting Helios light client (about twelve days of history)");
    let mut builder = PrivacyBuilder::new().extra_rpc_urls(args.extra_rpc_urls.clone());
    let outbound = if args.without_tor {
        builder = builder.without_tor();
        if let Some(urls) = urls {
            builder = attach_clearnet_pir(builder, args, urls);
        }
        RpcOutbound::Clearnet
    } else {
        info!("bootstrapping Tor (Arti)…");
        let tor = match kohaku_tor_rpc::TorRpc::connect().await {
            Ok(t) => t,
            Err(e) => {
                error!(error = %e, "Tor bootstrap failed");
                std::process::exit(1);
            }
        };
        info!("Tor ready");
        if let Some(urls) = urls {
            builder = attach_tor_pir(builder, args, urls, &tor).await;
        } else {
            builder = builder.tor(tor.clone());
        }
        RpcOutbound::tor(tor)
    };

    match builder.open_verified(outbound, default_data_dir()).await {
        Ok(builder) => match builder.build_transport() {
            Ok(transport) => Arc::new(Backend::new(transport)),
            Err(e) => {
                error!(error = %e, "PrivacyBuilder failed");
                std::process::exit(1);
            }
        },
        Err(e) => {
            error!(error = %e, "Helios failed to start");
            std::process::exit(1);
        }
    }
}

fn attach_clearnet_pir(
    builder: PrivacyBuilder,
    args: &Args,
    urls: &ports::PirUrls,
) -> PrivacyBuilder {
    info!(
        accounts = %urls.accounts,
        tokens = %urls.tokens,
        names = %urls.names,
        pool = args.pir_pool_size,
        "clearnet mode: connecting PirClient pools (no Tor)"
    );
    let accounts = connect_clearnet(&urls.accounts, args.pir_pool_size, ClearnetKeyMode::Account);
    let tokens = connect_clearnet(&urls.tokens, args.pir_pool_size, ClearnetKeyMode::Storage);
    let names = connect_clearnet(&urls.names, args.pir_pool_size, ClearnetKeyMode::Names);
    builder.pir_lookup(
        sync_pir_lookup(accounts),
        Some(sync_pir_lookup(tokens)),
        Some(sync_pir_lookup(names)),
        Vec::new(),
    )
}

async fn attach_tor_pir(
    builder: PrivacyBuilder,
    args: &Args,
    urls: &ports::PirUrls,
    tor: &kohaku_tor_rpc::TorRpc,
) -> PrivacyBuilder {
    info!(
        accounts = %urls.accounts,
        tokens = %urls.tokens,
        names = %urls.names,
        pool = args.pir_pool_size,
        "connecting PIR pools over Tor"
    );
    let accounts = connect_tor(
        tor.clone(),
        &urls.accounts,
        args.pir_pool_size,
        TorKeyMode::Account,
    )
    .await;
    let tokens = connect_tor(
        tor.clone(),
        &urls.tokens,
        args.pir_pool_size,
        TorKeyMode::Storage,
    )
    .await;
    let names = connect_tor(
        tor.clone(),
        &urls.names,
        args.pir_pool_size,
        TorKeyMode::Names,
    )
    .await;
    builder
        .tor(tor.clone())
        .pir_lookup(accounts, Some(tokens), Some(names), Vec::new())
}

fn connect_clearnet(
    url: &str,
    pool: usize,
    mode: ClearnetKeyMode,
) -> Arc<dyn kohaku_pir_rpc::LookupBackend> {
    match ClearnetPirLookup::connect_with_mode(url, pool, mode) {
        Ok(lookup) => {
            info!(pool = lookup.pool_size(), %url, ?mode, "clearnet PIR pool ready");
            lookup
        }
        Err(e) => {
            error!(error = %e, %url, ?mode, "PirClient::connect failed");
            std::process::exit(1);
        }
    }
}

async fn connect_tor(
    tor: kohaku_tor_rpc::TorRpc,
    url: &str,
    pool: usize,
    mode: TorKeyMode,
) -> Arc<dyn kohaku_privacy_rpc::AsyncLookupBackend> {
    match TorPirClientLookup::connect_with_mode(tor, url, pool, mode).await {
        Ok(lookup) => {
            info!(pool = lookup.pool_size(), %url, ?mode, "Tor PIR pool ready");
            CachingAsyncLookup::with_default_ttl(lookup)
        }
        Err(e) => {
            error!(error = %e, %url, ?mode, "PIR manifest over Tor failed");
            std::process::exit(1);
        }
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
}
