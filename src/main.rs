#![feature(try_blocks)]

mod ab_buffer;
mod bybit;
mod observability;
mod entities;
mod config;
mod recording;

use clap::Parser;
use std::{sync::Arc, time::Duration};
use tokio::{select, signal, spawn};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    symbol: Vec<String>,

    #[arg(long = "loki-url", env = "LOKI_URL")]
    loki_url: Option<String>,

    #[arg(long = "prometheus-push-url", env = "PROMETHEUS_PUSH_URL")]
    prometheus_push_url: Option<String>,

    #[arg(
        long = "prometheus-push-interval",
        env = "PROMETHEUS_PUSH_INERVAL",
        value_parser = humantime::parse_duration, 
        default_value = "15s"
    )]
    prometheus_push_interval: std::time::Duration,

    #[arg(long = "postgresql", env = "POSTGRESQL")]
    postgresql: String,

    #[arg(long = "save-interval", value_parser = humantime::parse_duration, default_value = "15s")]
    save_interval: std::time::Duration,

    #[arg(long = "snapshot-interval", value_parser = humantime::parse_duration, default_value = "1s")]
    snapshot_interval: std::time::Duration,

    #[arg(long = "orderbook-level", value_enum)]
    orderbook_level: bybit::OrderbookLevel,

    #[arg(long = "request-limit", default_value = "10")]
    request_limit: usize,

    #[arg(long = "request-interval", value_parser = humantime::parse_duration, default_value = "5s")]
    request_interval: std::time::Duration,

    #[arg(long = "ping-interval", value_parser = humantime::parse_duration, default_value = "30s")]
    ping_interval: std::time::Duration,

    #[arg(long = "read-timeout", value_parser = humantime::parse_duration, default_value = "15s")]
    read_timeout: std::time::Duration,

    #[arg(long = "symbols-config-interval", value_parser = humantime::parse_duration, default_value = "30s")]
    symbols_config_interval: std::time::Duration,

    #[arg(long = "consul", env = "CONSUL")]
    consul: String,

    #[arg(long = "consul-symbols-key")]
    consul_symbols_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI
    let cli = Cli::parse();

    // Setup tracing and metrics
    let tracing_teardown = match cli.loki_url.clone() {
        Some(loki_url) => {
            println!("Setting up Loki at {}", loki_url);
            Some(observability::setup_loki(loki_url).await?)
        }
        None => {
            tracing_subscriber::fmt::init();
            None
        }
    };

    // Setup DB connection
    let (mut db, db_conn) = tokio_postgres::connect(&cli.postgresql, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = db_conn.await {
            tracing::error!("database connection lost: {}", err);
        }
    });
    embedded::migrations::runner().run_async(&mut db).await?;

    //Setup Consul
    let consul = rs_consul::Consul::new(rs_consul::Config{
        address: cli.consul.clone(),
        ..Default::default()
    });

    // Run
    let result = run(cli, db, consul).await;

    if let Err(err) = &result {
        tracing::error!(err);
    }

    // Teardown tracing
    if let Some(teardown) = tracing_teardown {
        teardown.await?;
    }

    return result;
}

async fn run(cli: Cli, db: tokio_postgres::Client, consul: rs_consul::Consul) -> Result<(), Box<dyn std::error::Error>> {
    // Shutdown signals
    let _shutdown = tokio::spawn({
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        let sigctrlc = signal::ctrl_c();
        async move {
            select! {
                _ = sigterm.recv() => Err("SIGTERM received")?,
                _ = sigctrlc => Err("Ctrl-C received")?,
            }
            unreachable!()
        }
    });

    // Subscriptions
    let api = Arc::new(bybit::Bybit::connect_timeout(Duration::from_secs(15)).await?);
    tracing::info!("Connected to bybit");
    let (mut _manager, stream_messages_rx) = {
        let api = api.clone();
        let desired_streams_iter = config::watch_symbols(consul, cli.consul_symbols_key, cli.orderbook_level, cli.symbols_config_interval, Duration::from_secs(5));
        let (stream_messages_tx, stream_messages_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            spawn(async move {
                let result = bybit::manage(api, Box::pin(desired_streams_iter), stream_messages_tx, cli.request_limit, cli.request_interval, cli.read_timeout, cli.ping_interval).await;
                if let Err(err) = result {
                    Err(format!("Error during managing streams: {}", err))?;
                }
                Err::<(), String>("Streams manager finished".to_string())
            }),
            stream_messages_rx,
        )
    };

    // Message switch
    let (mut _message_switch, trades_rx, orderbook_updates_rx) = {
        let (trades_tx, trades_rx) = tokio::sync::mpsc::unbounded_channel();
        let (orderbook_updates_tx, orderbook_updates_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            spawn(async move {
                let result = recording::stream_message_switch(stream_messages_rx, trades_tx, orderbook_updates_tx).await;
                if let Err(err) = result {
                    Err(format!("Error during listening records: {}", err))?;
                }
                Err::<(), String>("Message switch finished".to_string())
            }),
            trades_rx,
            orderbook_updates_rx,
        )
    };

    // Save trades
    let db = Arc::new(db);
    let mut _trades_recorder = {
        let db = db.clone();
        tokio::spawn(async move {
            let result = recording::record_trades(cli.save_interval, db, trades_rx).await;
            if let Err(err) = result {
                Err(format!("Error during saving trades: {}", err))?;
            }
            Err::<(), String>("Trade recorder finished".to_string())
        })
    };

    // Save orderbook snapshots
    let mut _orderbook_recorder = {
        let db = db.clone();
        tokio::spawn(async move {
            let result = recording::record_orderbook_snapshots(cli.snapshot_interval, db, orderbook_updates_rx).await;
            if let Err(err) = result {
                Err(format!("Error during saving orderbook snapshots: {}", err))?;
            }
            Err::<(), String>("Orderbook snapshot recorder finished".to_string())
        })
    };

    // Join
    let result = select!(
        result = _shutdown => result,
        result = &mut _manager => result,
        result = &mut _message_switch => result,
        result = &mut _trades_recorder => result,
        result = &mut _orderbook_recorder => result,
    );

    // Graceful shutdown
    tracing::info!("Closing ByBit connection");
    api.close().await.map_err(|err| format!("Error during closing API: {}", err))?;

    tracing::info!("Finishing manager");
    if !_manager.is_finished() {
        let _ = _manager.await;
    }

    tracing::info!("Finishing message switch");
    if !_message_switch.is_finished() {
        let _ = _message_switch.await;
    }

    tracing::info!("Finishing trades recorder");
    if !_trades_recorder.is_finished() {
        let _ = _trades_recorder.await;
    }

    tracing::info!("Finishing orderbook recorder");
    if !_orderbook_recorder.is_finished() {
        let _ = _orderbook_recorder.await;
    }

    tracing::info!("Shutdown completed");

    result??;
    unreachable!()
}
