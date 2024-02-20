#![feature(try_blocks)]

mod ab_buffer;
mod binance;
mod record;
mod observability;

use ab_buffer::ABBuffer;
use clap::Parser;
use parquet::{file::writer::SerializedFileWriter, record::RecordWriter};
use rust_decimal::prelude::ToPrimitive;
use std::{error::Error, path::{Path, PathBuf}, sync::Arc};
use tokio::{
    fs::File, signal, sync::{mpsc, OnceCell}, time
};
use lazy_static::lazy_static;

use crate::{binance::TradeStreamEventType, record::TradeStreamRecord};

#[macro_use]
extern crate parquet_derive;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    symbol: Vec<String>,

    #[arg(long)]
    directory: String,

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

    #[arg(long = "save-interval", value_parser = humantime::parse_duration, default_value = "30s")]
    save_interval: std::time::Duration,

    #[arg(long = "seal-interval", value_parser = humantime::parse_duration, default_value = "30m")]
    seal_interval: std::time::Duration,

    #[arg(long = "subscribe-interval", value_parser = humantime::parse_duration, default_value = "5s")]
    subscribe_interval: std::time::Duration,

    #[arg(long = "subscribe-chunk", default_value = "5")]
    subscribe_chunk: usize,
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

    let metrics_teardown = match cli.prometheus_push_url.clone() {
        Some(prometheus_push_url) => {
            println!("Setting up Prometheus Push at {}", prometheus_push_url);
            Some(observability::setup_prometheus_push(prometheus_push_url, cli.prometheus_push_interval).await)
        }
        None => None,
    };

    // Run

    let result = run(cli).await;

    if let Err(err) = &result {
        tracing::error!(err);
    }

    // Teardown tracing
    if let Some(teardown) = tracing_teardown {
        teardown.await?;
    }

    // Teardown metrics
    if let Some(teardown) = metrics_teardown {
        teardown.await?;
    }

    return result;
}

lazy_static! {
    static ref TRADE_COUNTER: prometheus::IntCounterVec = prometheus::register_int_counter_vec!(
        "trade_counter",
        "Trade counter.",
        &["symbol", "buyer_maker"],
    )
    .unwrap();

    static ref STORAGE_GAUGE: prometheus::IntGauge = prometheus::register_int_gauge!(
        "storage_gauge",
        "Storage gauge.",
    )
    .unwrap();

    static ref TRADE_PRICE_GAUGE: prometheus::GaugeVec = prometheus::register_gauge_vec!(
        "trade_price_gauge",
        "Trade price gauge.",
        &["symbol"],
    )
    .unwrap();

    static ref TRADE_VALUE_COUNTER: prometheus::CounterVec = prometheus::register_counter_vec!(
        "trade_value_counter",
        "Trade value counter.",
        &["symbol"],
    )
    .unwrap();
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let api = Arc::new(binance::Binance::connect().await?);
    let parquet_type = TradeStreamRecord::parquet_type();
    let directory = Path::new(&cli.directory);
    let buffer: Arc<ABBuffer<_>> = Arc::new(ABBuffer::new());

    // Signal handlers
    let sigctrlc = signal::ctrl_c();
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

    // Save timer
    let mut save_interval =
        time::interval_at(time::Instant::now() + cli.save_interval, cli.save_interval);

    // Seal timer
    let mut seal_interval =
        time::interval_at(time::Instant::now() + cli.seal_interval, cli.seal_interval);

    // Subscriber
    let subscriber = tokio::spawn({
        let api = api.clone();
        async move {
            let result: Result<(), Box<dyn Error>> = try {
                for symbols_chunk in cli.symbol.chunks(cli.subscribe_chunk) {
                    api.subscribe_trade_stream(symbols_chunk).await?;
                    tokio::time::sleep(cli.subscribe_interval).await;
                }
            };
            if let Err(err) = result {
                tracing::error!("Error during subscribing for trade streams: {}", err);
                return Err(());
            }
            Ok(())
        }
    });

    // Record listener
    let listener = tokio::spawn({
        let buffer = buffer.clone();
        let api = api.clone();
        async move {
            let result: Result<(), Box<dyn Error + Send + Sync>> = try {
                while let Some(message) = api.read_message().await? {
                    let mut message = match message {
                        binance::BinanceMessage::Event(message) => message,
                        _ => continue,
                    };

                    if message.data.event_type != TradeStreamEventType::Trade {
                        continue;
                    }
                    let buyer_maker_label = match message.data.buyer_maker {
                        true => "buyer",
                        false => "maker",
                    };
                    TRADE_COUNTER.with_label_values(&[&message.data.symbol, buyer_maker_label]).inc();
                    
                    let value = message.data.price * message.data.quantity;
                    if let Some(value) = value.to_f64() {
                        TRADE_VALUE_COUNTER.with_label_values(&[&message.data.symbol]).inc_by(value);
                    } else {
                        tracing::warn!("Invalid value ({}): {:?}", value, message);
                    }

                    if let Some(price) = message.data.price.to_f64() {
                        TRADE_PRICE_GAUGE.with_label_values(&[&message.data.symbol]).set(price);
                    } else {
                        tracing::warn!("Invalid price({}): {:?}", message.data.price, message);
                    }

                    message.data.price.normalize_assign();
                    message.data.quantity.normalize_assign();
                    let record = TradeStreamRecord {
                        trade_time: message.data.trade_time.naive_utc(),
                        symbol: message.data.symbol,
                        trade_id: message.data.trade_id,
                        price: message.data.price.to_string(),
                        quantity: message.data.quantity.to_string(),
                        buyer_maker: message.data.buyer_maker,
                    };
                    buffer.mutate().await.push(record);
                }
            };
            if let Err(err) = result {
                tracing::error!("Error during listening records: {}", err);
            }
        }
    });

    // Terminate signal
    let (terminate_signal_tx, mut terminate_signal) = mpsc::channel(1);
    tokio::spawn({
        async move {
            tokio::select! {
                _ = sigctrlc => {
                    tracing::info!("Received Ctrl-C");
                }, // If received Ctrl-C, terminate
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM");
                }, // If received SIGTERM, terminate
                Err(_) = subscriber => (), // If subscriber fails, terminate; if succeeds, ignore
                _ = listener => (), // If listener fails or succeeds, terminate
            }
            terminate_signal_tx.send(()).await.unwrap();
        }
    });

    // Batch writer
    let session_directory = OnceCell::new();
    let mut terminate = false;
    let mut sealed_storage = 0;
    while !terminate {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let session_directory = session_directory
            .get_or_try_init(|| async {
                // Create new session directory
                let directory = directory.join(timestamp.to_string());
                tokio::fs::create_dir(&directory).await?;
                Result::<PathBuf, Box<dyn std::error::Error>>::Ok(directory)
            })
            .await?;

        // Create new Parquet file
        let path = session_directory.join(format!("{timestamp}.parquet"));
        let lock_path = session_directory.join(format!("{timestamp}.parquet.lock"));
        tracing::info!("Creating parquet lock file, {}", lock_path.display());
        let file = File::create(&lock_path).await?.into_std().await;
        let mut writer =
            tokio::task::block_in_place(||SerializedFileWriter::new(&file, parquet_type.clone(), Default::default()))?;

        // Save buffers until seal signal
        let mut seal = false;
        while !seal {
            tokio::select! {
                _ = save_interval.tick() => (),

                _ = seal_interval.tick() => {
                    seal = true;
                },

                _ = terminate_signal.recv() => {
                    seal = true;
                    terminate = true;
                },
            }

            // Swap the buffers and save the old one
            let mut buffer = buffer.swap().await;
            tokio::task::block_in_place(||{
                if !buffer.is_empty() {
                    tracing::info!("Saving {} trades", buffer.len());
                    let mut row_group_writer = writer.next_row_group()?;
                    (&buffer[..]).write_to_row_group(&mut row_group_writer)?;
                    row_group_writer.close()?;
                    buffer.clear();
                }
                Ok::<(), Box<dyn Error>>(())
            })?;

            // Update metrics
            STORAGE_GAUGE.set(sealed_storage + tokio::fs::metadata(&lock_path).await?.len() as i64);
        }

        // Seal the file
        tracing::info!("Sealing parquet file, {}", path.display());
        tokio::task::block_in_place(||writer.close())?;
        tokio::fs::rename(lock_path, &path).await?;

        // Update metrics
        sealed_storage += tokio::fs::metadata(path).await?.len() as i64;
        STORAGE_GAUGE.set(sealed_storage);
    }

    tracing::info!("Bye.");

    Ok(())
}
