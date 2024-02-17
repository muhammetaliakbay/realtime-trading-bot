#![feature(try_blocks)]

mod ab_buffer;
mod binance;
mod record;

use ab_buffer::ABBuffer;
use clap::Parser;
use parquet::{file::writer::SerializedFileWriter, record::RecordWriter};
use std::{error::Error, fs::File, path::Path, sync::Arc};
use tokio::{signal, sync::mpsc, time};

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
    // Signal handlers
    let sigctrlc = signal::ctrl_c();
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();

    // Setup
    let cli = Cli::parse();

    let api = Arc::new(binance::Binance::connect().await?);
    let parquet_type = TradeStreamRecord::parquet_type();
    let directory = Path::new(&cli.directory);
    let buffer: Arc<ABBuffer<_>> = Arc::new(ABBuffer::new());

    // Save timer
    let mut save_interval = time::interval(cli.save_interval);

    // Seal timer
    let mut seal_interval = time::interval(cli.seal_interval);

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
                eprintln!("Error: {}", err);
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
            let result: Result<(), Box<dyn Error>> = try {
                while let Some(message) = api.read_message().await? {
                    let mut message = match message {
                        binance::BinanceMessage::Event(message) => message,
                        _ => continue,
                    };
                    if message.data.event_type != TradeStreamEventType::Trade {
                        continue;
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
                    buffer.mutate().push(record);
                }
            };
            if let Err(err) = result {
                eprintln!("Error: {}", err);
            }
        }
    });

    // Terminate signal
    let (terminate_signal_tx, mut terminate_signal) = mpsc::channel(1);
    tokio::spawn({
        async move {
            tokio::select! {
                _ = sigctrlc => (), // If received Ctrl-C, terminate
                _ = sigterm.recv() => (), // If received SIGTERM, terminate
                Err(_) = subscriber => (), // If subscriber fails, terminate; if succeeds, ignore
                _ = listener => (), // If listener fails or succeeds, terminate
            }
            terminate_signal_tx.send(()).await.unwrap();
        }
    });

    // Batch writer
    let mut terminate = false;
    while !terminate {
        // Create new Parquet file
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let path = directory.join(format!("{timestamp}.parquet"));
        let file = File::create(&path)?;
        let mut writer =
            SerializedFileWriter::new(&file, parquet_type.clone(), Default::default())?;

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
            let mut buffer = buffer.swap();
            if !buffer.is_empty() {
                println!("Saving {} trades", buffer.len());
                let mut row_group_writer = writer.next_row_group().unwrap();
                (&buffer[..])
                    .write_to_row_group(&mut row_group_writer)
                    .unwrap();
                row_group_writer.close().unwrap();
                buffer.clear();
            }
        }

        // Seal the file
        println!("Sealing Parquet file");
        writer.close().unwrap();
    }

    println!("Bye.");
    return Ok(());
}
