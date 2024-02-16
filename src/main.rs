#![feature(try_blocks)]

mod ab_buffer;
mod binance;
mod record;

use ab_buffer::ABBuffer;
use clap::Parser;
use parquet::{file::writer::SerializedFileWriter, record::RecordWriter};
use std::{error::Error, fs::File, path::Path, sync::Arc, thread};

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

#[derive(PartialEq, Eq)]
enum Signal {
    Save,
    Seal,
    Terminate,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let api = Arc::new(binance::Binance::connect().await?);
    let parquet_type = TradeStreamRecord::parquet_type();
    let directory = Path::new(&cli.directory);
    let buffer: Arc<ABBuffer<_>> = Arc::new(ABBuffer::new());

    let (tx, rx) = std::sync::mpsc::channel();

    // Save timer
    thread::spawn({
        let tx = tx.clone();
        move || loop {
            thread::sleep(cli.save_interval);
            tx.send(Signal::Save).unwrap();
        }
    });

    // Seal timer
    thread::spawn({
        let tx = tx.clone();
        move || loop {
            thread::sleep(cli.seal_interval);
            tx.send(Signal::Seal).unwrap();
        }
    });

    // Terminate signal
    ctrlc::set_handler({
        let tx = tx.clone();
        move || {
            println!("Terminate requested");
            tx.send(Signal::Terminate).unwrap();
        }
    })?;

    // Subscriber
    tokio::spawn({
        let tx = tx.clone();
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
                tx.send(Signal::Terminate).unwrap();
            }
        }
    });

    // Record listener
    tokio::spawn({
        let tx = tx.clone();
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
                        event_time: message.data.event_time.naive_utc(),
                        symbol: message.data.symbol,
                        trade_id: message.data.trade_id,
                        price: message.data.price.to_string(),
                        quantity: message.data.quantity.to_string(),
                        buyer_order_id: message.data.buyer_order_id,
                        seller_order_id: message.data.seller_order_id,
                        trade_time: message.data.trade_time.naive_utc(),
                        buyer_maker: message.data.buyer_maker,
                        ignore: message.data.ignore,
                    };
                    buffer.mutate().push(record);
                }
            };
            if let Err(err) = result {
                eprintln!("Error: {}", err);
            }
            tx.send(Signal::Terminate).unwrap();
        }
    });

    // Batch writer
    let mut terminated = false;
    while !terminated {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let path = directory.join(format!("{timestamp}.parquet"));
        let file = File::create(&path)?;
        let mut writer =
            SerializedFileWriter::new(&file, parquet_type.clone(), Default::default())?;

        for signal in &rx {
            if signal == Signal::Terminate {
                terminated = true;
            }

            if signal == Signal::Save || signal == Signal::Seal || signal == Signal::Terminate {
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

            if signal == Signal::Seal || signal == Signal::Terminate {
                break;
            }
        }

        println!("Sealing Parquet file");
        writer.close()?;
    }

    println!("Bye.");
    return Ok(());
}
