#![feature(try_blocks)]

mod ab_buffer;
mod binance;

use ab_buffer::ABBuffer;
use binance::TradeStreamRecord;
use clap::{Args, Parser};
use parquet::{file::writer::SerializedFileWriter, record::RecordWriter};
use std::{error::Error, fs::File, path::Path, sync::Arc, thread};

#[macro_use]
extern crate parquet_derive;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    binance: BinanceApiCredentials,

    #[arg(long)]
    symbol: Vec<String>,

    #[arg(long)]
    directory: String,

    #[arg(long = "save-interval", value_parser = humantime::parse_duration, default_value = "30s")]
    save_interval: std::time::Duration,

    #[arg(long = "seal-interval", value_parser = humantime::parse_duration, default_value = "30m")]
    seal_interval: std::time::Duration,
}

#[derive(Args, Debug)]
struct BinanceApiCredentials {
    #[arg(long, env = "BINANCE_API_KEY")]
    api_key: Option<String>,

    #[arg(long, env = "BINANCE_API_SECRET")]
    api_secret: Option<String>,
}

#[derive(PartialEq, Eq)]
enum Signal {
    Save,
    Seal,
    Terminate,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut api = binance::BinanceApi::new(cli.binance.api_key, cli.binance.api_secret);
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

    // Record listener
    // TODO: `api` cannot be closed while listening, maybe better to switch using async
    thread::spawn({
        let tx = tx.clone();
        let buffer = buffer.clone();
        move || {
            let result: Result<(), Box<dyn Error>> = try {
                api.connect(&cli.symbol[..])?;
                for record in api.iter() {
                    let record = record?;
                    buffer.mutate().push(record);
                }
            };
            tx.send(Signal::Terminate).unwrap();
            if let Err(err) = result {
                eprintln!("Error: {}", err);
            }
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
