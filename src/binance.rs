use std::iter;
use std::net::TcpStream;
use std::sync::Arc;

use binance_spot_connector_rust::tungstenite::WebSocketState;
use binance_spot_connector_rust::{
    market_stream::trade::TradeStream, tungstenite::BinanceWebSocketClient,
};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use parquet::basic::LogicalType;
use parquet::schema::types::Type;
use parquet_derive::ParquetRecordWriter;
use rust_decimal::serde::str;
use rust_decimal::Decimal;
use serde::Deserialize;
use tungstenite::stream::MaybeTlsStream;

pub struct BinanceApi {
    api_key: Option<String>,
    api_secret: Option<String>,
    socket: Option<WebSocketState<MaybeTlsStream<TcpStream>>>,
}

#[derive(Debug, ParquetRecordWriter)]
pub struct TradeStreamRecord {
    event_time: NaiveDateTime,
    symbol: String,
    trade_id: u64,
    price: String,
    quantity: String,
    buyer_order_id: u64,
    seller_order_id: u64,
    trade_time: NaiveDateTime,
    buyer_maker: bool,
    ignore: bool,
}

lazy_static! {
    static ref TRADE_STREAM_RECORD_TYPE: Arc<Type> = {
        Arc::new(
            Type::group_type_builder("TradeStreamRecord")
                .with_fields(vec![
                    Arc::new(
                        Type::primitive_type_builder("event_time", parquet::basic::Type::INT64)
                            .with_logical_type(Some(LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit: parquet::basic::TimeUnit::MILLIS(
                                    parquet::format::MilliSeconds {},
                                ),
                            }))
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("symbol", parquet::basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(parquet::basic::LogicalType::String))
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("trade_id", parquet::basic::Type::INT64)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("price", parquet::basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(parquet::basic::LogicalType::String))
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("quantity", parquet::basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(parquet::basic::LogicalType::String))
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("buyer_order_id", parquet::basic::Type::INT64)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder(
                            "seller_order_id",
                            parquet::basic::Type::INT64,
                        )
                        .with_repetition(parquet::basic::Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("trade_time", parquet::basic::Type::INT64)
                            .with_logical_type(Some(LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit: parquet::basic::TimeUnit::MILLIS(
                                    parquet::format::MilliSeconds {},
                                ),
                            }))
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("buyer_maker", parquet::basic::Type::BOOLEAN)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("ignore", parquet::basic::Type::BOOLEAN)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        )
    };
}

impl TradeStreamRecord {
    pub fn parquet_type() -> Arc<Type> {
        TRADE_STREAM_RECORD_TYPE.clone()
    }
}

#[derive(Deserialize, Debug)]
struct TradeStreamMessage {
    #[serde(rename = "stream")]
    stream: String,

    #[serde(rename = "data")]
    data: TradeStreamEvent,
}

#[derive(Deserialize, Debug, PartialEq)]
enum TradeStreamEventType {
    #[serde(rename = "trade")]
    Trade,
}

#[derive(Deserialize, Debug)]
struct TradeStreamEvent {
    #[serde(rename = "e")]
    event_type: TradeStreamEventType,

    #[serde(rename = "E", with = "ts_milliseconds")]
    event_time: DateTime<Utc>,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "t")]
    trade_id: u64,

    #[serde(rename = "p")]
    price: Decimal,

    #[serde(rename = "q")]
    quantity: Decimal,

    #[serde(rename = "b")]
    buyer_order_id: u64,

    #[serde(rename = "a")]
    seller_order_id: u64,

    #[serde(rename = "T", with = "ts_milliseconds")]
    trade_time: DateTime<Utc>,

    #[serde(rename = "m")]
    buyer_maker: bool,

    #[serde(rename = "M")]
    ignore: bool,
}

impl BinanceApi {
    pub fn new(api_key: Option<String>, api_secret: Option<String>) -> BinanceApi {
        BinanceApi {
            api_key,
            api_secret,
            socket: None,
        }
    }

    pub fn connect(&mut self, symbols: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        let mut socket = BinanceWebSocketClient::connect()?;
        let streams = symbols
            .into_iter()
            .map(|symbol| TradeStream::new(symbol).into())
            .collect::<Vec<_>>();
        socket.subscribe(&streams);
        self.socket = Some(socket);
        Ok(())
    }

    pub fn read(&mut self) -> Result<Option<TradeStreamRecord>, Box<dyn std::error::Error>> {
        loop {
            let message = self.socket.as_mut().unwrap().as_mut().read()?;
            let message = message.into_text()?;
            let message = serde_json::from_str::<TradeStreamMessage>(&message);
            let mut message = match message {
                Ok(message) => message,
                Err(_) => {
                    continue;
                }
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
            return Ok(Some(record));
        }
    }

    pub fn iter(
        &mut self,
    ) -> impl Iterator<Item = Result<TradeStreamRecord, Box<dyn std::error::Error>>> + '_ {
        iter::from_fn(move || match self.read() {
            Ok(None) => None,
            Ok(Some(record)) => Some(Ok(record)),
            Err(err) => Some(Err(err)),
        })
    }
}
