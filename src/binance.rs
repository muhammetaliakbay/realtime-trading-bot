use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};

use rust_decimal::serde::str;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

type SSLWebSocketStream = WebSocketStream<
    async_tungstenite::stream::Stream<
        async_tungstenite::tokio::TokioAdapter<tokio::net::TcpStream>,
        async_tungstenite::tokio::TokioAdapter<
            std::pin::Pin<Box<tokio_openssl::SslStream<tokio::net::TcpStream>>>,
        >,
    >,
>;

pub struct Binance {
    tx: Mutex<(u64, SplitSink<SSLWebSocketStream, Message>)>,
    rx: Mutex<SplitStream<SSLWebSocketStream>>,
}

#[derive(Deserialize, Debug)]
pub struct TradeStreamMessage {
    #[serde(rename = "stream")]
    pub stream: String,

    #[serde(rename = "data")]
    pub data: TradeStreamEvent,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum TradeStreamEventType {
    #[serde(rename = "trade")]
    Trade,
}

#[derive(Deserialize, Debug)]
pub struct TradeStreamEvent {
    #[serde(rename = "e")]
    pub event_type: TradeStreamEventType,

    #[serde(rename = "E", with = "ts_milliseconds")]
    pub event_time: DateTime<Utc>,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "t")]
    pub trade_id: u64,

    #[serde(rename = "p")]
    pub price: Decimal,

    #[serde(rename = "q")]
    pub quantity: Decimal,

    #[serde(rename = "b")]
    pub buyer_order_id: u64,

    #[serde(rename = "a")]
    pub seller_order_id: u64,

    #[serde(rename = "T", with = "ts_milliseconds")]
    pub trade_time: DateTime<Utc>,

    #[serde(rename = "m")]
    pub buyer_maker: bool,

    #[serde(rename = "M")]
    pub ignore: bool,
}

#[derive(Deserialize, Debug)]
pub struct ResultMessage {
    #[serde(rename = "id")]
    pub id: u64,

    #[serde(rename = "result")]
    pub result: Value,
}

#[derive(Serialize, Debug)]
enum SubscribeRequestMethodType {
    #[serde(rename = "SUBSCRIBE")]
    Subscribe,
}

#[derive(Serialize, Debug)]
struct SubscribeRequestMessage {
    #[serde(rename = "method")]
    method: SubscribeRequestMethodType,

    #[serde(rename = "id")]
    id: u64,

    #[serde(rename = "params")]
    params: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BinanceMessage {
    Event(TradeStreamMessage),
    Result(ResultMessage),
}

impl Binance {
    pub async fn connect() -> Result<Self, Box<dyn std::error::Error>> {
        let (stream, _) = connect_async("wss://stream.binance.com/stream").await?;
        let (tx, rx) = stream.split();

        Ok(Binance {
            tx: Mutex::new((0, tx)),
            rx: Mutex::new(rx),
        })
    }

    pub async fn subscribe_trade_stream(
        &self,
        symbols: impl IntoIterator<Item = &String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut tx = self.tx.lock().await;
        let id = tx.0;
        tx.0 += 1;
        let params = symbols
            .into_iter()
            .map(|symbol| format!("{}@trade", symbol.to_lowercase()))
            .collect();
        let text = serde_json::to_string(&SubscribeRequestMessage {
            method: SubscribeRequestMethodType::Subscribe,
            id,
            params: params,
        })?;
        tx.1.send(Message::Text(text)).await?;
        Ok(())
    }

    pub async fn read_message(
        &self,
    ) -> Result<Option<BinanceMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut rx = self.rx.lock().await;
        loop {
            let message = match rx.next().await {
                None => return Ok(None),
                Some(message) => message,
            }?;
            match message {
                Message::Text(text) => {
                    let message = serde_json::from_str::<BinanceMessage>(&text)?;
                    return Ok(Some(message));
                }
                Message::Ping(binary) => {
                    let pong = Message::Pong(binary);
                    self.tx.lock().await.1.send(pong).await?;
                    continue;
                }
                Message::Close(_) => {
                    return Ok(None);
                }
                _ => {
                    return Err(format!("Unexpected message type: {:?}", message).into());
                }
            }
        }
    }
}
