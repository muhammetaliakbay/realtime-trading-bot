use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};

use rust_decimal::serde::str;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Serialize};
use tokio::sync::Mutex;

type SSLWebSocketStream = WebSocketStream<
    async_tungstenite::stream::Stream<
        async_tungstenite::tokio::TokioAdapter<tokio::net::TcpStream>,
        async_tungstenite::tokio::TokioAdapter<
            std::pin::Pin<Box<tokio_openssl::SslStream<tokio::net::TcpStream>>>,
        >,
    >,
>;

pub struct Bybit {
    tx: Mutex<(u64, SplitSink<SSLWebSocketStream, Message>)>,
    rx: Mutex<SplitStream<SSLWebSocketStream>>,
}

#[derive(Deserialize, Debug)]
pub struct TradeStreamMessage {
    #[serde(rename = "topic")]
    pub topic: String,

    #[serde(rename = "ts", with = "ts_milliseconds")]
    pub ts: DateTime<Utc>,

    #[serde(rename = "type")]
    pub typ: TradeStreamMessageType,

    #[serde(rename = "data")]
    pub data: Vec<TradeStreamEvent>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum TradeStreamEventType {
    #[serde(rename = "trade")]
    Trade,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum TakerSide {
    #[serde(rename = "Buy")]
    Buy,

    #[serde(rename = "Sell")]
    Sell,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum TradeStreamMessageType {
    #[serde(rename = "snapshot")]
    Snapshot,
}

pub fn deserialize_u64_string<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    s.parse().map_err(de::Error::custom)
}

#[derive(Deserialize, Debug)]
pub struct TradeStreamEvent {
    #[serde(rename = "i", deserialize_with = "deserialize_u64_string")]
    pub trade_id: u64,

    #[serde(rename = "T", with = "ts_milliseconds")]
    pub trade_time: DateTime<Utc>,

    #[serde(rename = "p")]
    pub price: Decimal,

    #[serde(rename = "v")]
    pub quantity: Decimal,

    #[serde(rename = "S")]
    pub taker_side: TakerSide,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "BT")]
    pub block_trade: bool,
}

#[derive(Deserialize, Debug)]
pub struct SubscribeResultMessage {
    #[serde(rename = "success")]
    pub success: bool,

    #[serde(rename = "ret_msg")]
    pub ret_msg: SubscribeRequestOpType,

    #[serde(rename = "conn_id")]
    pub conn_id: String,

    #[serde(rename = "req_id")]
    pub req_id: String,

    #[serde(rename = "op")]
    pub op: SubscribeRequestOpType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SubscribeRequestOpType {
    #[serde(rename = "subscribe")]
    Subscribe,
}

#[derive(Serialize, Debug)]
struct SubscribeRequestMessage {
    #[serde(rename = "op")]
    op: SubscribeRequestOpType,

    #[serde(rename = "req_id")]
    req_id: String,

    #[serde(rename = "args")]
    args: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BybitMessage {
    Event(TradeStreamMessage),
    Result(SubscribeResultMessage),
}

impl Bybit {
    pub async fn connect() -> Result<Self, Box<dyn std::error::Error>> {
        let (stream, _) = connect_async("wss://stream.bybit.com/v5/public/spot").await?;
        let (tx, rx) = stream.split();

        Ok(Bybit {
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
        let args = symbols
            .into_iter()
            .map(|symbol| format!("publicTrade.{}", symbol.to_uppercase()))
            .collect();
        let text = serde_json::to_string(&SubscribeRequestMessage {
            op: SubscribeRequestOpType::Subscribe,
            req_id: id.to_string(),
            args,
        })?;
        tx.1.send(Message::Text(text)).await?;
        Ok(())
    }

    pub async fn read_message(
        &self,
    ) -> Result<Option<BybitMessage>, Box<dyn std::error::Error + Send + Sync>> {
        let mut rx = self.rx.lock().await;
        loop {
            let message = match rx.next().await {
                None => return Ok(None),
                Some(message) => message,
            }?;
            match message {
                Message::Text(text) => {
                    let message = serde_json::from_str::<BybitMessage>(&text)
                        .map_err(|err| format!("Failed to parse message, {}: {}", err, text))?;
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
