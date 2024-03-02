use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use async_tungstenite::tokio::connect_async;
use async_tungstenite::tungstenite::Message;
use async_tungstenite::WebSocketStream;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};

use rust_decimal::serde::str;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::{select, spawn};

const BYBIT_PUBLIC_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";

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

#[derive(Debug, Deserialize)]
pub struct DateTimeUtc(#[serde(with = "ts_milliseconds")] pub DateTime<Utc>);

#[derive(Deserialize, Debug)]
pub struct StreamMessage {
    #[serde(rename = "topic")]
    pub topic: String,

    #[serde(rename = "ts", with = "ts_milliseconds")]
    pub ts: DateTime<Utc>,

    #[serde(rename = "type")]
    pub typ: StreamMessageType,

    #[serde(rename = "data")]
    pub data: StreamMessageData,

    #[serde(rename = "cts", default)]
    pub cts: Option<DateTimeUtc>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum TakerSide {
    #[serde(rename = "Buy")]
    Buy,

    #[serde(rename = "Sell")]
    Sell,
}

#[derive(Deserialize, Debug, PartialEq)]
pub enum StreamMessageType {
    #[serde(rename = "snapshot")]
    Snapshot,

    #[serde(rename = "delta")]
    Delta,
}

#[derive(Deserialize, Debug)]
pub struct StreamMessagePublicTradeData {
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
pub struct OrderbookRow(
    pub Decimal, // Price
    pub Decimal, // Size
);

#[derive(Deserialize, Debug)]
pub struct StreamMessageOrderbookData {
    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "b")]
    pub bids: Vec<OrderbookRow>,

    #[serde(rename = "a")]
    pub asks: Vec<OrderbookRow>,

    #[serde(rename = "u")]
    pub update_id: u64,

    #[serde(rename = "seq")]
    pub cross_sequence: u64,
}

#[derive(Deserialize, Debug)]
pub struct ResultMessage {
    #[serde(rename = "success")]
    pub success: bool,

    #[serde(rename = "ret_msg")]
    pub ret_msg: String,

    #[serde(rename = "conn_id")]
    pub conn_id: String,

    #[serde(rename = "req_id")]
    pub req_id: String,

    #[serde(rename = "op")]
    pub op: RequestOp,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RequestOp {
    #[serde(rename = "subscribe")]
    Subscribe,
    #[serde(rename = "unsubscribe")]
    Unsubscribe,
    #[serde(rename = "ping")]
    Ping,
}

#[derive(Serialize, Debug)]
struct RequestMessage {
    #[serde(rename = "op")]
    op: RequestOp,

    #[serde(rename = "req_id")]
    req_id: String,

    #[serde(rename = "args")]
    args: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum BybitMessage {
    Stream(StreamMessage),
    Result(ResultMessage),
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum StreamMessageData {
    PublicTrade(Vec<StreamMessagePublicTradeData>),
    Orderbook(StreamMessageOrderbookData),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Stream {
    PublicTrade {
        symbol: String,
    },
    Orderbook {
        symbol: String,
        level: OrderbookLevel,
    },
}

#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, Hash)]
pub enum OrderbookLevel {
    L1 = 1,
    L50 = 50,
    L200 = 200,
}

impl Bybit {
    pub async fn connect_timeout(timeout: Duration) -> Result<Self, Box<dyn std::error::Error>> {
        let (stream, _) =
            tokio::time::timeout(timeout, connect_async(BYBIT_PUBLIC_SPOT_URL)).await??;
        let (tx, rx) = stream.split();

        Ok(Bybit {
            tx: Mutex::new((0, tx)),
            rx: Mutex::new(rx),
        })
    }

    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tx = self.tx.lock().await;
        tx.1.close().await?;
        Ok(())
    }

    pub async fn request_ping(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut tx = self.tx.lock().await;
        let req_id = tx.0.to_string();
        tx.0 += 1;
        let text = serde_json::to_string(&RequestMessage {
            op: RequestOp::Ping,
            req_id: req_id.clone(),
            args: None,
        })?;
        tx.1.send(Message::Text(text)).await?;
        Ok(req_id)
    }

    pub async fn request_streams(
        &self,
        op: RequestOp,
        streams: impl IntoIterator<Item = &Stream>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut tx = self.tx.lock().await;
        let req_id = tx.0.to_string();
        tx.0 += 1;
        let args = Some(streams.into_iter().map(Stream::to_string).collect());
        let text = serde_json::to_string(&RequestMessage {
            op,
            req_id: req_id.clone(),
            args,
        })?;
        tx.1.send(Message::Text(text)).await?;
        Ok(req_id)
    }

    pub async fn read_message_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<BybitMessage>, Box<dyn std::error::Error + Send + Sync>> {
        return tokio::time::timeout(timeout, self.read_message()).await?;
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

impl Stream {
    pub fn to_string(&self) -> String {
        match self {
            Stream::PublicTrade { symbol } => format!("publicTrade.{}", symbol),
            Stream::Orderbook { symbol, level } => {
                format!("orderbook.{}.{}", level.clone() as u8, symbol)
            }
        }
    }
}

pub fn deserialize_u64_string<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    s.parse().map_err(de::Error::custom)
}

pub async fn manage<DSI: futures::Stream<Item = Vec<Stream>> + Unpin + Send + 'static>(
    client: Arc<Bybit>,
    desired_streams_iter: DSI,
    stream_messages_tx: mpsc::UnboundedSender<StreamMessage>,
    request_limit: usize,
    request_interval: Duration,
    read_timeout: Duration,
    ping_interval: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (results_tx, results_rx) = mpsc::unbounded_channel();
    let mut subscription_manager = spawn({
        let client = client.clone();
        manage_subscriptions(
            client,
            desired_streams_iter,
            results_rx,
            request_limit,
            request_interval,
        )
    });

    let (pong_tx, pong_rx) = mpsc::unbounded_channel();
    let mut ping_manager = spawn({
        let client = client.clone();
        manage_pings(client, pong_rx, ping_interval)
    });

    let mut message_manager: JoinHandle<Result<(), Box<dyn Error + Sync + Send>>> = spawn({
        let client = client.clone();
        async move {
            loop {
                match client.read_message_timeout(read_timeout).await {
                    Ok(None) => Err("end of message stream")?,
                    Err(err) => Err(err)?,
                    Ok(Some(message)) => match message {
                        BybitMessage::Stream(message) => {
                            if let Err(err) = stream_messages_tx.send(message) {
                                Err(err)?;
                            }
                        }
                        BybitMessage::Result(result) => {
                            if !result.success {
                                Err(format!("request failed: {}", result.ret_msg))?;
                            }
                            match result.op {
                                RequestOp::Subscribe => results_tx.send((true, result.req_id))?,
                                RequestOp::Unsubscribe => {
                                    results_tx.send((false, result.req_id))?
                                }
                                RequestOp::Ping => pong_tx.send(())?,
                            };
                        }
                    },
                }
            }
        }
    });

    select! {
        result = &mut subscription_manager => result??,
        result = &mut ping_manager => result??,
        result = &mut message_manager => result??,
    };

    tracing::info!("Aborting subscription manager");
    subscription_manager.abort();
    tracing::info!("Aborting ping manager");
    ping_manager.abort();
    tracing::info!("Aborting message manager");
    message_manager.abort();

    Ok(())
}

async fn manage_subscriptions<DSI: futures::Stream<Item = Vec<Stream>> + Unpin>(
    client: Arc<Bybit>,
    mut desired_streams_iter: DSI,
    mut results_rx: mpsc::UnboundedReceiver<(bool, String)>,
    request_limit: usize,
    request_interval: Duration,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    #[derive(PartialEq, Eq)]
    enum StreamState {
        SubscribeRequested,
        UnsubscribeRequested,
        Subscribed,
    }
    let mut stream_states: HashMap<Stream, StreamState> = HashMap::new();
    let mut desired_streams = HashSet::new();

    let mut requests: HashMap<String, (RequestOp, Vec<Stream>)> = HashMap::new();

    loop {
        select! {
            _ = tokio::time::sleep(request_interval) => (), // TODO: should wait between requests
            desired_streams_rcv = desired_streams_iter.next() => match desired_streams_rcv {
                Some(desired_streams_rcv) => {
                    desired_streams.clear();
                    for desired_stream in desired_streams_rcv {
                        desired_streams.insert(desired_stream);
                    }
                },
                None => Err("end of desired streams stream")?,
            },
            result = results_rx.recv() => match result {
                Some((sub_unsub, req_id)) => {
                    let (request_op, streams) = match requests.remove(&req_id) {
                        Some(request) => request,
                        None => Err("unexpected request id")?,
                    };
                    match sub_unsub {
                        true => {
                            if request_op != RequestOp::Subscribe {
                                Err("unexpected request op")?;
                            }
                            for stream in streams {
                                match stream_states.get_mut(&stream) {
                                    Some(state) if StreamState::SubscribeRequested == *state => {
                                        *state = StreamState::Subscribed;
                                    },
                                    _ => {
                                        Err("unexpected stream state")?;
                                    },
                                }
                            }
                        },
                        false => {
                            if request_op != RequestOp::Unsubscribe {
                                Err("unexpected request op")?;
                            }
                            for stream in streams {
                                match stream_states.get(&stream) {
                                    Some(state) if StreamState::UnsubscribeRequested == *state => {
                                        stream_states.remove(&stream);
                                    },
                                    _ => {
                                        Err("unexpected stream state")?;
                                    },
                                }
                            }
                        },
                    }
                },
                None => Err("end of results stream")?,
            },
        };

        let request_unsubscribes = stream_states
            .iter()
            .filter(|(stream, _)| !desired_streams.contains(stream))
            .filter_map(|(stream, state)| match state {
                StreamState::Subscribed => Some(stream),
                _ => None,
            })
            .take(request_limit)
            .cloned()
            .collect::<Vec<_>>();

        if !request_unsubscribes.is_empty() {
            for stream in request_unsubscribes.iter() {
                stream_states.insert(stream.clone(), StreamState::UnsubscribeRequested);
            }
            let req_id = match client
                .request_streams(RequestOp::Unsubscribe, &request_unsubscribes.clone())
                .await
            {
                Ok(req_id) => req_id,
                Err(_) => Err("request failed")?,
            };
            requests.insert(req_id, (RequestOp::Unsubscribe, request_unsubscribes));
            continue;
        }

        let request_subscribes = desired_streams
            .iter()
            .filter_map(|stream| match stream_states.get(stream) {
                None => Some(stream),
                _ => None,
            })
            .take(request_limit)
            .cloned()
            .collect::<Vec<_>>();
        if !request_subscribes.is_empty() {
            for stream in request_subscribes.iter() {
                stream_states.insert(stream.clone(), StreamState::SubscribeRequested);
            }
            let req_id = match client
                .request_streams(RequestOp::Subscribe, &request_subscribes.clone())
                .await
            {
                Ok(req_id) => req_id,
                Err(_) => Err("request failed")?,
            };
            requests.insert(req_id, (RequestOp::Subscribe, request_subscribes));
            continue;
        }
    }
}

async fn manage_pings(
    client: Arc<Bybit>,
    mut results_rx: mpsc::UnboundedReceiver<()>,
    ping_interval: Duration,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut ping_timer = tokio::time::interval_at(Instant::now() + ping_interval, ping_interval);
    let mut expecting_pong = false;
    loop {
        select! {
            _ = ping_timer.tick() => match client.request_ping().await {
                Ok(_) => {
                    if expecting_pong {
                        Err("pong timeout")?;
                    }
                    expecting_pong = true;
                },
                Err(err) => Err(err)?,
            },
            result = results_rx.recv() => match result {
                Some(_) => {
                    ping_timer.reset();
                    expecting_pong = false;
                },
                None => Err("end of results stream")?,
            },
        }
    }
}
