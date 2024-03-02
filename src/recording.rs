use std::{
    collections::{hash_map::Entry, BTreeMap},
    error::Error,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use futures::pin_mut;
use postgres_types::Type;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use tokio::{select, time};
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use crate::{
    bybit,
    entities::{self, OrderbookUpdate},
};

pub async fn record_trades(
    save_interval: time::Duration,
    db: Arc<tokio_postgres::Client>,
    mut trades_rx: tokio::sync::mpsc::UnboundedReceiver<entities::Trade>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Save timer
    let mut save_timer =
        tokio::time::interval_at(time::Instant::now() + save_interval, save_interval);

    let mut buffer = Vec::new();

    let mut open = true;
    while open {
        select! {
            trade = trades_rx.recv() => match trade {
                Some(trade) => {
                    buffer.push(trade);
                    continue;
                },
                None => {
                    open = false;
                },
            },
            _ = save_timer.tick() => (),
        }

        if !buffer.is_empty() {
            tracing::info!("Saving {} trade records", buffer.len());
            let sink = db
                .copy_in(
                    "
                    COPY trade(
                        trade_time,
                        symbol,
                        id,
                        price,
                        quantity,
                        taker_side,
                        block,
                        receive_time
                    ) FROM STDIN BINARY
                ",
                )
                .await?;

            let writer = BinaryCopyInWriter::new(
                sink,
                &[
                    Type::TIMESTAMPTZ,
                    Type::VARCHAR,
                    Type::INT8,
                    Type::FLOAT8,
                    Type::FLOAT8,
                    postgres_types::Type::new(
                        "taker_side".to_owned(),
                        0,
                        postgres_types::Kind::Enum(vec!["buy".to_string(), "sell".to_string()]),
                        "public".to_string(),
                    ),
                    Type::BOOL,
                    Type::TIMESTAMPTZ,
                ],
            );
            pin_mut!(writer);

            for trade in buffer.drain(..) {
                writer
                    .as_mut()
                    .write(&[
                        &trade.trade_time,
                        &trade.symbol,
                        &(trade.id as i64),
                        &trade.price,
                        &trade.quantity,
                        &trade.taker_side,
                        &trade.block,
                        &trade.receive_time,
                    ])
                    .await
                    .unwrap();
            }

            writer.finish().await?;
        }
    }

    Ok(())
}

pub async fn record_orderbook_snapshots(
    snapshot_interval: time::Duration,
    db: Arc<tokio_postgres::Client>,
    mut updates_rx: tokio::sync::mpsc::UnboundedReceiver<OrderbookUpdate>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Snapshot timer
    let mut snapshot_interval =
        tokio::time::interval_at(time::Instant::now() + snapshot_interval, snapshot_interval);

    // String -> (bids, asks, cross_sequence, update_id, update_time, receive_time, orderbook)
    let mut orderbooks = std::collections::HashMap::<
        String,
        (
            BTreeMap<Decimal, Decimal>,
            BTreeMap<Decimal, Decimal>,
            u64,
            u64,
            DateTime<Utc>,
            DateTime<Utc>,
        ),
    >::new();

    let mut open = true;
    while open {
        select! {
            update = updates_rx.recv() => match update {
                Some(update) => {
                    let entry = orderbooks.entry(update.symbol);
                    if update.clean {
                        let (bids, asks) = match entry {
                            Entry::Vacant(entry) => {
                                let (bids, asks, _, _, _, _) = entry.insert((
                                    BTreeMap::new(),
                                    BTreeMap::new(),
                                    update.cross_sequence,
                                    update.update_id,
                                    update.update_time,
                                    update.receive_time,
                                ));
                                (bids, asks)
                            },
                            Entry::Occupied(entry) => {
                                let (bids, asks, cross_sequence, update_id, update_time, receive_time) = entry.into_mut();
                                *cross_sequence = update.cross_sequence;
                                *update_id = update.update_id;
                                *update_time = update.update_time;
                                *receive_time = update.receive_time;
                                bids.clear();
                                asks.clear();
                                (bids, asks)
                            }
                        };
                        for (price, quantity) in update.bids {
                            bids.insert(price, quantity);
                        }
                        for (price, quantity) in update.asks {
                            asks.insert(price, quantity);
                        }
                    } else {
                        let (bids, asks, cross_sequence, update_id, update_time, receive_time) = match entry {
                            Entry::Vacant(_) => {
                                tracing::error!({symbol = entry.key(), update_id = update.update_id}, "missing snapshot");
                                continue;
                            },
                            Entry::Occupied(entry) => entry.into_mut(),
                        };
                        *cross_sequence = update.cross_sequence;
                        *update_id = update.update_id;
                        *update_time = update.update_time;
                        *receive_time = update.receive_time;
                        for (price, quantity) in update.bids {
                            if quantity.is_zero() {
                                bids.remove(&price);
                            } else {
                                bids.insert(price, quantity);
                            }
                        }
                        for (price, quantity) in update.asks {
                            if quantity.is_zero() {
                                asks.remove(&price);
                            } else {
                                asks.insert(price, quantity);
                            }
                        }
                    }
                    continue;
                },
                None => {
                    open = false;
                },
            },
            _ = snapshot_interval.tick() => (),
        };

        let sink = db
            .copy_in(
                "
                COPY orderbook_snapshot(
                    cross_sequence,
                    update_id,
                    update_time,
                    receive_time,
                    symbol,
                    bids,
                    asks
                ) FROM STDIN BINARY
            ",
            )
            .await?;

        let orderbook_row_type = entities::OrderbookRow::postgres_type();
        let orderbook_row_array_type = postgres_types::Type::new(
            "[]orderbook_row".to_owned(),
            0,
            postgres_types::Kind::Array(orderbook_row_type.clone()),
            "public".to_string(),
        );
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,
                Type::INT8,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMPTZ,
                Type::VARCHAR,
                orderbook_row_array_type.clone(),
                orderbook_row_array_type,
            ],
        );
        pin_mut!(writer);

        for (symbol, (bids, asks, cross_sequence, update_id, update_time, receive_time)) in
            orderbooks.iter()
        {
            writer
                .as_mut()
                .write(&[
                    &(*cross_sequence as i64),
                    &(*update_id as i64),
                    update_time,
                    receive_time,
                    symbol,
                    &bids
                        .iter()
                        .map(|(price, quantity)| entities::OrderbookRow {
                            price: price.to_f64().unwrap(),
                            quantity: quantity.to_f64().unwrap(),
                        })
                        .collect::<Vec<_>>(),
                    &asks
                        .iter()
                        .map(|(price, quantity)| entities::OrderbookRow {
                            price: price.to_f64().unwrap(),
                            quantity: quantity.to_f64().unwrap(),
                        })
                        .collect::<Vec<_>>(),
                ])
                .await
                .unwrap();
        }

        writer.finish().await?;
    }

    Ok(())
}

pub async fn stream_message_switch(
    mut stream_messages_rx: tokio::sync::mpsc::UnboundedReceiver<bybit::StreamMessage>,
    trades_tx: tokio::sync::mpsc::UnboundedSender<entities::Trade>,
    orderbook_updates_tx: tokio::sync::mpsc::UnboundedSender<OrderbookUpdate>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        let message = match stream_messages_rx.recv().await {
            Some(message) => message,
            None => Err("Stream message stream ended")?,
        };
        let timestamp = chrono::Utc::now();

        if let bybit::StreamMessageData::PublicTrade(trades) = message.data {
            for trade in trades {
                let price = match trade.price.to_f64() {
                    Some(price) => price,
                    None => {
                        tracing::error!({price = trade.price.to_string(), id = trade.trade_id, symbol = trade.symbol}, "unencodable price");
                        continue;
                    }
                };

                let quantity = match trade.quantity.to_f64() {
                    Some(quantity) => quantity,
                    None => {
                        tracing::error!({quantity = trade.quantity.to_string(), id = trade.trade_id, symbol = trade.symbol}, "unencodable quantity");
                        continue;
                    }
                };

                let taker_side = match trade.taker_side {
                    bybit::TakerSide::Buy => entities::TakerSide::Buy,
                    bybit::TakerSide::Sell => entities::TakerSide::Sell,
                };

                trades_tx.send(entities::Trade {
                    trade_time: trade.trade_time,
                    symbol: trade.symbol,
                    id: trade.trade_id,
                    price,
                    quantity,
                    taker_side,
                    block: trade.block_trade,
                    receive_time: timestamp,
                })?;
            }
        } else if let bybit::StreamMessageData::Orderbook(orderbook) = message.data {
            orderbook_updates_tx.send(entities::OrderbookUpdate{
                clean: match message.typ {
                    bybit::StreamMessageType::Delta => false,
                    bybit::StreamMessageType::Snapshot => true,
                },
                update_time: match message.cts {
                    Some(update_time) => update_time.0,
                    None => {
                        tracing::error!({symbol = orderbook.symbol, update_id = orderbook.update_id}, "missing cts");
                        Err("missing cts field in order book stream mesage")?;
                        break;
                    },
                },
                symbol: orderbook.symbol,
                receive_time: timestamp,
                bids: orderbook.bids.into_iter().map(|row| (row.0, row.1)).collect(),
                asks: orderbook.asks.into_iter().map(|row| (row.0, row.1)).collect(),
                cross_sequence: orderbook.cross_sequence,
                update_id: orderbook.update_id,
            })?;
        }
    }

    Ok(())
}
