use std::{error::Error, sync::Arc};

use tokio::time::Duration;

use crate::bybit;

pub fn watch_symbols(
    consul: rs_consul::Consul,
    consul_symbols_key: String,
    orderbook_level: bybit::OrderbookLevel,
    refresh_interval: Duration,
    retry_interval: Duration,
) -> impl futures::Stream<Item = Vec<bybit::Stream>> {
    let consul = Arc::new(consul);
    futures::stream::unfold(false, move |mut should_sleep| {
        let consul = consul.clone();
        let consul_symbols_key = consul_symbols_key.clone();
        let orderbook_level = orderbook_level.clone();
        async move {
            if should_sleep {
                tokio::time::sleep(refresh_interval).await;
            }
            loop {
                let result: Result<Vec<bybit::Stream>, Box<dyn Error + Send + Sync>> = try {
                    tracing::info!("Reading symbols from consul");
                    let response = match consul
                        .read_key(rs_consul::ReadKeyRequest {
                            key: &consul_symbols_key,
                            ..Default::default()
                        })
                        .await
                    {
                        Err(err) => Err(format!("Failed to read symbols from consul: {}", err))?,
                        Ok(response) => response,
                    };
                    if response.len() != 1 {
                        Err(format!("Unexpected response from consul: {:?}", response))?
                    }
                    let value = match &response[0].value {
                        Some(value) => value,
                        None => Err(format!(
                            "Missing value in response from consul: {:?}",
                            response
                        ))?,
                    };
                    let symbols: Vec<String> = match serde_yaml::from_str(value) {
                        Err(err) => {
                            Err(format!("Failed to decode yaml value from consul: {}", err))?
                        }
                        Ok(symbols) => symbols,
                    };
                    symbols
                        .into_iter()
                        .flat_map(|symbol| {
                            [
                                bybit::Stream::PublicTrade {
                                    symbol: symbol.clone(),
                                },
                                bybit::Stream::Orderbook {
                                    symbol: symbol.clone(),
                                    level: orderbook_level.clone(),
                                },
                            ]
                        })
                        .collect::<Vec<_>>()
                };
                match result {
                    Ok(streams) => return Some((streams, true)),
                    Err(err) => {
                        tracing::error!("Error reading symbols from consul: {}", err);
                        tokio::time::sleep(retry_interval).await;
                    }
                }
            }
        }
    })
}
