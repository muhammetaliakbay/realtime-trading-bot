use std::sync::Arc;

use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use parquet::{basic::LogicalType, schema::types::Type};

#[derive(Debug, ParquetRecordWriter)]
pub struct TradeStreamRecord {
    pub event_time: NaiveDateTime,
    pub symbol: String,
    pub trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub buyer_order_id: u64,
    pub seller_order_id: u64,
    pub trade_time: NaiveDateTime,
    pub buyer_maker: bool,
    pub ignore: bool,
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
