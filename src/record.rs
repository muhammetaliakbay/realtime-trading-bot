use std::sync::Arc;

use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use parquet::{basic::LogicalType, schema::types::Type};
use rust_decimal::{prelude::ToPrimitive, Decimal};

#[derive(Debug, ParquetRecordWriter)]
pub struct TradeStreamRecord {
    pub trade_time: NaiveDateTime,
    pub symbol: String,
    pub trade_id: i64,
    pub price: i64,
    pub quantity: i64,
    pub taker_side: String,
    pub block_trade: bool,
}

const DECIMAL_SCALE: u32 = 9;
const DECIMAL_PRECISION: u32 = 18;
const DECIMAL_MAX_MANTISSA: i64 = 10_i64.pow(DECIMAL_PRECISION);
const DECIMAL_MIN_MANTISSA: i64 = -DECIMAL_MAX_MANTISSA;

lazy_static! {
    static ref TRADE_STREAM_RECORD_TYPE: Arc<Type> = {
        Arc::new(
            Type::group_type_builder("TradeStreamRecord")
                .with_fields(vec![
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
                        Type::primitive_type_builder("symbol", parquet::basic::Type::BYTE_ARRAY)
                            .with_logical_type(Some(parquet::basic::LogicalType::Enum))
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
                        Type::primitive_type_builder("price", parquet::basic::Type::INT64)
                            .with_logical_type(Some(parquet::basic::LogicalType::Decimal {
                                scale: DECIMAL_SCALE as i32,
                                precision: DECIMAL_PRECISION as i32,
                            }))
                            .with_scale(DECIMAL_SCALE as i32)
                            .with_precision(DECIMAL_PRECISION as i32)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("quantity", parquet::basic::Type::INT64)
                            .with_logical_type(Some(parquet::basic::LogicalType::Decimal {
                                scale: DECIMAL_SCALE as i32,
                                precision: DECIMAL_PRECISION as i32,
                            }))
                            .with_scale(DECIMAL_SCALE as i32)
                            .with_precision(DECIMAL_PRECISION as i32)
                            .with_repetition(parquet::basic::Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder(
                            "taker_side",
                            parquet::basic::Type::BYTE_ARRAY,
                        )
                        .with_logical_type(Some(parquet::basic::LogicalType::Enum))
                        .with_repetition(parquet::basic::Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("block_trade", parquet::basic::Type::BOOLEAN)
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

    pub fn encode_decimal(org: &Decimal) -> Option<i64> {
        if org.scale() > DECIMAL_SCALE {
            return None;
        }
        let trunc = org.trunc_with_scale(DECIMAL_SCALE);
        let mantissa = match trunc.mantissa().to_i64() {
            Some(m) => m,
            None => return None,
        };
        if mantissa < DECIMAL_MIN_MANTISSA || mantissa > DECIMAL_MAX_MANTISSA {
            return None;
        }
        Some(mantissa)
    }
}
