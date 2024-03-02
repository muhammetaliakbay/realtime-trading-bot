use chrono::{DateTime, Utc};
use postgres_types::ToSql;
use rust_decimal::Decimal;

#[derive(Debug, ToSql)]
#[postgres(name = "taker_side", allow_mismatch)]
pub enum TakerSide {
    #[postgres(name = "buy")]
    Buy,
    #[postgres(name = "sell")]
    Sell,
}

pub struct Trade {
    pub id: u64,
    pub trade_time: DateTime<Utc>,
    pub receive_time: DateTime<Utc>,
    pub price: f64,
    pub quantity: f64,
    pub taker_side: TakerSide,
    pub block: bool,
    pub symbol: String,
}

#[derive(Debug, ToSql)]
#[postgres(name = "orderbook_row")]
pub struct OrderbookRow {
    #[postgres(name = "price")]
    pub price: f64,
    #[postgres(name = "quantity")]
    pub quantity: f64,
}

impl OrderbookRow {
    pub fn postgres_type() -> postgres_types::Type {
        postgres_types::Type::new(
            "orderbook_row".to_owned(),
            0,
            postgres_types::Kind::Composite(vec![
                postgres_types::Field::new("price".to_string(), postgres_types::Type::FLOAT8),
                postgres_types::Field::new("quantity".to_string(), postgres_types::Type::FLOAT8),
            ]),
            "public".to_string(),
        )
    }
}

pub struct _OrderbookSnapshot {
    pub cross_sequence: u64,
    pub update_id: u64,
    pub update_time: DateTime<Utc>,
    pub receive_time: DateTime<Utc>,
    pub symbol: String,
    pub bids: Vec<OrderbookRow>,
    pub asks: Vec<OrderbookRow>,
}

pub struct OrderbookUpdate {
    pub symbol: String,
    pub clean: bool,
    pub update_time: DateTime<Utc>,
    pub receive_time: DateTime<Utc>,
    pub update_id: u64,
    pub cross_sequence: u64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}
