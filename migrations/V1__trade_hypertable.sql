CREATE TABLE trade (
    id BIGINT NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    receive_time TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    taker_side taker_side NOT NULL,
    block BOOLEAN NOT null,
    symbol VARCHAR(10) NOT NULL
);

SELECT create_hypertable('trade', by_range('trade_time', INTERVAL '1 WEEK'));
