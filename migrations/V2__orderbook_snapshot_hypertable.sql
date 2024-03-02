CREATE TYPE orderbook_row AS (
    price DOUBLE PRECISION,
    quantity DOUBLE PRECISION
);

CREATE TABLE orderbook_snapshot (
    cross_sequence BIGINT NOT NULL,
    update_id BIGINT NOT NULL,
    update_time TIMESTAMPTZ NOT NULL,
    receive_time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    bids orderbook_row[] NOT NULL,
    asks orderbook_row[] NOT NULL
);

SELECT create_hypertable('orderbook_snapshot', by_range('update_time', INTERVAL '1 WEEK'));
