CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.agg_trades
(
    aggregate_trade_id UInt64,
    symbol LowCardinality(String),

    price Decimal(18, 8),
    quantity Decimal(18, 8),

    event_time DateTime64(3, 'UTC'),
    trade_time DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),

    first_trade_id UInt64,
    last_trade_id UInt64,
    buyer_is_market_maker Bool,
    
    kafka_partition UInt32,
    kafka_offset UInt64,

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (symbol, trade_time, aggregate_trade_id);