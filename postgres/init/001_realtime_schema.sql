-- Real-time serving store (ADR-011). Runs once, on first container start (docker-entrypoint-initdb.d).
-- Tables hold windowed aggregates upserted by the Spark realtime app with ABSOLUTE values
-- (idempotent under batch replay, ADR-007).

CREATE SCHEMA IF NOT EXISTS realtime;

-- Grain: one row per store per 5-minute event-time window.
CREATE TABLE IF NOT EXISTS realtime.store_revenue_5m (
    store_id       TEXT          NOT NULL,
    window_start   TIMESTAMPTZ   NOT NULL,
    window_end     TIMESTAMPTZ   NOT NULL,
    revenue        NUMERIC(14,2) NOT NULL,
    transactions   BIGINT        NOT NULL,
    units          BIGINT        NOT NULL,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id, window_start)
);
CREATE INDEX IF NOT EXISTS ix_store_revenue_5m_window ON realtime.store_revenue_5m (window_start);

-- Grain: one row per product per 1-hour event-time window.
CREATE TABLE IF NOT EXISTS realtime.product_units_1h (
    product_id     TEXT          NOT NULL,
    window_start   TIMESTAMPTZ   NOT NULL,
    window_end     TIMESTAMPTZ   NOT NULL,
    units          BIGINT        NOT NULL,
    revenue        NUMERIC(14,2) NOT NULL,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, window_start)
);
CREATE INDEX IF NOT EXISTS ix_product_units_1h_window ON realtime.product_units_1h (window_start);

-- Retention: operational data only; the warehouse keeps history.
CREATE OR REPLACE FUNCTION realtime.purge_old_windows(keep INTERVAL DEFAULT INTERVAL '7 days')
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    removed INTEGER := 0;
    n INTEGER;
BEGIN
    DELETE FROM realtime.store_revenue_5m WHERE window_start < now() - keep;
    GET DIAGNOSTICS n = ROW_COUNT; removed := removed + n;
    DELETE FROM realtime.product_units_1h WHERE window_start < now() - keep;
    GET DIAGNOSTICS n = ROW_COUNT; removed := removed + n;
    RETURN removed;
END $$;
