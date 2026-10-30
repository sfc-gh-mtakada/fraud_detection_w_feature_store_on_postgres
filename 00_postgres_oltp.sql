-- =============================================================================
-- 00_postgres_oltp.sql : OLTP schema on Snowflake Postgres (instance FRAUD_OLTP)
--   The authorization service writes every decision here (low-latency OLTP);
--   the Streamlit app reads the live feed and the analyst case queue from it.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.auth_decisions (
    txn_id        TEXT PRIMARY KEY,
    card_id       TEXT        NOT NULL,
    acceptor_id   TEXT        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    amount        NUMERIC(12,0) NOT NULL,
    entry_mode    TEXT        NOT NULL,
    scenario      TEXT,                       -- NORMAL / BURST / TERMINAL / HIGH_VALUE (demo ground truth)
    score         DOUBLE PRECISION NOT NULL,
    decision      TEXT        NOT NULL CHECK (decision IN ('APPROVE', 'REVIEW', 'DECLINE')),
    features      JSONB       NOT NULL,       -- feature vector used for the decision
    feature_ms    DOUBLE PRECISION,           -- online feature retrieval latency (client side)
    total_ms      DOUBLE PRECISION,           -- end-to-end authorization latency
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_auth_decisions_created ON fraud.auth_decisions (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_auth_decisions_card    ON fraud.auth_decisions (card_id, ts DESC);

-- Analyst case queue: REVIEW / DECLINE decisions waiting for a human verdict
CREATE TABLE IF NOT EXISTS fraud.cases (
    case_id     BIGSERIAL PRIMARY KEY,
    txn_id      TEXT NOT NULL REFERENCES fraud.auth_decisions (txn_id),
    status      TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CONFIRMED_FRAUD', 'FALSE_POSITIVE')),
    analyst     TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_cases_status ON fraud.cases (status, updated_at DESC);

-- Demo control: the app enqueues fraud scenarios, the simulator picks them up
CREATE TABLE IF NOT EXISTS fraud.scenario_requests (
    request_id  BIGSERIAL PRIMARY KEY,
    scenario    TEXT NOT NULL CHECK (scenario IN ('BURST', 'TERMINAL', 'HIGH_VALUE')),
    target_id   TEXT,                         -- optional card or acceptor id
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at TIMESTAMPTZ
);
