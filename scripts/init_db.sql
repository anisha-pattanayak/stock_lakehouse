-- ============================================================
-- Stock Lakehouse Platform - PostgreSQL Initialization
-- ============================================================

-- Create Airflow database
CREATE DATABASE airflow_db;

-- Create metadata tables in lakehouse_meta
\c lakehouse_meta;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          SERIAL PRIMARY KEY,
    dag_id      VARCHAR(255),
    run_id      VARCHAR(255),
    status      VARCHAR(50),
    started_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at    TIMESTAMP WITH TIME ZONE,
    metadata    JSONB
);

CREATE TABLE IF NOT EXISTS quality_check_results (
    id          SERIAL PRIMARY KEY,
    check_name  VARCHAR(255),
    layer       VARCHAR(50),     -- bronze, silver, gold
    passed      BOOLEAN,
    details     JSONB,
    checked_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_log (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(20),
    price       NUMERIC(12,4),
    z_score     NUMERIC(8,4),
    anomaly_type VARCHAR(100),
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_pipeline_runs_dag ON pipeline_runs(dag_id);
CREATE INDEX idx_quality_checks_layer ON quality_check_results(layer, checked_at);
CREATE INDEX idx_anomaly_symbol ON anomaly_log(symbol, detected_at);

-- Seed some config data
INSERT INTO pipeline_runs (dag_id, run_id, status)
VALUES ('system_init', 'init_run_001', 'completed');
