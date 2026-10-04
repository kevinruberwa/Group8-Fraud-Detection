-- MySQL schema for the fraud detection project (Step 5)
-- Database: fraud_db (created automatically by docker-compose)
-- Each row = one transaction scored in real time by the Spark Stage 3 consumer.
-- The Django dashboard (Step 6) reads from this table.

USE fraud_db;

CREATE TABLE IF NOT EXISTS fraud_predictions (
    id                     BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id         BIGINT        NOT NULL,
    customer_id            BIGINT,
    event_time             DATETIME,
    amount                 DECIMAL(12,2),
    merchant_category      VARCHAR(30),
    device_type            VARCHAR(30),
    txn_city               VARCHAR(50),
    card_present           BOOLEAN,
    distance_from_home_km  DOUBLE,
    transactions_last_1h   INT,
    amount_to_avg_ratio    DOUBLE,
    hour_of_day            INT,
    is_high_risk_merchant  TINYINT,
    is_far_from_home       TINYINT,
    is_high_velocity       TINYINT,
    fraud_probability      DOUBLE,        -- model's fraud probability (0 to 1)
    prediction             TINYINT,       -- model's decision: 1 = flagged as fraud
    risk_level             VARCHAR(10),   -- LOW / MEDIUM / HIGH (from fraud_probability)
    is_fraud               TINYINT,       -- actual label from the dataset (to measure accuracy live)
    batch_id               BIGINT,        -- Spark micro-batch that processed this row
    processed_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_transaction (transaction_id),
    INDEX idx_event_time (event_time),
    INDEX idx_risk (risk_level),
    INDEX idx_prediction (prediction)
);
