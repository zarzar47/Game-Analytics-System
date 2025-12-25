-- ==============================================================================
-- RELAXED STAR SCHEMA FOR REAL-TIME STREAMING
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSION TABLES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_player (
    player_id VARCHAR(255) PRIMARY KEY,
    username VARCHAR(255),
    country_code VARCHAR(10),
    region VARCHAR(50),
    platform VARCHAR(50),
    player_segment VARCHAR(50),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_game (
    game_id VARCHAR(255) PRIMARY KEY,
    game_name VARCHAR(255),
    genre VARCHAR(100),
    developer VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    hour INT,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) UNIQUE,
    region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_item (
    item_id VARCHAR(255) PRIMARY KEY,
    item_name VARCHAR(255),
    category VARCHAR(100),
    base_price DECIMAL(10, 2)
);

-- -----------------------------------------------------------------------------
-- FACT TABLES (Relaxed Constraints for session_id)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_session (
    session_id VARCHAR(255) PRIMARY KEY,
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    start_time_id INT REFERENCES dim_time(time_id),
    duration_seconds DECIMAL(10, 2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_id VARCHAR(255) PRIMARY KEY,
    session_id VARCHAR(255), -- Relaxed: No REFERENCES
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT REFERENCES dim_time(time_id),
    amount_usd DECIMAL(10, 2),
    currency VARCHAR(10) DEFAULT 'USD'
);

CREATE TABLE IF NOT EXISTS fact_telemetry (
    telemetry_id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(255), -- Relaxed: No REFERENCES
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT REFERENCES dim_time(time_id),
    fps DECIMAL(5, 2),
    latency_ms DECIMAL(7, 2),
    platform VARCHAR(50),
    region VARCHAR(50)
);

-- -----------------------------------------------------------------------------
-- INDEXES
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_telemetry_session ON fact_telemetry(session_id);
CREATE INDEX IF NOT EXISTS idx_transaction_player ON fact_transaction(player_id);
