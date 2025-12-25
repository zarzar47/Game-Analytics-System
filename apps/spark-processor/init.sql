-- Table for Real-time Revenue Analytics
CREATE TABLE IF NOT EXISTS realtime_revenue (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    game_name VARCHAR(255),
    player_type VARCHAR(50),
    total_revenue DOUBLE PRECISION,
    avg_purchase DOUBLE PRECISION,
    unique_purchasers BIGINT,
    PRIMARY KEY (window_start, game_name, player_type)
);

-- Table for Real-time Player Concurrency
CREATE TABLE IF NOT EXISTS realtime_concurrency (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    game_name VARCHAR(255),
    region VARCHAR(50),
    concurrent_players BIGINT,
    PRIMARY KEY (window_start, game_name, region)
);

-- Table for Real-time Game Performance
CREATE TABLE IF NOT EXISTS realtime_performance (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    game_name VARCHAR(255),
    platform VARCHAR(50),
    region VARCHAR(50),
    avg_fps DOUBLE PRECISION,
    avg_latency DOUBLE PRECISION,
    PRIMARY KEY (window_start, game_name, platform, region)
);

CREATE DATABASE airflow;

-- ==============================================================================
-- ENHANCED STAR SCHEMA FOR GAME ANALYTICS
-- Designed for dimensional queries with joins (GROUP BY, WHERE, HAVING)
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSION TABLES (Slowly Changing Dimensions)
-- -----------------------------------------------------------------------------

-- Dim_Player: Player profile and demographics
CREATE TABLE IF NOT EXISTS dim_player (
    player_id VARCHAR(255) PRIMARY KEY,
    username VARCHAR(255),
    country_code VARCHAR(10),
    region VARCHAR(50), -- NA, EU, ASIA, SA, OCE
    platform VARCHAR(50), -- PC, PS5, Xbox, Mobile
    player_segment VARCHAR(50), -- CASUAL, HARDCORE, WHALE
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    -- SCD Type 2 fields
    effective_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dim_Game: Game catalog
CREATE TABLE IF NOT EXISTS dim_game (
    game_id VARCHAR(255) PRIMARY KEY,
    game_name VARCHAR(255),
    genre VARCHAR(100),
    developer VARCHAR(255),
    release_date DATE,
    price_tier VARCHAR(50), -- Free, Budget, Premium
    is_multiplayer BOOLEAN DEFAULT TRUE
);

-- Dim_Time: Time dimension for temporal analysis
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    hour INT,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(10),
    is_weekend BOOLEAN,
    quarter INT,
    week_of_year INT
);

-- Dim_Geography: Location hierarchy
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id SERIAL PRIMARY KEY,
    country_code VARCHAR(10),
    country_name VARCHAR(255),
    region VARCHAR(50), -- NA, EU, ASIA
    timezone VARCHAR(50),
    avg_latency_baseline INT -- Expected latency in ms
);

-- Dim_Item: In-game purchasable items
CREATE TABLE IF NOT EXISTS dim_item (
    item_id VARCHAR(255) PRIMARY KEY,
    item_name VARCHAR(255),
    category VARCHAR(100), -- Cosmetic, Weapon, Powerup
    rarity VARCHAR(50), -- Common, Rare, Legendary
    base_price DECIMAL(10, 2),
    currency VARCHAR(10) DEFAULT 'USD'
);

-- -----------------------------------------------------------------------------
-- FACT TABLES (Transactional and Periodic Snapshots)
-- -----------------------------------------------------------------------------

-- Fact_Session: Player session lifecycle
CREATE TABLE IF NOT EXISTS fact_session (
    session_id VARCHAR(255) PRIMARY KEY,
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    start_time_id INT REFERENCES dim_time(time_id),
    end_time_id INT REFERENCES dim_time(time_id),
    geo_id INT REFERENCES dim_geography(geo_id),
    
    -- KPIs (FACTS)
    duration_seconds DECIMAL(10, 2),
    matches_played INT DEFAULT 0,
    levels_gained INT DEFAULT 0,
    termination_reason VARCHAR(50), -- user_quit, crash, timeout
    
    -- Derived metrics
    avg_fps DECIMAL(5, 2),
    avg_latency_ms DECIMAL(7, 2),
    peak_concurrent_players INT
);

-- Fact_Transaction: Monetization events
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_id VARCHAR(255) PRIMARY KEY,
    session_id VARCHAR(255),
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    item_id VARCHAR(255) REFERENCES dim_item(item_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT REFERENCES dim_time(time_id),
    
    -- KPIs (FACTS)
    amount_usd DECIMAL(10, 2),
    currency VARCHAR(10),
    payment_method VARCHAR(50), -- Credit, PayPal, Crypto
    discount_applied DECIMAL(5, 2) DEFAULT 0.0,
    
    -- Business context
    is_first_purchase BOOLEAN DEFAULT FALSE,
    days_since_registration INT
);

-- Fact_Telemetry: Performance metrics (Periodic snapshot)
CREATE TABLE IF NOT EXISTS fact_telemetry (
    telemetry_id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(255) REFERENCES fact_session(session_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT REFERENCES dim_time(time_id),
    
    -- KPIs (FACTS)
    fps DECIMAL(5, 2),
    latency_ms DECIMAL(7, 2),
    cpu_usage DECIMAL(5, 2),
    memory_usage_mb DECIMAL(10, 2),
    packet_loss_percent DECIMAL(5, 3),
    
    -- Context
    platform VARCHAR(50),
    region VARCHAR(50)
);

-- Fact_Engagement: User feedback and sentiment
CREATE TABLE IF NOT EXISTS fact_engagement (
    engagement_id BIGSERIAL PRIMARY KEY,
    player_id VARCHAR(255) REFERENCES dim_player(player_id),
    game_id VARCHAR(255) REFERENCES dim_game(game_id),
    time_id INT REFERENCES dim_time(time_id),
    
    -- KPIs (FACTS)
    sentiment_score DECIMAL(3, 2), -- 0.0 to 1.0
    review_length INT,
    playtime_at_review_hours DECIMAL(10, 2),
    
    -- Text for NLP
    review_text TEXT
);

-- -----------------------------------------------------------------------------
-- INDEXES FOR PERFORMANCE (Critical for GROUP BY and JOIN queries)
-- -----------------------------------------------------------------------------

CREATE INDEX idx_session_player ON fact_session(player_id);
CREATE INDEX idx_session_game ON fact_session(game_id);
CREATE INDEX idx_session_time ON fact_session(start_time_id);

CREATE INDEX idx_transaction_player ON fact_transaction(player_id);
CREATE INDEX idx_transaction_item ON fact_transaction(item_id);
CREATE INDEX idx_transaction_time ON fact_transaction(time_id);

CREATE INDEX idx_telemetry_session ON fact_telemetry(session_id);
CREATE INDEX idx_telemetry_time ON fact_telemetry(time_id);

CREATE INDEX idx_engagement_player ON fact_engagement(player_id);
CREATE INDEX idx_engagement_game ON fact_engagement(game_id);

-- -----------------------------------------------------------------------------
-- EXAMPLE DIMENSIONAL QUERIES (For BI Dashboards)
-- -----------------------------------------------------------------------------

-- Query 1: Revenue by Player Segment and Region (JOIN + GROUP BY)
-- Shows whale vs casual spending across geographies
SELECT 
    dp.player_segment,
    dg.region,
    COUNT(DISTINCT ft.transaction_id) AS total_purchases,
    SUM(ft.amount_usd) AS total_revenue,
    AVG(ft.amount_usd) AS avg_purchase_value
FROM fact_transaction ft
JOIN dim_player dp ON ft.player_id = dp.player_id
JOIN dim_geography dg ON dg.country_code = dp.country_code
WHERE ft.time_id IN (
    SELECT time_id FROM dim_time WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
)
GROUP BY dp.player_segment, dg.region
HAVING SUM(ft.amount_usd) > 1000 -- Only regions with >$1000 revenue
ORDER BY total_revenue DESC;

-- Query 2: Game Performance by Platform (JOIN + WHERE + GROUP BY)
-- Identifies platforms with poor FPS
SELECT 
    dgame.game_name,
    ft.platform,
    AVG(ft.fps) AS avg_fps,
    AVG(ft.latency_ms) AS avg_latency,
    COUNT(*) AS sample_count
FROM fact_telemetry ft
JOIN dim_game dgame ON ft.game_id = dgame.game_id
WHERE ft.time_id IN (
    SELECT time_id FROM dim_time WHERE hour BETWEEN 18 AND 23 -- Peak hours
)
GROUP BY dgame.game_name, ft.platform
HAVING AVG(ft.fps) < 50 -- Flag low-performing platforms
ORDER BY avg_fps ASC;

-- Query 3: Player Retention Analysis (Complex JOIN)
-- Calculates churn rate by cohort
SELECT 
    dt.month AS registration_month,
    dp.player_segment,
    COUNT(DISTINCT dp.player_id) AS total_players,
    SUM(CASE WHEN dp.is_active = TRUE THEN 1 ELSE 0 END) AS active_players,
    (SUM(CASE WHEN dp.is_active = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT dp.player_id)) AS retention_rate
FROM dim_player dp
JOIN dim_time dt ON DATE_TRUNC('month', dp.registration_date) = DATE_TRUNC('month', dt.timestamp)
GROUP BY dt.month, dp.player_segment
HAVING COUNT(DISTINCT dp.player_id) > 100 -- Statistically significant cohorts
ORDER BY registration_month DESC, retention_rate DESC;

-- Query 4: Item Popularity and Revenue (JOIN + HAVING)
SELECT 
    di.item_name,
    di.category,
    di.rarity,
    COUNT(ft.transaction_id) AS times_purchased,
    SUM(ft.amount_usd) AS total_revenue,
    AVG(ft.amount_usd) AS avg_price_paid
FROM fact_transaction ft
JOIN dim_item di ON ft.item_id = di.item_id
GROUP BY di.item_name, di.category, di.rarity
HAVING COUNT(ft.transaction_id) > 50 -- Popular items only
ORDER BY total_revenue DESC
LIMIT 20;

-- Query 5: Weekend vs Weekday Engagement (Temporal Analysis)
SELECT 
    dt.is_weekend,
    dgame.game_name,
    COUNT(DISTINCT fs.session_id) AS total_sessions,
    AVG(fs.duration_seconds / 3600.0) AS avg_session_hours,
    SUM(fs.matches_played) AS total_matches
FROM fact_session fs
JOIN dim_time dt ON fs.start_time_id = dt.time_id
JOIN dim_game dgame ON fs.game_id = dgame.game_id
WHERE dt.timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY dt.is_weekend, dgame.game_name
ORDER BY avg_session_hours DESC;
