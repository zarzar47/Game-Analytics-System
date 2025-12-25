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