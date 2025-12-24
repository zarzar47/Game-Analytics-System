# Data Dictionary & Schema Design

## Business Problem: Real-Time LiveOps Optimization
**Goal:** Maximize player retention and revenue by monitoring real-time game health, economy balance, and player sentiment.
**Justification:** Modern F2P games rely on microtransactions. Technical issues (latency) or poor balance (too hard/easy) cause immediate churn. Real-time dashboards allow LiveOps managers to:
- Detect regional server outages (Latency spikes in specific regions).
- Balance economy (Monitor "Whale" spending vs "Casual" churn).
- React to negative sentiment after patches.

## Big Data Schema (Star Schema for Analytics)

### Fact Tables (Streams)

#### 1. Fact_Session_Activity
Tracks the lifecycle of player sessions.
- `session_id` (UUID): Unique session identifier.
- `player_id` (UUID): Foreign Key to Dim_Player.
- `game_id` (String): Foreign Key to Dim_Game.
- `start_time` (Timestamp): Session start.
- `end_time` (Timestamp): Session end.
- `duration_sec` (Float): Total duration.
- `termination_reason` (String): 'user_quit', 'crash', 'timeout'.

#### 2. Fact_Telemetry (Heartbeats)
High-frequency performance data (sampled every 5s).
- `event_id` (UUID)
- `session_id` (UUID)
- `timestamp` (Timestamp)
- `fps` (Float): Frames Per Second.
- `latency_ms` (Int): Network latency.
- `cpu_usage` (Float): Client device load.

#### 3. Fact_Transactions
In-game purchases.
- `transaction_id` (UUID)
- `session_id` (UUID)
- `player_id` (UUID)
- `item_id` (String): Foreign Key to Dim_Item.
- `amount_usd` (Float): Real money value.
- `currency` (String): 'USD', 'EUR'.

#### 4. Fact_Progression
Player leveling events.
- `event_id` (UUID)
- `session_id` (UUID)
- `level_index` (Int): New level reached.
- `xp_gained` (Int).

#### 5. Fact_Feedback
User reviews and sentiment.
- `review_id` (UUID)
- `player_id` (UUID)
- `sentiment_score` (Float): 0.0 (Negative) to 1.0 (Positive).
- `text` (String): Raw review text.

### Dimension Tables

#### Dim_Player
- `player_id` (PK)
- `username`
- `country_code`
- `region` (Derived: NA, EU, ASIA)
- `platform` (PC, PS5, Mobile)
- `player_segment` (Casual, Hardcore, Whale) - *Slowly Changing Dimension*

#### Dim_Game
- `game_id` (PK)
- `name`
- `genre`
- `developer`
- `release_date`

#### Dim_Time (Derived)
- `hour`, `day`, `month`, `is_weekend`

---

## Archiving Policy
- **Hot Storage (Kafka/Mongo):** Last 24 hours of raw telemetry.
- **Warm Storage (Parquet/S3):** Aggregated hourly stats for dashboarding (kept for 1 year).
- **Cold Archive (Glacier):** Raw logs compressed (kept for 5 years for compliance/deep ML training).
- **Trigger:** If `hot_storage > 300MB`, rotate oldest segments to S3 Parquet.

## Data Generation Strategy (AI/Statistical)
The generator uses statistical distributions to mimic real human behavior:
- **Arrivals:** Sinusoidal daily wave + Random Noise (mimics day/night cycles).
- **Session Duration:** Log-Normal Distribution (Long tail of hardcore users).
- **Spending:** Pareto Distribution (80/20 rule - few "Whales" spend the most).
- **Performance:** Gaussian Distribution (Dependent on Region/Distance).
