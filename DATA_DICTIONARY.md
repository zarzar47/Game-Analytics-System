# Data Dictionary & Schema Design

## Business Problem: Real-Time LiveOps Optimization
**Goal:** Maximize player retention and revenue by monitoring real-time game health, economy balance, and player sentiment.
**Justification:** Modern F2P games rely on microtransactions. Technical issues (latency) or poor balance (too hard/easy) cause immediate churn. Real-time dashboards allow LiveOps managers to:
- Detect regional server outages (Latency spikes in specific regions).
- Balance economy (Monitor "Whale" spending vs "Casual" churn).
- React to negative sentiment after patches.

## Big Data Schema (Star Schema for Analytics)

This schema is implemented in the PostgreSQL database and represents the data available for querying by the backend API.

### Dimension Tables

#### 1. Dim_Player
Stores information about individual players.
- `player_id` (PK, VARCHAR): Unique identifier for each player.
- `username` (VARCHAR): The player's username.
- `country_code` (VARCHAR): Two-letter country code (e.g., 'US', 'GB').
- `region` (VARCHAR): Geographic region (e.g., 'NA', 'EU', 'ASIA').
- `platform` (VARCHAR): The platform the player uses (e.g., 'PC', 'PS5', 'Mobile').
- `player_segment` (VARCHAR): Player archetype (e.g., 'CASUAL', 'HARDCORE', 'WHALE'). *Slowly Changing Dimension.*
- `registration_date` (TIMESTAMP): When the player first registered.
- `last_login` (TIMESTAMP): The last time the player logged in.
- `is_active` (BOOLEAN): Whether the player's account is currently active.

#### 2. Dim_Game
Stores information about the games.
- `game_id` (PK, VARCHAR): Unique identifier for each game.
- `game_name` (VARCHAR): The name of the game.
- `genre` (VARCHAR): The game's genre (e.g., 'RPG', 'Shooter').
- `developer` (VARCHAR): The company that developed the game.

#### 3. Dim_Time
Provides temporal context for events, allowing for time-based analysis.
- `time_id` (PK, SERIAL): Unique identifier for each time record.
- `timestamp` (TIMESTAMP): The specific timestamp.
- `hour` (INT): The hour of the day (0-23).
- `day` (INT): The day of the month.
- `month` (INT): The month of the year.
- `year` (INT): The year.
- `day_of_week` (VARCHAR): The name of the day (e.g., 'Monday').
- `is_weekend` (BOOLEAN): True if the day is a Saturday or Sunday.

#### 4. Dim_Geography
Maps country codes to larger regions.
- `geo_id` (PK, SERIAL): Unique identifier for the geography record.
- `country_code` (VARCHAR): Two-letter country code.
- `region` (VARCHAR): The associated region.

#### 5. Dim_Item
Stores information about in-game items available for purchase.
- `item_id` (PK, VARCHAR): Unique identifier for the item.
- `item_name` (VARCHAR): The name of the item.
- `category` (VARCHAR): The item's category (e.g., 'Weapon', 'Cosmetic').
- `base_price` (DECIMAL): The base price of the item in USD.

### Fact Tables

#### 1. Fact_Session
Tracks player session lifecycle.
- `session_id` (PK, VARCHAR): Unique identifier for the session.
- `player_id` (FK, VARCHAR): Foreign key to `dim_player`.
- `game_id` (FK, VARCHAR): Foreign key to `dim_game`.
- `start_time_id` (FK, INT): Foreign key to `dim_time`, marking the session start.
- `duration_seconds` (DECIMAL): The total duration of the session in seconds.

#### 2. Fact_Transaction
Records in-game purchases.
- `transaction_id` (PK, VARCHAR): Unique identifier for the transaction.
- `session_id` (VARCHAR): The session in which the transaction occurred. (Relaxed foreign key)
- `player_id` (FK, VARCHAR): Foreign key to `dim_player`.
- `game_id` (FK, VARCHAR): Foreign key to `dim_game`.
- `time_id` (FK, INT): Foreign key to `dim_time`.
- `amount_usd` (DECIMAL): The value of the transaction in USD.
- `currency` (VARCHAR): The currency used ('USD').

#### 3. Fact_Telemetry
High-frequency performance data.
- `telemetry_id` (PK, BIGSERIAL): Unique identifier for the telemetry event.
- `session_id` (VARCHAR): The session the telemetry belongs to. (Relaxed foreign key)
- `game_id` (FK, VARCHAR): Foreign key to `dim_game`.
- `time_id` (FK, INT): Foreign key to `dim_time`.
- `fps` (DECIMAL): Client frames per second.
- `latency_ms` (DECIMAL): Network latency in milliseconds.
- `platform` (VARCHAR): The player's platform.
- `region` (VARCHAR): The player's region.

---

## Untracked Data Streams

The data generator (`faker` service) produces a richer set of events than what is currently loaded into the final PostgreSQL star schema. This additional data is available in the Kafka stream and could be integrated into the analytics database in the future.

- **Player Progression (`level_up` event):**
  - `level_index` (Int): The new level a player has reached.
  - `xp_gained` (Int): Experience points gained in the event.

- **Player Feedback (`review` event):**
  - `review_id` (UUID): Unique identifier for the review.
  - `sentiment_score` (Float): Calculated sentiment from 0.0 (Negative) to 1.0 (Positive).
  - `review_text` (String): The raw text of the player's review.

- **Detailed Transaction Data (`purchase` event):**
  - `item_id` (String): The specific item that was purchased. This data is generated but not loaded into `fact_transaction`.

- **Extended Telemetry (`heartbeat` event):**
  - `cpu_usage` (Float): Client-side CPU load.
  - `packet_loss_percent` (Float): Network packet loss.