from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from sqlalchemy.sql import func
from .database import Base

class GameMetric(Base):
    __tablename__ = "game_metrics"

    id = Column(Integer, primary_key=True, index=True)
    game_id = Column(String, index=True)
    game_name = Column(String)
    event_type = Column(String, index=True)  # "status", "review", "purchase"
    
    # Common Metrics
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    
    # Status Fields
    player_count = Column(Integer, nullable=True)
    sentiment_score = Column(Float, nullable=True)
    
    # Review Fields
    review_text = Column(String, nullable=True)
    playtime_session = Column(Float, nullable=True)
    playtime_total = Column(Float, nullable=True)
    
    # Purchase Fields
    purchase_amount = Column(Float, nullable=True)

    # --- Player & Session Details (New) ---
    player_id = Column(String, nullable=True, index=True)
    session_id = Column(String, nullable=True, index=True)
    match_id = Column(String, nullable=True, index=True)
    region = Column(String, nullable=True)
    platform = Column(String, nullable=True)
    player_type = Column(String, nullable=True) # Archetype: CASUAL, HARDCORE, WHALE
    country = Column(String, nullable=True)

    # --- Performance & Gameplay (New) ---
    fps = Column(Integer, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    level = Column(Integer, nullable=True) # From level_up events

    # --- Monetization Details (New) ---
    currency = Column(String, nullable=True)
    item_id = Column(String, nullable=True)

class RealtimeRevenue(Base):
    __tablename__ = "realtime_revenue"
    window_start = Column(DateTime, primary_key=True)
    window_end = Column(DateTime)
    game_name = Column(String, primary_key=True)
    player_type = Column(String, primary_key=True)
    total_revenue = Column(Float)
    avg_purchase = Column(Float)
    unique_purchasers = Column(BigInteger)

class RealtimeConcurrency(Base):
    __tablename__ = "realtime_concurrency"
    window_start = Column(DateTime, primary_key=True)
    window_end = Column(DateTime)
    game_name = Column(String, primary_key=True)
    region = Column(String, primary_key=True)
    concurrent_players = Column(BigInteger)

class RealtimePerformance(Base):
    __tablename__ = "realtime_performance"
    window_start = Column(DateTime, primary_key=True)
    window_end = Column(DateTime)
    game_name = Column(String, primary_key=True)
    platform = Column(String, primary_key=True)
    region = Column(String, primary_key=True)
    avg_fps = Column(Float)
    avg_latency = Column(Float)
