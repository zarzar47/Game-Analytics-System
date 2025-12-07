from sqlalchemy import Column, Integer, String, Float, DateTime
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
