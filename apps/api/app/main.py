from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from .database import get_db, AsyncSessionLocal, engine, Base
from .models import GameMetric, RealtimeRevenue, RealtimeConcurrency, RealtimePerformance
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.sql import func
from game_library import get_games
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import os
import datetime

app = FastAPI(title="Game Analytics Core")

# --- KAFKA CONFIG ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

# --- DATA MODELS ---
class GameEvent(BaseModel):
    game_id: str
    event_type: str
    game_name: Optional[str] = None
    player_count: Optional[int] = None
    sentiment_score: Optional[float] = None
    review_text: Optional[str] = None
    playtime_session: Optional[float] = None
    playtime_total: Optional[float] = None
    purchase_amount: Optional[float] = None
    timestamp: Optional[float] = None

class GameInfo(BaseModel):
    id: str
    name: str
    cover_url: str
    tags: List[str]

class HistoricalMetric(BaseModel):
    timestamp: datetime.datetime
    player_count: Optional[int]
    sentiment_score: Optional[float]
    purchase_amount: Optional[float]

# --- BACKGROUND TASK ---
async def consume_kafka():
    """Background task to consume Kafka messages and save to DB."""
    print("Starting Kafka Consumer Background Task...")
    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="api-db-worker",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    try:
        await consumer.start()
        print("API Consumer Connected to Kafka")
        async for msg in consumer:
            payload = msg.value
            
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    # Parse timestamp
                    ts = None
                    if payload.get("timestamp"):
                        ts = datetime.datetime.fromtimestamp(payload["timestamp"])

                    new_metric = GameMetric(
                        # Base
                        game_id=payload.get("game_id"),
                        game_name=payload.get("game_name"),
                        event_type=payload.get("event_type"),
                        timestamp=ts,
                        
                        # Player & Session
                        player_id=payload.get("player_id"),
                        session_id=payload.get("session_id"),
                        match_id=payload.get("match_id"),
                        region=payload.get("region"),
                        platform=payload.get("platform"),
                        player_type=payload.get("player_type"),
                        country=payload.get("country"),

                        # Gameplay & Performance
                        player_count=payload.get("player_count"),
                        sentiment_score=payload.get("sentiment_score"),
                        review_text=payload.get("review_text"),
                        level=payload.get("level"),
                        fps=payload.get("fps"),
                        latency_ms=payload.get("latency_ms"),

                        # Monetization
                        purchase_amount=payload.get("purchase_amount"),
                        currency=payload.get("currency"),
                        item_id=payload.get("item_id"),

                        # Legacy - not in faker, but in model
                        playtime_session=payload.get("playtime_session"),
                        playtime_total=payload.get("playtime_total"),
                    )
                    print(f'Added new metric to session {new_metric}')
                    session.add(new_metric)
                # Commit happens automatically on exit of session.begin() block if no error
            
            print(f"DB STORE: {payload.get('event_type')} for {payload.get('game_id')}")
    except Exception as e:
        print(f"Kafka Consumer Error: {e}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    # Create Tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    # Run consumer in background
    asyncio.create_task(consume_kafka())

# --- ENDPOINTS ---
@app.get("/health")
def health_check():
    return {"status": "operational", "service": "api"}

@app.get("/games", response_model=List[GameInfo])
def list_games():
    return get_games()

@app.get("/games/{game_id}/history", response_model=List[HistoricalMetric])
async def get_game_history(
    game_id: str, 
    hours: int = 24, 
    db: AsyncSession = Depends(get_db)
):
    """
    Fetch historical data for charts. 
    Currently returns raw data points for player_count and sentiment.
    """
    cutoff = datetime.datetime.now() - datetime.timedelta(hours=hours)
    
    # We want status updates mainly for the charts
    query = (
        select(GameMetric)
        .where(GameMetric.game_id == game_id)
        .where(GameMetric.timestamp >= cutoff)
        .where(GameMetric.event_type == "status")
        .order_by(GameMetric.timestamp.asc())
    )
    
    result = await db.execute(query)
    records = result.scalars().all()
    
    return [
        HistoricalMetric(
            timestamp=r.timestamp,
            player_count=r.player_count,
            sentiment_score=r.sentiment_score,
            purchase_amount=r.purchase_amount
        )
        for r in records
    ]

@app.get("/analytics/revenue")
async def get_spark_revenue(db: AsyncSession = Depends(get_db)):
    # 1. Wrap the string in text()
    query = text("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, player_type ORDER BY window_start DESC) as rn
            FROM realtime_revenue
        ) AS ranked
        WHERE rn = 1;
    """)
    
    try:
        result = await db.execute(query)
        # 2. Convert result rows to dictionaries for JSON response
        return [dict(row._mapping) for row in result]
    except Exception as e:
        if "does not exist" in str(e):
             raise HTTPException(status_code=404, detail=f"Table realtime_revenue not found. Spark job may not have run yet.")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/concurrency")
async def get_spark_concurrency(db: AsyncSession = Depends(get_db)):
    query = text("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, region ORDER BY window_start DESC) as rn
            FROM realtime_concurrency
        ) AS ranked
        WHERE rn = 1;
    """)
    try:
        result = await db.execute(query)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        if "does not exist" in str(e):
             raise HTTPException(status_code=404, detail=f"Table realtime_concurrency not found. Spark job may not have run yet.")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/analytics/performance")
async def get_spark_performance(db: AsyncSession = Depends(get_db)):
    query = text("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, platform, region ORDER BY window_start DESC) as rn
            FROM realtime_performance
        ) AS ranked
        WHERE rn = 1;
    """)
    try:
        result = await db.execute(query)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        if "does not exist" in str(e):
             raise HTTPException(status_code=404, detail=f"Table realtime_performance not found. Spark job may not have run yet.")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/internal/ingest", status_code=201)
async def ingest_metrics(payload: GameEvent, db: AsyncSession = Depends(get_db)):
    """
    Legacy HTTP endpoint.
    """
    print(f"HTTP INGEST: {payload.game_id}")
    return {"msg": "Data received"}