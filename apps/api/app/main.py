from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from .database import get_db, AsyncSessionLocal
from sqlalchemy.ext.asyncio import AsyncSession
from game_library import get_games
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import os

app = FastAPI(title="Game Analytics Core")

# --- KAFKA CONFIG ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

# --- DATA MODELS ---
class GameEvent(BaseModel):
    game_id: str
    event_type: str
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
            # HERE: Save to Database logic
            # For now, we just log that we would have saved it
            # async with AsyncSessionLocal() as session:
            #     ... save to db ...
            print(f"DB STORE: {payload.get('event_type')} for {payload.get('game_id')}")
    except Exception as e:
        print(f"Kafka Consumer Error: {e}")
    finally:
        await consumer.stop()

@app.on_event("startup")
async def startup_event():
    # Run consumer in background
    asyncio.create_task(consume_kafka())

# --- ENDPOINTS ---
@app.get("/health")
def health_check():
    return {"status": "operational", "service": "api"}

@app.get("/games", response_model=List[GameInfo])
def list_games():
    return get_games()

@app.post("/internal/ingest", status_code=201)
async def ingest_metrics(payload: GameEvent, db: AsyncSession = Depends(get_db)):
    """
    Legacy HTTP endpoint.
    """
    print(f"HTTP INGEST: {payload.game_id}")
    return {"msg": "Data received"}
