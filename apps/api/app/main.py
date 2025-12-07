from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from .database import get_db
from sqlalchemy.ext.asyncio import AsyncSession
from game_library import get_games, get_game_by_id

app = FastAPI(title="Game Analytics Core")

# --- DATA MODELS ---
class GameEvent(BaseModel):
    game_id: str
    event_type: str  # "status", "review", "purchase"
    player_count: Optional[int] = None
    sentiment_score: Optional[float] = None
    review_text: Optional[str] = None
    playtime_session: Optional[float] = None # Hours
    playtime_total: Optional[float] = None # Hours
    purchase_amount: Optional[float] = None
    timestamp: Optional[float] = None

class GameInfo(BaseModel):
    id: str
    name: str
    cover_url: str
    tags: List[str]

# --- ENDPOINTS ---
@app.get("/health")
def health_check():
    return {"status": "operational", "service": "api"}

@app.get("/games", response_model=List[GameInfo])
def list_games():
    """Return static list of games from shared library."""
    return get_games()

@app.post("/internal/ingest", status_code=201)
async def ingest_metrics(payload: GameEvent, db: AsyncSession = Depends(get_db)):
    """
    Internal endpoint for Faker Service (or real scrapers) to push data.
    """
    # Logic to save to DB goes here. For MVP, we just log it.
    print(f"🔥 INGEST [{payload.event_type}]: Game {payload.game_id} | Players: {payload.player_count} | Sentiment: {payload.sentiment_score}")
    return {"msg": "Data received"}