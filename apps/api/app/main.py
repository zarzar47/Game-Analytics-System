from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from .database import get_db
from sqlalchemy.ext.asyncio import AsyncSession

app = FastAPI(title="Game Analytics Core")

# --- DATA MODELS ---
class GameMetricIngest(BaseModel):
    game_id: int
    player_count: int
    sentiment_score: float

# --- ENDPOINTS ---
@app.get("/health")
def health_check():
    return {"status": "operational", "service": "api"}

@app.post("/internal/ingest", status_code=201)
async def ingest_metrics(payload: GameMetricIngest, db: AsyncSession = Depends(get_db)):
    """
    Internal endpoint for Faker Service (or real scrapers) to push data.
    """
    # Logic to save to DB goes here. For MVP, we just log it.
    print(f"🔥 INGEST: Game {payload.game_id} | Players: {payload.player_count}")
    return {"msg": "Data received"}
