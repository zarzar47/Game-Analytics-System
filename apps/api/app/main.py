import redis.asyncio as redis
import os
import json
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from .database import get_db, AsyncSessionLocal
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from game_library import get_games
import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- REDIS SETUP ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_pool = redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Game Analytics API - Star Schema")

# --- DATA MODELS ---

class GameInfo(BaseModel):
    id: str
    name: str
    cover_url: str
    tags: List[str]

class StarSchemaMetrics(BaseModel):
    """Aggregated metrics from star schema"""
    game_name: str
    total_players: int
    total_revenue: float
    avg_session_duration: float
    total_sessions: int
    avg_fps: Optional[float] = None
    avg_latency: Optional[float] = None

class PlayerSegmentRevenue(BaseModel):
    player_segment: str
    total_revenue: float
    avg_purchase: float
    purchase_count: int

class RegionalPerformance(BaseModel):
    region: str
    concurrent_players: int
    avg_fps: float
    avg_latency: float

class TimeSeriesData(BaseModel):
    timestamp: datetime.datetime
    metric_value: float

# --- HEALTH CHECK ---

@app.get("/health")
def health_check():
    return {
        "status": "operational", 
        "service": "star-schema-api",
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

# --- DEBUG ENDPOINTS ---

@app.get("/debug/cache-info")
async def cache_info():
    """Get information about cache status"""
    try:
        info = await redis_pool.info()
        keys = await redis_pool.keys("overview:*")
        return {
            "redis_connected": True,
            "total_keys": len(keys),
            "cached_games": [k.replace("overview:", "") for k in keys],
            "redis_version": info.get("redis_version", "unknown")
        }
    except Exception as e:
        return {"redis_connected": False, "error": str(e)}

@app.post("/debug/clear-cache")
async def clear_cache():
    """Clear all Redis caches"""
    try:
        keys = await redis_pool.keys("overview:*")
        if keys:
            await redis_pool.delete(*keys)
        return {"cleared": len(keys), "message": f"Cleared {len(keys)} cache entries"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cache clear failed: {str(e)}")

@app.get("/debug/database-stats")
async def database_stats(db: AsyncSession = Depends(get_db)):
    """Get database table row counts"""
    try:
        tables = [
            "dim_player", "dim_game", "dim_time", "dim_geography", "dim_item",
            "fact_session", "fact_transaction", "fact_telemetry"
        ]
        
        stats = {}
        for table in tables:
            result = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            stats[table] = count
        
        return {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "table_counts": stats,
            "total_records": sum(stats.values())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# --- GAME LIST ---

@app.get("/games", response_model=List[GameInfo])
def list_games():
    return get_games()

# ============================================================================
# STAR SCHEMA ANALYTICS ENDPOINTS (ENHANCED)
# ============================================================================

@app.get("/analytics/star-schema/overview/{game_id}")
async def get_game_overview(
    game_id: str, 
    db: AsyncSession = Depends(get_db),
    cache_ttl: int = 10,  # Changed default from 300 to 10
    skip_cache: bool = False  # New parameter to force fresh data
):
    """
    Get comprehensive overview for a game using star schema joins.
    NOW WITH ENHANCED LOGGING AND CACHE CONTROL
    """
    cache_key = f"overview:{game_id}"
    
    try:
        # 1. Check cache first (unless skip_cache is True)
        if not skip_cache:
            cached_result = await redis_pool.get(cache_key)
            if cached_result:
                logger.info(f" Cache HIT for game {game_id}")
                return json.loads(cached_result)
            else:
                logger.info(f" Cache MISS for game {game_id}")
        else:
            logger.info(f" Cache SKIPPED for game {game_id}")

        # 2. Query database
        logger.info(f"🔍 Querying database for game {game_id}")
        
        query = text("""
            WITH player_metrics AS (
                SELECT 
                    dg.game_name,
                    COUNT(DISTINCT fs.player_id) as total_players,
                    AVG(fs.duration_seconds) as avg_session_duration,
                    COUNT(fs.session_id) as total_sessions
                FROM fact_session fs
                JOIN dim_game dg ON fs.game_id = dg.game_id
                WHERE dg.game_id = :game_id
                GROUP BY dg.game_name
            ),
            revenue_metrics AS (
                SELECT 
                    SUM(ft.amount_usd) as total_revenue
                FROM fact_transaction ft
                WHERE ft.game_id = :game_id
            ),
            performance_metrics AS (
                SELECT 
                    AVG(ftel.fps) as avg_fps,
                    AVG(ftel.latency_ms) as avg_latency
                FROM fact_telemetry ftel
                WHERE ftel.game_id = :game_id
            )
            SELECT 
                COALESCE(pm.game_name, 'Unknown') as game_name,
                COALESCE(pm.total_players, 0) as total_players,
                COALESCE(rm.total_revenue, 0) as total_revenue,
                COALESCE(pm.avg_session_duration, 0) as avg_session_duration,
                COALESCE(pm.total_sessions, 0) as total_sessions,
                perf.avg_fps,
                perf.avg_latency
            FROM player_metrics pm
            CROSS JOIN revenue_metrics rm
            CROSS JOIN performance_metrics perf
        """)
        
        db_result = await db.execute(query, {"game_id": game_id})
        row = db_result.fetchone()
        
        if not row or row[1] == 0:  # No players
            logger.warning(f" No data found for game {game_id}")
            # Return empty state instead of 404
            result_model = StarSchemaMetrics(
                game_name="Unknown",
                total_players=0,
                total_revenue=0.0,
                avg_session_duration=0.0,
                total_sessions=0,
                avg_fps=None,
                avg_latency=None
            )
        else:
            result_model = StarSchemaMetrics(
                game_name=row[0],
                total_players=row[1] or 0,
                total_revenue=float(row[2] or 0),
                avg_session_duration=float(row[3] or 0),
                total_sessions=row[4] or 0,
                avg_fps=float(row[5]) if row[5] else None,
                avg_latency=float(row[6]) if row[6] else None
            )
            logger.info(f" Found data for game {game_id}: {row[1]} players, ${row[2]:.2f} revenue")
        
        # 3. Store result in cache
        if not skip_cache:
            await redis_pool.set(cache_key, result_model.model_dump_json(), ex=cache_ttl)
            logger.info(f" Cached data for game {game_id} (TTL: {cache_ttl}s)")
        
        return result_model

    except redis.RedisError as e:
        logger.error(f"Redis error for game {game_id}: {e}")
        # Continue without cache
        pass
    except Exception as e:
        logger.error(f"Database error for game {game_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/star-schema/revenue-by-segment/{game_id}")
async def get_revenue_by_player_segment(game_id: str, db: AsyncSession = Depends(get_db)):
    """Revenue breakdown by player segment"""
    query = text("""
        SELECT 
            dp.player_segment,
            SUM(ft.amount_usd) as total_revenue,
            AVG(ft.amount_usd) as avg_purchase,
            COUNT(ft.transaction_id) as purchase_count
        FROM fact_transaction ft
        JOIN dim_player dp ON ft.player_id = dp.player_id
        WHERE ft.game_id = :game_id
        GROUP BY dp.player_segment
        HAVING SUM(ft.amount_usd) > 0
        ORDER BY total_revenue DESC
    """)
    
    try:
        result = await db.execute(query, {"game_id": game_id})
        rows = result.fetchall()
        
        return [
            PlayerSegmentRevenue(
                player_segment=row[0],
                total_revenue=float(row[1]),
                avg_purchase=float(row[2]),
                purchase_count=row[3]
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching revenue by segment: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/star-schema/regional-performance/{game_id}")
async def get_regional_performance(game_id: str, db: AsyncSession = Depends(get_db)):
    """Performance metrics by region"""
    query = text("""
        SELECT 
            ftel.region,
            COUNT(DISTINCT fs.player_id) as concurrent_players,
            AVG(ftel.fps) as avg_fps,
            AVG(ftel.latency_ms) as avg_latency
        FROM fact_telemetry ftel
        JOIN fact_session fs ON ftel.session_id = fs.session_id
        WHERE ftel.game_id = :game_id
        GROUP BY ftel.region
        ORDER BY concurrent_players DESC
    """)
    
    try:
        result = await db.execute(query, {"game_id": game_id})
        rows = result.fetchall()
        
        return [
            RegionalPerformance(
                region=row[0],
                concurrent_players=row[1],
                avg_fps=float(row[2]) if row[2] else 0.0,
                avg_latency=float(row[3]) if row[3] else 0.0
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching regional performance: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/star-schema/revenue-timeline/{game_id}")
async def get_revenue_timeline(
    game_id: str, 
    hours: int = 24,
    db: AsyncSession = Depends(get_db)
):
    """Revenue over time using dim_time"""
    query = text("""
        SELECT 
            dt.timestamp,
            SUM(ft.amount_usd) as total_revenue
        FROM fact_transaction ft
        JOIN dim_time dt ON ft.time_id = dt.time_id
        WHERE ft.game_id = :game_id
        AND dt.timestamp >= NOW() - INTERVAL ':hours hours'
        GROUP BY dt.timestamp
        ORDER BY dt.timestamp ASC
    """)
    
    try:
        result = await db.execute(query, {"game_id": game_id, "hours": hours})
        rows = result.fetchall()
        
        return [
            TimeSeriesData(
                timestamp=row[0],
                metric_value=float(row[1])
            )
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/star-schema/top-spenders/{game_id}")
async def get_top_spenders(
    game_id: str,
    limit: int = 10,
    db: AsyncSession = Depends(get_db)
):
    """Top spending players"""
    query = text("""
        SELECT 
            dp.username,
            dp.player_segment,
            dp.region,
            dp.platform,
            COUNT(ft.transaction_id) as purchase_count,
            SUM(ft.amount_usd) as total_spent
        FROM fact_transaction ft
        JOIN dim_player dp ON ft.player_id = dp.player_id
        WHERE ft.game_id = :game_id
        GROUP BY dp.player_id, dp.username, dp.player_segment, dp.region, dp.platform
        ORDER BY total_spent DESC
        LIMIT :limit
    """)
    
    try:
        result = await db.execute(query, {"game_id": game_id, "limit": limit})
        rows = result.fetchall()
        
        return [
            {
                "username": row[0],
                "player_segment": row[1],
                "region": row[2],
                "platform": row[3],
                "purchase_count": row[4],
                "total_spent": float(row[5])
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/star-schema/weekend-vs-weekday/{game_id}")
async def get_weekend_vs_weekday_metrics(game_id: str, db: AsyncSession = Depends(get_db)):
    """Compare weekend vs weekday metrics"""
    query = text("""
        SELECT 
            CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as period_type,
            COUNT(DISTINCT fs.session_id) as total_sessions,
            AVG(fs.duration_seconds) as avg_session_duration,
            SUM(ft.amount_usd) as total_revenue
        FROM fact_session fs
        JOIN dim_time dt ON fs.start_time_id = dt.time_id
        LEFT JOIN fact_transaction ft ON fs.session_id = ft.session_id
        WHERE fs.game_id = :game_id
        GROUP BY dt.is_weekend
    """)
    
    try:
        result = await db.execute(query, {"game_id": game_id})
        rows = result.fetchall()
        
        return [
            {
                "period": row[0],
                "sessions": row[1],
                "avg_duration": float(row[2]) if row[2] else 0.0,
                "revenue": float(row[3]) if row[3] else 0.0
            }
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ============================================================================
# LEGACY SPARK ENDPOINTS
# ============================================================================

@app.get("/analytics/revenue")
async def get_spark_revenue(db: AsyncSession = Depends(get_db)):
    """Legacy Spark endpoint"""
    query = text("SELECT * FROM realtime_revenue ORDER BY window_start DESC LIMIT 100")
    try:
        result = await db.execute(query)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        return []

@app.get("/analytics/concurrency")
async def get_spark_concurrency(db: AsyncSession = Depends(get_db)):
    """Legacy Spark endpoint"""
    query = text("SELECT * FROM realtime_concurrency ORDER BY window_start DESC LIMIT 100")
    try:
        result = await db.execute(query)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        return []

@app.get("/analytics/performance")
async def get_spark_performance(db: AsyncSession = Depends(get_db)):
    """Legacy Spark endpoint"""
    query = text("SELECT * FROM realtime_performance ORDER BY window_start DESC LIMIT 100")
    try:
        result = await db.execute(query)
        return [dict(row._mapping) for row in result]
    except Exception as e:
        return []