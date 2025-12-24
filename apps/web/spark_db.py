import os
import pandas as pd
import psycopg2
import streamlit as st
from sqlalchemy import create_engine

# --- DATABASE CONNECTION ---

# Use Streamlit's connection management to cache the connection engine
@st.cache_resource
def get_db_engine():
    """Establishes and returns a SQLAlchemy engine for the PostgreSQL database."""
    try:
        # postgresql+psycopg2://user:password@host:port/dbname
        db_url = "postgresql+psycopg2://rafay:rafay@db:5432/game_analytics"
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        st.error(f"🔥 Could not create database engine: {e}")
        return None

# --- DATA FETCHING FUNCTIONS ---

@st.cache_data(ttl=60) # Cache data for 60 seconds
def get_revenue_data():
    """Fetches real-time revenue analytics from the Spark-processed table."""
    engine = get_db_engine()
    if engine:
        try:
            # Query for the latest window of data for each game/player_type combo
            query = """
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, player_type ORDER BY window_start DESC) as rn
                    FROM realtime_revenue
                ) AS ranked
                WHERE rn = 1;
            """
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.warning(f"Could not query revenue data. Is the Spark Processor running? Error: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=60)
def get_concurrency_data():
    """Fetches real-time player concurrency from the Spark-processed table."""
    engine = get_db_engine()
    if engine:
        try:
            # Query for the latest window of data for each game/region
            query = """
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, region ORDER BY window_start DESC) as rn
                    FROM realtime_concurrency
                ) AS ranked
                WHERE rn = 1;
            """
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.warning(f"Could not query concurrency data: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=60)
def get_performance_data():
    """Fetches real-time game performance from the Spark-processed table."""
    engine = get_db_engine()
    if engine:
        try:
            # Query for the latest window of data
            query = """
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_name, platform, region ORDER BY window_start DESC) as rn
                    FROM realtime_performance
                ) AS ranked
                WHERE rn = 1;
            """
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.warning(f"Could not query performance data: {e}")
    return pd.DataFrame()
