import streamlit as st
import pandas as pd
import requests
import os
import time
import plotly.express as px
from game_library import get_games, get_game_by_id

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
st.set_page_config(page_title="Game Analytics Dashboard", layout="wide")

# --- STATE MANAGEMENT ---
if "selected_game_id" not in st.session_state:
    st.session_state.selected_game_id = None

# --- SIDEBAR ---
st.sidebar.title("Navigation")
if st.sidebar.button("🏠 Home / Global Stats"):
    st.session_state.selected_game_id = None

st.sidebar.header("Connection Status")
try:
    health = requests.get(f"{API_URL}/health", timeout=1).json()
    st.sidebar.success(f"Core Backend: {health.get('status', 'Unknown')}")
except Exception:
    st.sidebar.error("Core Backend Offline")

# --- MAIN CONTENT ---

# 1. GLOBAL VIEW
if st.session_state.selected_game_id is None:
    st.title("🎮 Game Analytics: Global Overview")
    
    st.markdown("### Monitor all your games in one place")
    
    games = get_games()
    
    # Display Grid of Games
    cols = st.columns(3)
    for idx, game in enumerate(games):
        with cols[idx % 3]:
            # Card-like container
            with st.container(border=True):
                st.image(game["cover_url"], use_container_width=True)
                st.subheader(game["name"])
                st.write(f"**Tags:** {', '.join(game['tags'])}")
                if st.button(f"View Analytics ->", key=f"btn_{game['id']}"):
                    st.session_state.selected_game_id = game["id"]
                    st.rerun()

    st.divider()
    st.subheader("Aggregate Performance (Mock)")
    # Mock Global Chart
    mock_data = pd.DataFrame([
        {"game": g["name"], "players": len(g["name"])*1200} for g in games
    ])
    fig = px.bar(mock_data, x="game", y="players", title="Real-Time Active Players per Game")
    st.plotly_chart(fig, use_container_width=True)

# 2. SPECIFIC GAME DETAIL VIEW
else:
    game_id = st.session_state.selected_game_id
    game = get_game_by_id(game_id)
    
    if not game:
        st.error("Game not found!")
        st.stop()
        
    st.button("<- Back to Home", on_click=lambda: st.session_state.update(selected_game_id=None))
    
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image(game["cover_url"], use_container_width=True)
    with col2:
        st.title(game["name"])
        st.write(f"**Tags:** {', '.join(game['tags'])}")
        st.info("Live Incoming Telemetry...")

    # Mock Data for this game
    # In real app: fetch from API: requests.get(f"{API_URL}/metrics/{game_id}")
    
    st.markdown("### 📊 Key Metrics")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Active Players", "12,450", "+5%")
    m2.metric("Avg Sentiment", "0.85", "+0.02")
    m3.metric("Total Revenue (1h)", "$4,200", "120 tx")
    m4.metric("Avg Playtime", "2.4h")

    st.markdown("### 📝 Recent Reviews")
    reviews = [
        {"user": "PlayerOne", "text": "Amazing update!", "score": 0.9, "playtime": "450h"},
        {"user": "NoobMaster", "text": "Too hard.", "score": 0.3, "playtime": "2h"},
        {"user": "Critic99", "text": "Balanced gameplay.", "score": 0.8, "playtime": "120h"},
    ]
    for r in reviews:
        with st.expander(f"{r['user']} (Playtime: {r['playtime']}) - Sentiment: {r['score']}"):
            st.write(r['text'])
            
    st.markdown("### 💰 Purchase Activity")
    st.line_chart([10, 20, 15, 30, 45, 20, 60])