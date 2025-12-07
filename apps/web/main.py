import streamlit as st
import pandas as pd
import requests
import os
import time
import plotly.express as px

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
st.set_page_config(page_title="Game Analytics Dashboard", layout="wide")

st.title("🎮 Game Analytics Real-Time Dashboard")

# Sidebar
st.sidebar.header("Connection Status")
try:
    health = requests.get(f"{API_URL}/health", timeout=2).json()
    st.sidebar.success(f"Core Backend: {health.get('status', 'Unknown')}")
except Exception as e:
    st.sidebar.error(f"Core Backend Offline: {e}")

# Main Content
st.markdown("""
This dashboard visualizes real-time game telemetry data ingested by the Core Backend.
""")

# Placeholder for real-time data
st.subheader("Real-Time Telemetry")

# NOTE: In a real app, you'd fetch from a proper endpoint. 
# Since we only have an ingest endpoint implemented so far, 
# we will mock the display or you would add a GET endpoint to `apps/api` to retrieve data.
# For now, let's visualize what the system *would* show.

if st.button("Refresh Data"):
    # This is where you would call: requests.get(f"{API_URL}/metrics")
    st.info("Fetching latest metrics from Core Backend...")
    # Mocking response for visualization
    data = [
        {"game_id": 101, "players": 12050, "sentiment": 0.85},
        {"game_id": 102, "players": 4500, "sentiment": 0.45},
        {"game_id": 2077, "players": 89000, "sentiment": 0.92},
    ]
    df = pd.DataFrame(data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("### Active Players")
        fig_players = px.bar(df, x="game_id", y="players", color="game_id")
        st.plotly_chart(fig_players, use_container_width=True)
        
    with col2:
        st.write("### Sentiment Score")
        fig_sent = px.scatter(df, x="game_id", y="sentiment", size="players", color="game_id")
        st.plotly_chart(fig_sent, use_container_width=True)

st.write("---")
st.caption("Powered by Streamlit & FastAPI")
