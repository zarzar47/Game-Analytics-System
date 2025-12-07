import streamlit as st
import pandas as pd
import requests
import os
import time
import json
import plotly.express as px
from kafka import KafkaConsumer
from game_library import get_games, get_game_by_id
from streamlit_autorefresh import st_autorefresh
import threading

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

st.set_page_config(page_title="Game Analytics Real-Time", layout="wide")

# --- KAFKA CONSUMER AND AGGREGATION ---
if 'game_metrics' not in st.session_state:
    st.session_state.game_metrics = {
        game['id']: {'player_count': 0, 'sentiment_score': 0.0, 'total_purchases': 0, 'purchase_amount': 0.0, 'event_count': 0}
        for game in get_games()
    }
    st.session_state.event_log = []

def consume_and_aggregate():
    consumer = None
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',  # Start from new messages
            enable_auto_commit=True,
            group_id='streamlit-dashboard-aggregator'
        )
        print("Streamlit Background Consumer Connected to Kafka")
        
        for message in consumer:
            data = message.value
            game_id = data.get("game_id")
            event_type = data.get("event_type")
            
            if game_id in st.session_state.game_metrics:
                metrics = st.session_state.game_metrics[game_id]
                metrics['event_count'] += 1 # General event counter for averaging
                
                if event_type == "status":
                    metrics['player_count'] = data.get("player_count", metrics['player_count'])
                    # Simple moving average for sentiment (consider more robust methods for real apps)
                    current_sentiment = data.get("sentiment_score")
                    if current_sentiment is not None:
                        metrics['sentiment_score'] = (metrics['sentiment_score'] * (metrics['event_count'] - 1) + current_sentiment) / metrics['event_count']
                
                elif event_type == "purchase":
                    metrics['total_purchases'] += 1
                    metrics['purchase_amount'] += data.get("purchase_amount", 0.0)
                
                # Add to event log
                log_entry = f"[{time.strftime('%H:%M:%S')}] {event_type.upper()} - {data.get('game_name', game_id)}: {json.dumps(data)}"
                st.session_state.event_log.insert(0, log_entry)
                st.session_state.event_log = st.session_state.event_log[:20] # Keep last 20
                
    except Exception as e:
        print(f"Background Kafka Consumer Error: {e}")
    finally:
        if consumer:
            consumer.close()

# Start consumer thread once
if 'consumer_thread_started' not in st.session_state:
    st.session_state.consumer_thread_started = True
    consumer_thread = threading.Thread(target=consume_and_aggregate, daemon=True)
    consumer_thread.start()
    print("Kafka consumer thread started.")

# --- AUTO-REFRESH UI ---
st_autorefresh(interval=3000, key="data_refresher") # Refresh every 3 seconds

# --- SIDEBAR ---
st.sidebar.title("Configuration")
try:
    health = requests.get(f"{API_URL}/health", timeout=1).json()
    st.sidebar.success(f"Core Backend: {health.get('status', 'Unknown')}")
except Exception:
    st.sidebar.error("Core Backend Offline")

# --- MAIN CONTENT ---
st.title("Game Analytics Real-Time Dashboard")
st.markdown("Global Overview (Live Data)")

games = get_games()

# Display Grid of Games
cols = st.columns(3)
for idx, game in enumerate(games):
    with cols[idx % 3]:
        with st.container(border=True):
            st.image(game["cover_url"], use_container_width=True)
            st.subheader(game["name"])
            st.write(f"Tags: {', '.join(game['tags'])}")
            
            # Display real-time metrics
            metrics = st.session_state.game_metrics.get(game['id'], {})
            st.metric("Players", f"{metrics.get('player_count', 0):,}")
            st.metric("Sentiment", f"{metrics.get('sentiment_score', 0.0):.2f}")
            st.metric("Purchases", f"{metrics.get('total_purchases', 0)} (${metrics.get('purchase_amount', 0.0):.2f})")
            
            # Add a button for game details (future expansion)
            if st.button(f"View Details ->", key=f"btn_{game['id']}"):
                st.session_state.selected_game_id = game["id"]
                # This reruns the script, could be expanded to a detail page
                st.rerun()

st.divider()

st.subheader("Recent Events Log")
log_placeholder = st.empty()
with log_placeholder.container():
    for log_entry in st.session_state.event_log:
        st.code(log_entry, language="json")

# Conditional rendering for game details (future feature)
if st.session_state.get("selected_game_id"):
    # This block would handle displaying a detail page, for now just a message
    st.info(f"Viewing details for {st.session_state.selected_game_id} (feature coming soon!)")