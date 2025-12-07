import streamlit as st
import pandas as pd
import requests
import os
import time
import json
import plotly.express as px
from kafka import KafkaConsumer
from game_library import get_games, get_game_by_id

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

st.set_page_config(page_title="Game Analytics Real-Time", layout="wide")

# --- KAFKA CONSUMER SETUP ---
# We cache the consumer to avoid reconnecting on every script rerun (if that happens)
# However, for a streaming loop, we won't strictly rerun the script often.

@st.cache_resource
def get_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest', # Start from new messages
            enable_auto_commit=True,
            group_id='streamlit-dashboard-group'
        )
        print("✅ Streamlit Connected to Kafka")
        return consumer
    except Exception as e:
        print(f"❌ Kafka Connection Error: {e}")
        return None

# --- SIDEBAR ---
st.sidebar.title("Navigation")
view_mode = st.sidebar.radio("View Mode", ["Live Stream", "Global Overview (Static)", "Game Details"])

if view_mode == "Live Stream":
    st.title("⚡ Real-Time Data Stream")
    st.markdown("Listening to `GameAnalytics` Kafka topic...")
    
    # Placeholders for metrics
    col1, col2, col3 = st.columns(3)
    p1 = col1.empty()
    p2 = col2.empty()
    p3 = col3.empty()
    
    st.divider()
    
    # Placeholder for the log
    log_container = st.container()
    
    if st.button("Start Streaming"):
        consumer = get_consumer()
        if not consumer:
            st.error("Could not connect to Kafka broker.")
            st.stop()
            
        st.success("Streaming started... (Stop via Stop button usually, but here just refresh)")
        
        # Buffer for log
        log_data = []
        
        for message in consumer:
            data = message.value
            event_type = data.get("event_type")
            game_name = data.get("game_name")
            
            # Update metrics based on type
            if event_type == "status":
                p1.metric(f"Players: {game_name}", data.get("player_count"))
                p2.metric(f"Sentiment: {game_name}", data.get("sentiment_score"))
            
            elif event_type == "purchase":
                p3.metric(f"New Purchase: {game_name}", f"${data.get('purchase_amount')}")

            # Update Log
            log_entry = f"[{time.strftime('%H:%M:%S')}] {event_type.upper()} - {game_name}: {json.dumps(data)}"
            log_data.insert(0, log_entry)
            log_data = log_data[:10] # Keep last 10
            
            with log_container:
                # We clear and redraw the log
                log_container.empty()
                for line in log_data:
                    st.code(line, language="text")
                    
            # In a real app, you might want a break condition or non-blocking loop
            # Streamlit loops block the UI, but updates are pushed.

elif view_mode == "Global Overview (Static)":
    st.title("🎮 Global Overview")
    games = get_games()
    for game in games:
        st.write(f"**{game['name']}** - {', '.join(game['tags'])}")
    st.info("Switch to 'Live Stream' to see incoming data.")

elif view_mode == "Game Details":
    st.title("Game Details")
    st.write("Select a game from the sidebar (Not implemented in this demo mode)")

