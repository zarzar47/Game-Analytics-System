import streamlit as st
import pandas as pd
import requests
import os
import time
import json
import threading
import copy
from kafka import KafkaConsumer
from game_library import get_games, get_game_by_id
from streamlit_autorefresh import st_autorefresh

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

st.set_page_config(page_title="Game Analytics Real-Time", layout="wide")

# --- GLOBAL DATA STORE (Thread-Safe) ---
class GameDataStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.games = get_games()
        # Initialize metrics for each game
        self.metrics = {
            game['id']: {
                'player_count': 0, 
                'sentiment_score': 0.0, 
                'total_purchases': 0, 
                'purchase_amount': 0.0, 
                'event_count': 0
            }
            for game in self.games
        }
        self.logs = []
        self._running = False
        self._thread = None

    def update(self, data):
        with self.lock:
            game_id = data.get("game_id")
            event_type = data.get("event_type")
            
            if game_id in self.metrics:
                m = self.metrics[game_id]
                m['event_count'] += 1
                
                if event_type == "status":
                    m['player_count'] = data.get("player_count", m['player_count'])
                    curr_sent = data.get("sentiment_score")
                    if curr_sent is not None:
                        m['sentiment_score'] = (m['sentiment_score'] * (m['event_count'] - 1) + curr_sent) / m['event_count']
                
                elif event_type == "purchase":
                    m['total_purchases'] += 1
                    m['purchase_amount'] += data.get("purchase_amount", 0.0)
            
            # Update Logs
            log_entry = f"[{time.strftime('%H:%M:%S')}] {event_type.upper()} - {data.get('game_name', game_id)}"
            self.logs.insert(0, log_entry)
            self.logs = self.logs[:20]

    def get_snapshot(self):
        with self.lock:
            return copy.deepcopy(self.metrics), list(self.logs)

    def start_consumer(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self):
        print("✅ Kafka Consumer Thread Started")
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='streamlit-dashboard-global'
            )
            for message in consumer:
                self.update(message.value)
        except Exception as e:
            print(f"❌ Kafka Consumer Failed: {e}")
            self._running = False

@st.cache_resource
def get_store():
    store = GameDataStore()
    store.start_consumer()
    return store

store = get_store()

# --- AUTO-REFRESH UI ---
st_autorefresh(interval=2000, key="data_refresher")

# --- STATE MANAGEMENT ---
if 'view_mode' not in st.session_state:
    st.session_state.view_mode = 'global' # 'global' or 'detail'
if 'selected_game_id' not in st.session_state:
    st.session_state.selected_game_id = None

# --- CSS STYLING ---
st.markdown(
    """
    <style>
    /* Card Container Styling */
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {
        border-radius: 10px;
    }
    
    /* Small Image Height for Compact Cards */
    div[data-testid="stImage"] > img {
        height: 100px !important; 
        width: 100% !important;
        object-fit: cover !important;
        border-radius: 5px;
        margin-bottom: 5px;
    }

    /* Metric Value Font Size */
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem !important;
    }
    
    /* Metric Label Font Size */
    div[data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
    }
    
    /* Compact Header */
    h3 {
        font-size: 1.1rem !important;
        padding-top: 0px !important;
        margin-bottom: 5px !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- MAIN UI ---
metrics_snapshot, logs_snapshot = store.get_snapshot()

# === VIEW 1: GLOBAL GRID ===
if st.session_state.view_mode == 'global':
    st.title("Game Analytics Real-Time Dashboard")
    
    games = get_games()
    
    # Use 4 columns instead of 3 to make cards smaller
    cols = st.columns(4)
    
    for idx, game in enumerate(games):
        with cols[idx % 4]:
            with st.container(border=True):
                # Header
                st.subheader(game["name"])
                
                # Image
                st.image(game["cover_url"], use_container_width=True)
                
                # Metrics
                m = metrics_snapshot.get(game['id'], {})
                
                # Compact Metrics
                c1, c2 = st.columns(2)
                c1.metric("Users", f"{m.get('player_count', 0):,}")
                c2.metric("Mood", f"{m.get('sentiment_score', 0.0):.2f}")
                
                # Action Button
                if st.button(f"Analytics", key=f"btn_{game['id']}", use_container_width=True):
                    st.session_state.selected_game_id = game['id']
                    st.session_state.view_mode = 'detail'
                    st.rerun()

    st.divider()
    st.subheader("Live Event Log")
    st.text_area("Latest Events", value="\n".join(logs_snapshot), height=150, disabled=True)

# === VIEW 2: DETAIL PAGE ===
elif st.session_state.view_mode == 'detail':
    game_id = st.session_state.selected_game_id
    game = get_game_by_id(game_id)
    m = metrics_snapshot.get(game_id, {})
    
    if st.button("← Back to Global View"):
        st.session_state.view_mode = 'global'
        st.session_state.selected_game_id = None
        st.rerun()
        
    st.title(f"{game['name']} - Analytics")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image(game["cover_url"], use_container_width=True)
        st.write(f"**Tags:** {', '.join(game['tags'])}")
        
    with col2:
        # Real-time Metrics Big Display
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Active Players", f"{m.get('player_count', 0):,}")
        m2.metric("Sentiment Score", f"{m.get('sentiment_score', 0.0):.2f}")
        m3.metric("Total Purchases", f"{m.get('total_purchases', 0)}")
        m4.metric("Revenue", f"${m.get('purchase_amount', 0.0):.2f}")
        
        st.markdown("### Real-Time Charts")
        # In a real implementation, we would keep a history list in the store for charting.
        # For now, we mock a chart based on current snapshot to show layout.
        
        # Mocking a history for visualization purposes since we only store snapshot
        chart_data = pd.DataFrame({
            "Time": pd.date_range(start="now", periods=10, freq="min"),
            "Players": [m.get('player_count', 0) * (1 + i*0.01) for i in range(10)]
        })
        st.line_chart(chart_data, x="Time", y="Players")
        
        st.info("Charts will populate with historical data in the next update.")

    st.markdown("### Recent Events for this Game")
    # Filter logs for this game
    game_logs = [l for l in logs_snapshot if game['name'] in l]
    st.text_area("Game Events", value="\n".join(game_logs) if game_logs else "No recent events.", height=200)
