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

st.set_page_config(page_title="Game Analytics | LiveOps", layout="wide", page_icon="🎮")

# --- DATA STORE (Decoupled Logic) ---
class GameDataStore:
    """
    A thread-safe store for REAL-TIME alerts and snapshots.
    NOTE: For heavy historical analytics, this should query a Spark/Data Warehouse backend.
    This store is optimized for 'Latest State' visualization.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.games = get_games()
        # Schema: { game_id: { ...metrics... } }
        self.metrics = {
            game['id']: {
                'active_users': 0,      # From 'status' events
                'total_revenue': 0.0,   # From 'purchase' events
                'avg_fps': 60.0,        # From 'heartbeat' events (rolling avg)
                'avg_latency': 50.0,    # From 'heartbeat' events (rolling avg)
                'sentiment': 0.8,       # From 'review' events
                'events_processed': 0
            }
            for game in self.games
        }
        self.recent_logs = []
        self._running = False

    def update(self, event):
        """Process incoming Kafka event and update in-memory state."""
        with self.lock:
            gid = event.get('game_id')
            etype = event.get('event_type')
            
            if gid not in self.metrics: return
            
            m = self.metrics[gid]
            m['events_processed'] += 1
            
            # 1. System Status (Concurrency)
            if etype == 'status':
                m['active_users'] = event.get('player_count', 0)
            
            # 2. Financials (Revenue)
            elif etype == 'purchase':
                m['total_revenue'] += event.get('purchase_amount', 0.0)
                self._add_log(f"💰 PURCHASE: ${event.get('purchase_amount')} in {event.get('game_name')}")
            
            # 3. Performance (Heartbeats) - Simple Exponential Moving Average
            elif etype == 'heartbeat':
                alpha = 0.1 # Smoothing factor
                new_fps = event.get('fps', 60)
                new_lat = event.get('latency_ms', 50)
                m['avg_fps'] = (m['avg_fps'] * (1-alpha)) + (new_fps * alpha)
                m['avg_latency'] = (m['avg_latency'] * (1-alpha)) + (new_lat * alpha)
                
            # 4. Sentiment (Reviews)
            elif etype == 'review':
                s = event.get('sentiment_score', 0.5)
                m['sentiment'] = (m['sentiment'] * 0.9) + (s * 0.1) # Slowly shift sentiment

            # Log significant events
            if etype in ['session_start', 'level_up', 'review']:
                user = event.get('player_id', 'Unknown')[-4:]
                self._add_log(f"{etype.upper()}: User-{user} in {event.get('game_name')}")

    def _add_log(self, msg):
        self.recent_logs.insert(0, f"[{time.strftime('%H:%M:%S')}] {msg}")
        self.recent_logs = self.recent_logs[:15] # Keep last 15

    def get_view_data(self):
        with self.lock:
            return copy.deepcopy(self.metrics), list(self.recent_logs)

    def start(self):
        if not self._running:
            self._running = True
            threading.Thread(target=self._kafka_listener, daemon=True).start()

    def _kafka_listener(self):
        # Retry logic for container startup
        while self._running:
            try:
                consumer = KafkaConsumer(
                    TOPIC_NAME,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='latest'
                )
                print("✅ Frontend connected to Kafka Stream")
                for msg in consumer:
                    if not self._running: break
                    self.update(msg.value)
            except Exception:
                time.sleep(2)

@st.cache_resource
def get_global_store():
    store = GameDataStore()
    store.start()
    return store

store = get_global_store()

# --- UI COMPONENTS ---

def render_kpi_card(label, value, delta=None, color="normal"):
    st.metric(label=label, value=value, delta=delta)

def render_game_detail(game, metrics):
    st.title(f"📊 {game['name']} Analytics")
    
    # Top Level KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Live Players", f"{metrics['active_users']:,}", delta_color="normal")
    k2.metric("Revenue (Session)", f"${metrics['total_revenue']:,.2f}", delta="Real-time")
    k3.metric("Avg Latency", f"{int(metrics['avg_latency'])} ms", delta="-2ms" if metrics['avg_latency'] < 50 else "+10ms", delta_color="inverse")
    k4.metric("Sentiment", f"{metrics['sentiment']:.2f}", delta="Stable")

    st.divider()

    # Real-Time Health Monitor
    st.subheader("🟢 Real-Time Health Monitor")
    c1, c2 = st.columns(2)
    
    with c1:
        st.info(f"**Avg FPS:** {metrics['avg_fps']:.1f}")
        st.progress(min(1.0, metrics['avg_fps']/144.0), text="Frame Rate Stability")
        
    with c2:
        # Color code latency
        lat = metrics['avg_latency']
        state = "success" if lat < 60 else "warning" if lat < 120 else "error"
        st.metric("Network Status", "Healthy" if lat < 100 else "Congested")

    # Placeholder for Spark integration
    st.warning("⚠️ Deep Analytics (Retention, LTV) require the Spark Processing Engine (Coming Soon).")

# --- MAIN APP LAYOUT ---

st_autorefresh(interval=1000, key="ui_refresh") # 1s Refresh Rate

if 'view' not in st.session_state: st.session_state.view = 'dashboard'
if 'selected_game' not in st.session_state: st.session_state.selected_game = None

metrics, logs = store.get_view_data()

# SIDEBAR
with st.sidebar:
    st.header("🎮 Operations Center")
    st.write("Stream Status: **ONLINE**")
    st.divider()
    st.subheader("Live Feed")
    for log in logs:
        st.caption(log)

# PAGE ROUTING
if st.session_state.view == 'dashboard':
    st.title("🌍 Global Operations Dashboard")
    
    # Game Grid
    games = get_games()
    cols = st.columns(len(games))
    
    for idx, game in enumerate(games):
        m = metrics.get(game['id'], {})
        with cols[idx]:
            with st.container(border=True):
                st.image(game['cover_url'], use_container_width=True)
                st.markdown(f"**{game['name']}**")
                st.write(f"👥 {m.get('active_users', 0)} Active")
                st.write(f"💰 ${int(m.get('total_revenue', 0))}")
                
                if st.button("View Details", key=f"btn_{game['id']}"):
                    st.session_state.selected_game = game
                    st.session_state.view = 'detail'
                    st.rerun()

elif st.session_state.view == 'detail':
    if st.button("← Back to Global"):
        st.session_state.view = 'dashboard'
        st.rerun()
        
    render_game_detail(st.session_state.selected_game, metrics[st.session_state.selected_game['id']])
