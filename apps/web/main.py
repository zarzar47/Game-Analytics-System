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
                'active_users': 0,
                'total_revenue': 0.0,
                'avg_fps': 60.0,
                'avg_latency': 50.0,
                'sentiment': 0.8,
                'events_processed': 0,
                'recent_purchases': [], # New: Store {item, amount}
                'player_archetypes': {}, # New: Count of CASUAL, WHALE etc.
                'events_by_type': {}, # New: Count of heartbeats, reviews etc.
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
            
            # Increment event type count
            m['events_by_type'][etype] = m['events_by_type'].get(etype, 0) + 1

            # Increment player archetype count (if present)
            ptype = event.get('player_type')
            if ptype:
                m['player_archetypes'][ptype] = m['player_archetypes'].get(ptype, 0) + 1

            # 1. System Status (Concurrency)
            if etype == 'status':
                m['active_users'] = event.get('player_count', 0)
            
            # 2. Financials (Revenue)
            elif etype == 'purchase':
                amount = event.get('purchase_amount', 0.0)
                item = event.get('item_id', 'Unknown Item')
                m['total_revenue'] += amount
                m['recent_purchases'].insert(0, {'item': item, 'amount': amount})
                m['recent_purchases'] = m['recent_purchases'][:5] # Keep last 5
                self._add_log(f"💰 PURCHASE: ${amount} for {item} in {event.get('game_name')}")
            
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
    k1.metric("Live Players", f"{metrics['active_users']:,}")
    k2.metric("Revenue (Session)", f"${metrics['total_revenue']:,.2f}")
    k3.metric("Avg Latency", f"{int(metrics['avg_latency'])} ms", delta_color="inverse")
    k4.metric("Sentiment", f"{metrics['sentiment']:.2f}")

    st.divider()

    # Detailed Analytics
    c1, c2 = st.columns((1, 1))

    with c1:
        st.subheader("Player Archetypes")
        if metrics['player_archetypes']:
            # Create a DataFrame for the pie chart
            archetype_df = pd.DataFrame(
                metrics['player_archetypes'].items(), 
                columns=['Archetype', 'Count']
            )
            st.bar_chart(archetype_df, x='Archetype', y='Count', color="#00aaff")
        else:
            st.caption("No archetype data yet.")

        st.subheader("Recent Purchases")
        if metrics['recent_purchases']:
            purchase_df = pd.DataFrame(metrics['recent_purchases'])
            st.dataframe(purchase_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No purchases this session.")

    with c2:
        st.subheader("Real-Time Health Monitor")
        st.progress(min(1.0, metrics['avg_fps']/144.0), text=f"Avg. FPS: {metrics['avg_fps']:.1f}")
        
        # Color code latency
        lat = metrics['avg_latency']
        lat_progress = min(1.0, (200 - lat) / 200) # Inverse relationship
        st.progress(lat_progress, text=f"Avg. Latency: {lat:.0f}ms")

        st.subheader("Event Stream")
        if metrics['events_by_type']:
            events_df = pd.DataFrame(
                metrics['events_by_type'].items(),
                columns=['Event Type', 'Count']
            )
            st.bar_chart(events_df, x='Event Type', y='Count')
        else:
            st.caption("Waiting for events...")

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
