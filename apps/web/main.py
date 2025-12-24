import streamlit as st
import pandas as pd
import requests
import os
import time
import json
import threading
import copy
import warnings
from kafka import KafkaConsumer
from game_library import get_games, get_game_by_id
from streamlit_autorefresh import st_autorefresh
from spark_db import get_revenue_data, get_concurrency_data, get_performance_data

# Suppress annoying deprecation warnings from Streamlit
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configuration
API_URL = os.getenv("API_URL", "http://api:8000")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "GameAnalytics"

st.set_page_config(page_title="Game Analytics | LiveOps", layout="wide", page_icon="🎮")

# --- DATA STORE (Decoupled Logic) ---
class GameDataStore:
    """
    A thread-safe store for REAL-TIME alerts and snapshots from Kafka.
    This store is optimized for 'Latest State' visualization and live event feeds.
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

def render_game_detail(game, metrics, revenue_df, concurrency_df, performance_df):
    st.title(f"📊 {game['name']} Analytics")
    
    # Top Level KPIs from Live Kafka Stream
    st.subheader("Live Status (from Kafka)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Live Players", f"{metrics['active_users']:,}")
    k2.metric("Revenue (Session)", f"${metrics['total_revenue']:,.2f}")
    k3.metric("Avg Latency", f"{int(metrics['avg_latency'])} ms", delta_color="inverse")
    k4.metric("Sentiment", f"{metrics['sentiment']:.2f}")

    st.divider()

    # Spark-Powered Analytics Section
    st.header("⚡ Spark-Powered Analytics (1-minutely)")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Revenue by Player Type")
        if not revenue_df.empty:
            game_revenue = revenue_df[revenue_df['game_name'] == game['name']]
            st.bar_chart(game_revenue, x='player_type', y='total_revenue', color='player_type')
        else:
            st.caption("Waiting for Spark revenue data...")

        st.subheader("Player Concurrency by Region")
        if not concurrency_df.empty:
            game_concurrency = concurrency_df[concurrency_df['game_name'] == game['name']]
            st.bar_chart(game_concurrency, x='region', y='concurrent_players', color='region')
        else:
            st.caption("Waiting for Spark concurrency data...")

    with c2:
        st.subheader("Performance by Platform & Region")
        if not performance_df.empty:
            game_perf = performance_df[performance_df['game_name'] == game['name']]
            # FPS Chart
            st.bar_chart(game_perf, x='platform', y='avg_fps', color='region')
             # Latency Chart
            st.bar_chart(game_perf, x='platform', y='avg_latency', color='region')
        else:
            st.caption("Waiting for Spark performance data...")

    st.divider()
    
    # Existing detailed analytics from Kafka
    st.header("Live Feed Details (from Kafka)")
    c1, c2 = st.columns((1, 1))
    with c1:
        st.subheader("Recent Purchases")
        if metrics['recent_purchases']:
            purchase_df = pd.DataFrame(metrics['recent_purchases'])
            st.dataframe(purchase_df, use_container_width=True, hide_index=True)
        else:
            st.caption("No purchases this session.")

    with c2:
        st.subheader("Event Stream")
        if metrics['events_by_type']:
            events_df = pd.DataFrame(
                metrics['events_by_type'].items(),
                columns=['Event Type', 'Count']
            )
            st.bar_chart(events_df, x='Event Type', y='Count')
        else:
            st.caption("Waiting for events...")


# --- MAIN APP LAYOUT ---
st_autorefresh(interval=2000, key="ui_refresh") # 2s Refresh Rate

if 'view' not in st.session_state: st.session_state.view = 'dashboard'
if 'selected_game' not in st.session_state: st.session_state.selected_game = None

# Get latest data from both sources
kafka_metrics, logs = store.get_view_data()
spark_revenue = get_revenue_data()
spark_concurrency = get_concurrency_data()
spark_performance = get_performance_data()


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
    
    games = get_games()
    cols = st.columns(len(games))
    
    for idx, game in enumerate(games):
        m = kafka_metrics.get(game['id'], {})
        with cols[idx]:
            with st.container(border=True):
                st.image(game['cover_url'], use_container_width=True)
                st.markdown(f"**{game['name']}**")
                
                # Default values from Kafka
                total_players = m.get('active_users', 0)
                total_rev = int(m.get('total_revenue', 0))

                # Show concurrency from Spark if available and not empty
                if not spark_concurrency.empty:
                    game_concurrency = spark_concurrency[spark_concurrency['game_name'] == game['name']]
                    if not game_concurrency.empty:
                        total_players = int(game_concurrency['concurrent_players'].sum())
                
                # Show revenue from Spark if available and not empty
                if not spark_revenue.empty:
                    game_revenue = spark_revenue[spark_revenue['game_name'] == game['name']]
                    if not game_revenue.empty:
                        total_rev = int(game_revenue['total_revenue'].sum())

                st.write(f"👥 {total_players} Active")
                st.write(f"💰 ${total_rev}")
                
                if st.button("View Details", key=f"btn_{game['id']}"):
                    st.session_state.selected_game = game
                    st.session_state.view = 'detail'
                    st.rerun()

elif st.session_state.view == 'detail':
    if st.button("← Back to Global"):
        st.session_state.view = 'dashboard'
        st.rerun()
        
    game = st.session_state.selected_game
    render_game_detail(game, kafka_metrics[game['id']], spark_revenue, spark_concurrency, spark_performance)
