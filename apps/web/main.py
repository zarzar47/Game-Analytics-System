import streamlit as st
import pandas as pd
import os
import requests
import plotly.express as px
import plotly.graph_objects as go
from game_library import get_games
from streamlit_autorefresh import st_autorefresh

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Game Analytics | Star Schema",
    layout="wide",
    page_icon="🎮"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data(ttl=10)
def fetch_star_schema_overview(game_id: str):
    """Fetch comprehensive overview from star schema"""
    try:
        response = requests.get(f"{API_URL}/analytics/star-schema/overview/{game_id}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching overview: {e}")
        return None

@st.cache_data(ttl=10)
def fetch_revenue_by_segment(game_id: str):
    """Fetch revenue breakdown by player segment"""
    try:
        response = requests.get(f"{API_URL}/analytics/star-schema/revenue-by-segment/{game_id}")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.warning(f"No revenue data yet: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_regional_performance(game_id: str):
    """Fetch performance metrics by region"""
    try:
        response = requests.get(f"{API_URL}/analytics/star-schema/regional-performance/{game_id}")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.warning(f"No performance data yet: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_top_spenders(game_id: str):
    """Fetch top spending players"""
    try:
        response = requests.get(f"{API_URL}/analytics/star-schema/top-spenders/{game_id}?limit=10")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.warning(f"No spender data yet: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_weekend_metrics(game_id: str):
    """Fetch weekend vs weekday comparison"""
    try:
        response = requests.get(f"{API_URL}/analytics/star-schema/weekend-vs-weekday/{game_id}")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        st.warning(f"No temporal data yet: {e}")
        return pd.DataFrame()

# ============================================================================
# GAME DETAIL VIEW
# ============================================================================

def render_game_detail(game):
    st.title(f"{game['name']} - Star Schema Analytics")
    
    # Fetch all data
    overview = fetch_star_schema_overview(game['id'])
    revenue_df = fetch_revenue_by_segment(game['id'])
    regional_df = fetch_regional_performance(game['id'])
    spenders_df = fetch_top_spenders(game['id'])
    weekend_df = fetch_weekend_metrics(game['id'])
    
    # ========================================================================
    # SECTION 1: KEY METRICS (From Star Schema Joins)
    # ========================================================================
    st.header("Key Performance Indicators")
    
    if overview:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Players",
                value=f"{overview['total_players']:,}",
                help="Unique players from fact_session JOIN dim_player"
            )
        
        with col2:
            st.metric(
                label="Total Revenue",
                value=f"${overview['total_revenue']:,.2f}",
                help="Sum from fact_transaction"
            )
        
        with col3:
            st.metric(
                label="Avg Session Duration",
                value=f"{int(overview['avg_session_duration'])}s",
                help="Average from fact_session.duration_seconds"
            )
        
        with col4:
            st.metric(
                label="Total Sessions",
                value=f"{overview['total_sessions']:,}",
                help="Count from fact_session"
            )
        
        # Performance metrics
        st.subheader("Performance Metrics (From fact_telemetry)")
        col1, col2 = st.columns(2)
        
        with col1:
            if overview.get('avg_fps'):
                st.metric("Average FPS", f"{overview['avg_fps']:.1f}")
            else:
                st.info("No FPS data yet")
        
        with col2:
            if overview.get('avg_latency'):
                st.metric("Average Latency", f"{overview['avg_latency']:.0f} ms")
            else:
                st.info("No latency data yet")
    else:
        st.warning("⏳ Waiting for data... Star schema is being populated by Airflow ETL.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 2: REVENUE ANALYSIS BY PLAYER SEGMENT
    # ========================================================================
    st.header("💰 Revenue by Player Segment")
    st.caption("**Query:** JOIN fact_transaction with dim_player, GROUP BY player_segment")
    
    if not revenue_df.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Bar chart
            fig = px.bar(
                revenue_df,
                x='player_segment',
                y='total_revenue',
                color='player_segment',
                title='Total Revenue by Segment',
                labels={'total_revenue': 'Revenue ($)', 'player_segment': 'Player Type'},
                color_discrete_map={
                    'CASUAL': '#3498db',
                    'HARDCORE': '#e74c3c',
                    'WHALE': '#f39c12'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Breakdown")
            for _, row in revenue_df.iterrows():
                st.markdown(f"""
                **{row['player_segment']}**
                - Revenue: ${row['total_revenue']:,.2f}
                - Avg Purchase: ${row['avg_purchase']:.2f}
                - Purchases: {row['purchase_count']}
                """)
    else:
        st.info("📊 No revenue data yet. Purchases will appear here once generated.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 3: REGIONAL PERFORMANCE
    # ========================================================================
    st.header("🌍 Regional Performance Analysis")
    st.caption("**Query:** JOIN fact_telemetry with fact_session, GROUP BY region")
    
    if not regional_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Player Concurrency by Region")
            fig = px.bar(
                regional_df,
                x='region',
                y='concurrent_players',
                color='region',
                title='Active Players per Region'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Performance Metrics by Region")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=regional_df['region'],
                y=regional_df['avg_fps'],
                name='Avg FPS',
                marker_color='lightblue'
            ))
            fig.add_trace(go.Bar(
                x=regional_df['region'],
                y=regional_df['avg_latency'],
                name='Avg Latency (ms)',
                marker_color='salmon'
            ))
            fig.update_layout(barmode='group', title='FPS & Latency by Region')
            st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("Detailed Regional Stats")
        st.dataframe(regional_df, use_container_width=True, hide_index=True)
    else:
        st.info("📡 No telemetry data yet. Heartbeats will populate this section.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 4: TOP SPENDERS
    # ========================================================================
    st.header("Top Spenders (Whale Analysis)")
    st.caption("**Query:** JOIN fact_transaction with dim_player, ORDER BY total revenue DESC")
    
    if not spenders_df.empty:
        # Format for display
        spenders_df['total_spent'] = spenders_df['total_spent'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(
            spenders_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "username": "Username",
                "player_segment": st.column_config.TextColumn(
                    "Segment",
                    help="Player archetype (CASUAL, HARDCORE, WHALE)"
                ),
                "region": "Region",
                "platform": "Platform",
                "purchase_count": "# Purchases",
                "total_spent": "Total Spent"
            }
        )
    else:
        st.info("🐋 No whale data yet. High spenders will appear here.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 5: TEMPORAL ANALYSIS (Weekend vs Weekday)
    # ========================================================================
    st.header("📅 Weekend vs Weekday Analysis")
    st.caption("**Query:** JOIN fact_session with dim_time, GROUP BY is_weekend")
    
    if not weekend_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                weekend_df,
                x='period',
                y='sessions',
                color='period',
                title='Sessions: Weekend vs Weekday',
                labels={'sessions': 'Total Sessions', 'period': 'Period'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                weekend_df,
                x='period',
                y='revenue',
                color='period',
                title='Revenue: Weekend vs Weekday',
                labels={'revenue': 'Total Revenue ($)', 'period': 'Period'}
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("📆 Temporal data will appear once dim_time is populated.")

# ============================================================================
# DASHBOARD VIEW (Global Overview)
# ============================================================================

def render_dashboard():
    st.title("Game Analytics Dashboard - Star Schema Edition")
    
    st.markdown("""
    ### 🎯 What's New in This Version:
    - ✅ **Star Schema Integration**: All analytics now use proper dimensional modeling
    - ✅ **Enhanced Data Generation**: Comprehensive event simulation with all dimensions populated
    - ✅ **MongoDB → Airflow → PostgreSQL**: Clear ETL pipeline via Airflow DAGs
    - ✅ **Complex SQL Analytics**: Multi-table JOINs, GROUP BY, HAVING, aggregations
    """)
    
    st.divider()
    
    games = get_games()
    
    st.header("🎮 Select a Game for Detailed Analytics")
    
    cols = st.columns(4)
    
    for idx, game in enumerate(games):
        col_idx = idx % 4
        
        with cols[col_idx]:
            with st.container(border=True):
                st.image(game['cover_url'], width=150)
                st.markdown(f"##### {game['name']}")
                
                # Try to fetch quick stats
                overview = fetch_star_schema_overview(game['id'])
                if overview:
                    st.caption(f"👥 {overview['total_players']} players")
                    st.caption(f"💰 ${overview['total_revenue']:.0f}")
                else:
                    st.caption("⏳ Loading...")
                
                if st.button("View Analytics", key=f"btn_{game['id']}", use_container_width=True):
                    st.session_state.selected_game = game
                    st.session_state.view = 'detail'
                    st.rerun()

# ============================================================================
# MAIN APP LOGIC
# ============================================================================

# Auto-refresh every 10 seconds
st_autorefresh(interval=10000, key="ui_refresh")

# Initialize session state
if 'view' not in st.session_state:
    st.session_state.view = 'dashboard'
if 'selected_game' not in st.session_state:
    st.session_state.selected_game = None

# Sidebar
with st.sidebar:
    st.header("📊 System Status")
    
    st.markdown("### Architecture")
    st.code("""
    Kafka → MongoDB (Hot)
         ↓
    Airflow ETL
         ↓
    PostgreSQL (Star Schema)
         ↓
    Analytics API
         ↓
    This Dashboard
    """)
    
    st.divider()
    
    st.markdown("### Star Schema Tables")
    st.markdown("""
    **Dimensions:**
    - dim_player
    - dim_game
    - dim_time
    - dim_geography
    - dim_item
    
    **Facts:**
    - fact_session
    - fact_transaction
    - fact_telemetry
    """)
    
    st.divider()
    
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")

# Route to appropriate view
if st.session_state.view == 'dashboard':
    render_dashboard()
elif st.session_state.view == 'detail':
    if st.button("← Back to Dashboard"):
        st.session_state.view = 'dashboard'
        st.rerun()
    
    game = st.session_state.selected_game
    render_game_detail(game)