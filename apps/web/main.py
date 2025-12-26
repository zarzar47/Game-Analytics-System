import streamlit as st
import pandas as pd
import os
import requests
import plotly.express as px
import plotly.graph_objects as go
from game_library import get_games
from streamlit_autorefresh import st_autorefresh
from datetime import datetime
import time

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Game Analytics | Star Schema",
    layout="wide"
)

# Modern Custom CSS
st.markdown("""
<style>
    /* Global Styles */
    .stApp {
        background: linear-gradient(135deg, #0f0f1e 0%, #1a1a2e 100%);
    }
    
    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Modern Card Styling */
    .modern-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 20px;
        padding: 24px;
        transition: all 0.3s ease;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }
    
    .modern-card:hover {
        transform: translateY(-5px);
        border-color: rgba(99, 102, 241, 0.5);
        box-shadow: 0 12px 48px rgba(99, 102, 241, 0.2);
    }
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, rgba(99, 102, 241, 0.2) 0%, rgba(168, 85, 247, 0.2) 100%);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 16px;
        padding: 24px;
        color: white;
        transition: all 0.3s ease;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
    }
    
    .metric-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.3);
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 8px;
    }
    
    .metric-label {
        font-size: 0.95rem;
        opacity: 0.8;
        color: #e2e8f0;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 500;
    }
    
    /* Game Card Container */
    .game-card-container {
        height: 100%;
        display: flex;
        flex-direction: column;
    }
    
    .game-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 20px;
        padding: 20px;
        transition: all 0.3s ease;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        height: 100%;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: space-between;
    }
    
    .game-card:hover {
        transform: translateY(-8px);
        border-color: rgba(99, 102, 241, 0.5);
        box-shadow: 0 16px 48px rgba(99, 102, 241, 0.3);
    }
    
    .game-image-container {
        width: 100%;
        height: 200px;
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden;
        border-radius: 16px;
        background: rgba(0, 0, 0, 0.2);
        margin-bottom: 16px;
    }
    
    .game-image-container img {
        width: 100%;
        height: 100%;
        object-fit: cover;
        border-radius: 16px;
    }
    
    .game-title {
        font-size: 1.25rem;
        font-weight: 700;
        color: #e2e8f0;
        text-align: center;
        margin: 12px 0;
        min-height: 60px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    .game-stats {
        width: 100%;
        text-align: center;
        margin: 12px 0;
        padding: 12px;
        background: rgba(0, 0, 0, 0.2);
        border-radius: 12px;
    }
    
    .game-stat-item {
        color: #94a3b8;
        font-size: 0.9rem;
        margin: 4px 0;
    }
    
    /* Section Headers */
    h1 {
        background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        margin-bottom: 24px;
    }
    
    h2, h3 {
        color: #e2e8f0 !important;
        font-weight: 700;
    }
    
    /* Button Styling */
    .stButton > button {
        background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 16px rgba(99, 102, 241, 0.3);
        width: 100%;
        margin-top: 12px;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(99, 102, 241, 0.5);
        background: linear-gradient(135deg, #7c3aed 0%, #c026d3 100%);
    }
    
    /* Data Frame Styling */
    .dataframe {
        background: rgba(255, 255, 255, 0.05) !important;
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* Info/Warning Boxes */
    .stAlert {
        background: rgba(255, 255, 255, 0.05);
        border-left: 4px solid #6366f1;
        border-radius: 12px;
        backdrop-filter: blur(10px);
    }
    
    /* Divider */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.5), transparent);
        margin: 32px 0;
    }
    
    /* Caption Styling */
    .caption {
        color: #94a3b8;
        font-size: 0.9rem;
        font-style: italic;
        margin-top: -8px;
        margin-bottom: 16px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fetch_star_schema_overview(game_id: str, cache_ttl: int):
    """Fetch comprehensive overview from star schema"""
    try:
        url = f"{API_URL}/analytics/star-schema/overview/{game_id}?cache_ttl={cache_ttl}&skip_cache=true"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        data['_fetched_at'] = datetime.now().isoformat()
        return data
    except requests.exceptions.Timeout:
        st.error(f"Request timeout for game {game_id}")
        return None
    except Exception as e:
        st.error(f"Error fetching overview: {e}")
        return None

def fetch_revenue_by_segment(game_id: str):
    """Fetch revenue breakdown by player segment"""
    try:
        response = requests.get(
            f"{API_URL}/analytics/star-schema/revenue-by-segment/{game_id}",
            timeout=5
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        return pd.DataFrame()

def fetch_regional_performance(game_id: str):
    """Fetch performance metrics by region"""
    try:
        response = requests.get(
            f"{API_URL}/analytics/star-schema/regional-performance/{game_id}",
            timeout=5
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        return pd.DataFrame()

def fetch_top_spenders(game_id: str):
    """Fetch top spending players"""
    try:
        response = requests.get(
            f"{API_URL}/analytics/star-schema/top-spenders/{game_id}?limit=10",
            timeout=5
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        return pd.DataFrame()

def fetch_weekend_metrics(game_id: str):
    """Fetch weekend vs weekday comparison"""
    try:
        response = requests.get(
            f"{API_URL}/analytics/star-schema/weekend-vs-weekday/{game_id}",
            timeout=5
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        return pd.DataFrame()

# ============================================================================
# DEBUG PANEL
# ============================================================================

def render_debug_panel():
    """Show system health and data freshness"""
    with st.expander("Debug Information", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Last UI Refresh", datetime.now().strftime("%H:%M:%S"))
        
        with col2:
            try:
                response = requests.get(f"{API_URL}/health", timeout=2)
                if response.status_code == 200:
                    st.success("API Online")
                else:
                    st.error("API Error")
            except:
                st.error("API Offline")
        
        with col3:
            st.metric("Cache TTL", f"{st.session_state.cache_ttl}s")

# ============================================================================
# GAME DETAIL VIEW
# ============================================================================

def render_game_detail(game):
    st.title(f"{game['name']} - Star Schema Analytics")
    
    render_debug_panel()
    
    # FIXED: Removed time component from cache key for stability
    cache_key = f"game_detail_{game['id']}"
    
    overview = fetch_star_schema_overview(game['id'], st.session_state.cache_ttl)
    revenue_df = fetch_revenue_by_segment(game['id'])
    regional_df = fetch_regional_performance(game['id'])
    spenders_df = fetch_top_spenders(game['id'])
    weekend_df = fetch_weekend_metrics(game['id'])
    
    st.info(f"Data fetched at: {datetime.now().strftime('%H:%M:%S')}")
    
    # ========================================================================
    # SECTION 1: KEY METRICS
    # ========================================================================
    st.header("Key Performance Indicators")
    
    if overview and overview.get('total_players', 0) > 0:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Players</div>
                <div class="metric-value">{overview['total_players']:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Revenue</div>
                <div class="metric-value">${overview['total_revenue']:,.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Avg Session Duration</div>
                <div class="metric-value">{int(overview['avg_session_duration'])}s</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Sessions</div>
                <div class="metric-value">{overview['total_sessions']:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.subheader("Performance Metrics")
        col1, col2 = st.columns(2)
        
        with col1:
            if overview.get('avg_fps'):
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Average FPS</div>
                    <div class="metric-value">{overview['avg_fps']:.1f}</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No FPS data yet")
        
        with col2:
            if overview.get('avg_latency'):
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Average Latency</div>
                    <div class="metric-value">{overview['avg_latency']:.0f} ms</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No latency data yet")
    else:
        st.warning("No data available for this game")
    
    st.divider()
    
    # ========================================================================
    # SECTION 2: REVENUE ANALYSIS BY PLAYER SEGMENT
    # ========================================================================
    st.header("Revenue by Player Segment")
    st.markdown('<p class="caption">Query: JOIN fact_transaction with dim_player, GROUP BY player_segment</p>', unsafe_allow_html=True)
    
    if not revenue_df.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                revenue_df,
                x='player_segment',
                y='total_revenue',
                color='player_segment',
                title='Total Revenue by Segment',
                labels={'total_revenue': 'Revenue ($)', 'player_segment': 'Player Type'},
                color_discrete_map={
                    'CASUAL': '#60a5fa',
                    'HARDCORE': '#f87171',
                    'WHALE': '#fbbf24'
                }
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True, key=f"revenue_chart_{cache_key}")
        
        with col2:
            st.subheader("Breakdown")
            for _, row in revenue_df.iterrows():
                st.markdown(f"""
                <div class="modern-card" style="margin-bottom: 12px;">
                    <strong style="color: #a78bfa; font-size: 1.1rem;">{row['player_segment']}</strong><br>
                    <span style="color: #94a3b8;">Revenue:</span> <strong style="color: #60a5fa;">${row['total_revenue']:,.2f}</strong><br>
                    <span style="color: #94a3b8;">Avg Purchase:</span> <strong>${row['avg_purchase']:.2f}</strong><br>
                    <span style="color: #94a3b8;">Purchases:</span> {row['purchase_count']}
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No revenue data yet")
    
    st.divider()
    
    # ========================================================================
    # SECTION 3: REGIONAL PERFORMANCE
    # ========================================================================
    st.header("Regional Performance Analysis")
    st.markdown('<p class="caption">Query: JOIN fact_telemetry with fact_session, GROUP BY region</p>', unsafe_allow_html=True)
    
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
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True, key=f"concurrency_chart_{cache_key}")
        
        with col2:
            st.subheader("Performance Metrics by Region")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=regional_df['region'],
                y=regional_df['avg_fps'],
                name='Avg FPS',
                marker_color='#60a5fa'
            ))
            fig.add_trace(go.Bar(
                x=regional_df['region'],
                y=regional_df['avg_latency'],
                name='Avg Latency (ms)',
                marker_color='#f87171'
            ))
            fig.update_layout(
                barmode='group',
                title='FPS & Latency by Region',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0')
            )
            st.plotly_chart(fig, use_container_width=True, key=f"performance_chart_{cache_key}")
        
        st.subheader("Detailed Regional Stats")
        st.dataframe(regional_df, use_container_width=True, hide_index=True)
    else:
        st.info("No telemetry data yet")
    
    st.divider()
    
    # ========================================================================
    # SECTION 4: TOP SPENDERS
    # ========================================================================
    st.header("Top Spenders")
    st.markdown('<p class="caption">Query: JOIN fact_transaction with dim_player, ORDER BY total revenue DESC</p>', unsafe_allow_html=True)
    
    if not spenders_df.empty:
        spenders_df['total_spent'] = spenders_df['total_spent'].apply(lambda x: f"${x:,.2f}")
        st.dataframe(spenders_df, use_container_width=True, hide_index=True)
    else:
        st.info("No spender data yet")
    
    st.divider()
    
    # ========================================================================
    # SECTION 5: TEMPORAL ANALYSIS (Weekend vs Weekday)
    # ========================================================================
    st.header("Weekend vs Weekday Analysis")
    st.markdown('<p class="caption">Query: JOIN fact_session with dim_time, GROUP BY is_weekend</p>', unsafe_allow_html=True)
    
    if not weekend_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                weekend_df,
                x='period',
                y='sessions',
                color='period',
                title='Sessions: Weekend vs Weekday',
                labels={'sessions': 'Total Sessions', 'period': 'Period'},
                color_discrete_map={'Weekend': '#a78bfa', 'Weekday': '#60a5fa'}
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True, key=f"sessions_chart_{cache_key}")
        
        with col2:
            fig = px.bar(
                weekend_df,
                x='period',
                y='revenue',
                color='period',
                title='Revenue: Weekend vs Weekday',
                labels={'revenue': 'Total Revenue ($)', 'period': 'Period'},
                color_discrete_map={'Weekend': '#a78bfa', 'Weekday': '#60a5fa'}
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e2e8f0'),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True, key=f"revenue_temporal_chart_{cache_key}")
    else:
        st.info("No temporal data yet")

# ============================================================================
# DASHBOARD VIEW
# ============================================================================

def render_dashboard():
    st.title("Game Analytics Dashboard")
    st.subheader("Star Schema Edition")
    
    st.markdown("""
    <div class="modern-card" style="margin-bottom: 24px;">
        <h3 style="color: #a78bfa; margin-top: 0;">Real-Time Game Analytics System</h3>
        <ul style="color: #cbd5e1; line-height: 1.8;">
            <li><strong>Star Schema:</strong> Dimensional modeling with fact and dimension tables</li>
            <li><strong>ETL Pipeline:</strong> Kafka → MongoDB → Airflow → PostgreSQL</li>
            <li><strong>Analytics:</strong> Multi-table JOINs, aggregations, GROUP BY</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    render_debug_panel()
    
    st.divider()
    
    games = get_games()
    
    st.header("Select a Game for Detailed Analytics")
    
    cols = st.columns(4)
    
    for idx, game in enumerate(games):
        col_idx = idx % 4
        
        with cols[col_idx]:
            overview = fetch_star_schema_overview(game['id'], st.session_state.cache_ttl)
            
            st.markdown(f"""
            <div class="game-card">
                <div class="game-image-container">
                    <img src="{game['cover_url']}" alt="{game['name']}">
                </div>
                <div class="game-title">{game['name']}</div>
                <div class="game-stats">
                    {"<div class='game-stat-item'>Players: " + f"{overview['total_players']:,}</div><div class='game-stat-item'>Revenue: ${overview['total_revenue']:.0f}</div>" if overview and overview.get('total_players', 0) > 0 else "<div class='game-stat-item'>Loading...</div>"}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # FIXED: Removed time.time() from button key - now stable across refreshes
            if st.button("View Analytics", key=f"btn_{game['id']}", use_container_width=True):
                st.session_state.selected_game = game
                st.session_state.view = 'detail'
                st.rerun()

def render_settings_page():
    st.title("Settings")
    
    st.markdown('<div class="modern-card">', unsafe_allow_html=True)
    
    st.subheader("Cache Configuration")
    
    new_ttl = st.slider(
        "Backend Cache TTL (seconds)",
        min_value=1,
        max_value=600,
        value=st.session_state.cache_ttl,
        help="Lower values = faster updates but more database load"
    )
    if new_ttl != st.session_state.cache_ttl:
        st.session_state.cache_ttl = new_ttl
        st.success(f"Cache TTL updated to {new_ttl} seconds")

    st.divider()
    
    st.subheader("UI Configuration")
    new_refresh = st.slider(
        "UI Auto-Refresh (seconds)",
        min_value=5,
        max_value=60,
        value=st.session_state.auto_refresh_interval,
        help="How often the UI refreshes automatically"
    )
    if new_refresh != st.session_state.auto_refresh_interval:
        st.session_state.auto_refresh_interval = new_refresh
        st.success(f"UI refresh interval updated to {new_refresh} seconds")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    st.divider()
    if st.button("Clear All Caches & Force Refresh", type="primary"):
        st.cache_data.clear()
        st.success("Caches cleared!")
        st.rerun()

# ============================================================================
# MAIN APP LOGIC
# ============================================================================

if 'view' not in st.session_state:
    st.session_state.view = 'dashboard'
if 'selected_game' not in st.session_state:
    st.session_state.selected_game = None
if 'cache_ttl' not in st.session_state:
    st.session_state.cache_ttl = 10
if 'auto_refresh_interval' not in st.session_state:
    st.session_state.auto_refresh_interval = 10

# FIXED: Stable refresh key
refresh_count = st_autorefresh(
    interval=st.session_state.auto_refresh_interval * 1000, 
    key="autorefresh_main"
)

with st.sidebar:
    st.markdown("<h1>Game Analytics</h1>", unsafe_allow_html=True)
    
    page = st.radio(
        "Navigation",
        ("Dashboard", "Settings"),
        key="navigation_page"
    )
    
    st.divider()
    st.info(f"Refresh #{refresh_count}")
    st.info(f"Cache TTL: {st.session_state.cache_ttl}s")
    st.info(f"UI Refresh: {st.session_state.auto_refresh_interval}s")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

if page == "Dashboard":
    if st.session_state.view == 'dashboard':
        render_dashboard()
    elif st.session_state.view == 'detail':
        if st.button("← Back to Dashboard"):
            st.session_state.view = 'dashboard'
            st.rerun()
        
        game = st.session_state.selected_game
        render_game_detail(game)
else:
    render_settings_page()