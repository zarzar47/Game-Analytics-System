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
    
    /* Container Borders */
    [data-testid="stVerticalBlock"] > [style*="flex-direction: column;"] > [data-testid="stVerticalBlock"] {
        background: rgba(255, 255, 255, 0.02);
        border-radius: 16px;
        padding: 12px;
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
        
        metrics = [
            ("Total Players", f"{overview['total_players']:,}", "Unique players from fact_session JOIN dim_player"),
            ("Total Revenue", f"${overview['total_revenue']:,.2f}", "Sum from fact_transaction"),
            ("Avg Session Duration", f"{int(overview['avg_session_duration'])}s", "Average from fact_session.duration_seconds"),
            ("Total Sessions", f"{overview['total_sessions']:,}", "Count from fact_session")
        ]
        
        for col, (label, value, help_text) in zip([col1, col2, col3, col4], metrics):
            with col:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value">{value}</div>
                </div>
                """, unsafe_allow_html=True)
        
        # Performance metrics
        st.subheader("Performance Metrics (From fact_telemetry)")
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
        st.warning("Waiting for data... Star schema is being populated by Airflow ETL.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 2: REVENUE ANALYSIS BY PLAYER SEGMENT
    # ========================================================================
    st.header("Revenue by Player Segment")
    st.markdown('<p class="caption">Query: JOIN fact_transaction with dim_player, GROUP BY player_segment</p>', unsafe_allow_html=True)
    
    if not revenue_df.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Bar chart with modern styling
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
            st.plotly_chart(fig, use_container_width=True)
        
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
        st.info("No revenue data yet. Purchases will appear here once generated.")
    
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
            st.plotly_chart(fig, use_container_width=True)
        
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
            st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("Detailed Regional Stats")
        st.dataframe(regional_df, use_container_width=True, hide_index=True)
    else:
        st.info("No telemetry data yet. Heartbeats will populate this section.")
    
    st.divider()
    
    # ========================================================================
    # SECTION 4: TOP SPENDERS
    # ========================================================================
    st.header("Top Spenders (Whale Analysis)")
    st.markdown('<p class="caption">Query: JOIN fact_transaction with dim_player, ORDER BY total revenue DESC</p>', unsafe_allow_html=True)
    
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
        st.info("No whale data yet. High spenders will appear here.")
    
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
            st.plotly_chart(fig, use_container_width=True)
        
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
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Temporal data will appear once dim_time is populated.")

# ============================================================================
# DASHBOARD VIEW (Global Overview)
# ============================================================================

def render_dashboard():
    st.title("Game Analytics Dashboard")
    st.subheader("Star Schema Edition")
    
    st.markdown("""
    <div class="modern-card" style="margin-bottom: 24px;">
        <h3 style="color: #a78bfa; margin-top: 0;">What's New in This Version</h3>
        <ul style="color: #cbd5e1; line-height: 1.8;">
            <li><strong>Star Schema Integration:</strong> All analytics now use proper dimensional modeling</li>
            <li><strong>Enhanced Data Generation:</strong> Comprehensive event simulation with all dimensions populated</li>
            <li><strong>MongoDB → Airflow → PostgreSQL:</strong> Clear ETL pipeline via Airflow DAGs</li>
            <li><strong>Complex SQL Analytics:</strong> Multi-table JOINs, GROUP BY, HAVING, aggregations</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    games = get_games()
    
    st.header("Select a Game for Detailed Analytics")
    
    cols = st.columns(4)
    
    for idx, game in enumerate(games):
        col_idx = idx % 4
        
        with cols[col_idx]:
            # Fetch quick stats
            overview = fetch_star_schema_overview(game['id'])
            
            st.markdown(f"""
            <div class="game-card">
                <div class="game-image-container">
                    <img src="{game['cover_url']}" alt="{game['name']}">
                </div>
                <div class="game-title">{game['name']}</div>
                <div class="game-stats">
                    {"<div class='game-stat-item'>Players: " + f"{overview['total_players']:,}</div><div class='game-stat-item'>Revenue: ${overview['total_revenue']:.0f}</div>" if overview else "<div class='game-stat-item'>Loading...</div>"}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
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

# Route to appropriate view
if st.session_state.view == 'dashboard':
    render_dashboard()
elif st.session_state.view == 'detail':
    if st.button("← Back to Dashboard"):
        st.session_state.view = 'dashboard'
        st.rerun()
    
    game = st.session_state.selected_game
    render_game_detail(game)