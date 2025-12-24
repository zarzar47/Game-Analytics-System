import os
import pandas as pd
import requests
import streamlit as st

# --- API CONNECTION ---

# Fetch the API URL from environment variables, with a default for local dev
API_URL = os.getenv("API_URL", "http://api:8000")

# --- DATA FETCHING FUNCTIONS ---

@st.cache_data(ttl=10) # Cache data for 10 seconds to align with Spark's trigger
def get_revenue_data():
    """Fetches real-time revenue analytics from the Spark-processed table via the API."""
    try:
        response = requests.get(f"{API_URL}/analytics/revenue")
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.warning(f"Could not fetch revenue data from API. Is it running? Error: {e}")
    except Exception as e:
        st.warning(f"An error occurred while processing revenue data: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=10)
def get_concurrency_data():
    """Fetches real-time player concurrency from the Spark-processed table via the API."""
    try:
        response = requests.get(f"{API_URL}/analytics/concurrency")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.warning(f"Could not fetch concurrency data from API: {e}")
    except Exception as e:
        st.warning(f"An error occurred while processing concurrency data: {e}")
    return pd.DataFrame()

@st.cache_data(ttl=10)
def get_performance_data():
    """Fetches real-time game performance from the Spark-processed table via the API."""
    try:
        response = requests.get(f"{API_URL}/analytics/performance")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        st.warning(f"Could not fetch performance data from API: {e}")
    except Exception as e:
        st.warning(f"An error occurred while processing performance data: {e}")
    return pd.DataFrame()
