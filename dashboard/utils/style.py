"""
dashboard/utils/style.py
Shared styling and CSS for the Streamlit App.
Adapts automatically to Light and Dark modes using CSS variables.
"""

import streamlit as st


def apply_custom_css():
    """Injects custom CSS for a professional look."""
    st.markdown(
        """
        <style>
        /* --- GLOBAL FONTS --- */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }
        
        /* --- METRIC CARDS --- */
        /* Target the container of st.metric */
        div[data-testid="stMetric"] {
            background-color: rgba(128, 128, 128, 0.1); /* Transparent gray works in both modes */
            border: 1px solid rgba(128, 128, 128, 0.2);
            padding: 15px;
            border-radius: 8px;
            transition: transform 0.2s ease;
        }
        
        div[data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            border-color: #2D6CDF;
        }
        
        /* --- CUSTOM CONTAINERS (Cards) --- */
        .card {
            background-color: rgba(255, 255, 255, 0.05); /* Subtle overlay */
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }
        
        /* --- STATUS MESSAGES --- */
        .stSuccess {
            border-left: 5px solid #2ECC71;
        }
        .stError {
            border-left: 5px solid #E74C3C;
        }
        .stInfo {
            border-left: 5px solid #2D6CDF;
        }
        
        /* --- HEADERS --- */
        h1 {
            font-weight: 800;
            letter-spacing: -1px;
            padding-bottom: 10px;
        }
        
        h2, h3 {
            font-weight: 600;
        }
        
        /* --- SIDEBAR --- */
        section[data-testid="stSidebar"] {
            border-right: 1px solid rgba(128, 128, 128, 0.2);
        }
        
        </style>
    """,
        unsafe_allow_html=True,
    )


def sidebar_logo():
    """Adds a sidebar header."""
    st.sidebar.title("🤖 SalesOps")
    st.sidebar.caption("Autonomous Agent Suite v1.0")
    st.sidebar.markdown("---")
