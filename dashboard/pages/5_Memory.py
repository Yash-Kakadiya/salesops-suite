import streamlit as st
import sys
import pandas as pd
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dashboard.utils.style import apply_custom_css, sidebar_logo
from memory.memory_bank import MemoryBank

st.set_page_config(page_title="Memory Bank", page_icon="🧠", layout="wide")
apply_custom_css()
sidebar_logo()

st.title("🧠 Semantic Memory Bank")
st.markdown(
    "Inspect the Long-Term Vector Store. The agent uses this to 'learn' from past resolutions."
)

# Initialize Bank (Read-Only Mode technically, but we query it)
# Point to the production bank
BANK_PATH = Path("outputs/memory/memory_bank.json")

if BANK_PATH.exists():
    try:
        bank = MemoryBank(persistence_path=str(BANK_PATH))

        # 1. Stats
        count = bank.backend.count()
        st.metric("Total Memories Stored", count)

        st.markdown("---")

        # 2. Interactive Search
        st.subheader("🔎 Test Retrieval (RAG)")
        query = st.text_input("Ask the Memory Bank:", value="Sales drop in Technology")

        if query:
            results = bank.query(query, top_k=5)

            if results:
                for res in results:
                    score = res.get("_score", 0)
                    # Visual Score Bar
                    st.progress(min(score, 1.0), text=f"Similarity: {score:.4f}")
                    st.info(res.get("text", "No text content"))
                    with st.expander("View Metadata"):
                        st.json(res.get("metadata", {}))
            else:
                st.warning("No relevant memories found.")

    except Exception as e:
        st.error(f"Could not load Memory Bank: {e}")
else:
    st.warning(
        f"Memory Bank file not found at `{BANK_PATH}`. Run the pipeline to generate it."
    )
