import streamlit as st
import pandas as pd
from pathlib import Path

st.title("Harvested Queue Times Viewer")

RAW = Path("data/raw")
dates = sorted(p.name for p in RAW.glob("2025-*"))

selected_date = st.selectbox("Select Date", dates)
parts = list((RAW / selected_date).rglob("*.parquet"))

if not parts:
    st.warning("No data for this date.")
else:
    df = pd.concat(pd.read_parquet(p) for p in parts)
    st.write(f"Loaded {len(df)} rows")
    st.dataframe(df.head(100))
