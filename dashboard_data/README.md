# Dashboard Data Layer

This directory contains the **Data Access Layer** for the Streamlit App.

## 📂 Files
* **`loaders.py`**: Python functions to load and format data.
* **`*.json / *.parquet`**: Static artifacts exported by `scripts/run_pipeline.py`.

## 🔄 How to Refresh Data
Run the pipeline script from the root directory:
```bash
python scripts/run_pipeline.py