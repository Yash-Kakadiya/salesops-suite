"""
agents/data_ingestor.py

Data Ingestor Agent for SalesOps Suite.

Responsibilities:
- Load raw CSV data from flexible paths.
- Handle encoding issues (utf-8 vs latin1).
- Normalize column names (strip whitespace).
- Parse dates robustly.
- Validate schema against required columns.
- Save processed snapshots to Parquet for downstream agents.

This module defines the DataIngestorAgent class.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Optional, List, Any, Dict
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataIngestorAgent:
    """
    Agent responsible for loading, validating, and cleaning sales data.
    """

    # Default columns we expect in the Superstore dataset
    REQUIRED_COLUMNS = [
        "Order Date",
        "Sales",
        "Profit",
        "Region",
        "Category",
        "Order ID",
    ]

    def __init__(self, file_path: str):
        """
        Initialize the agent with the path to the raw CSV file.

        Args:
            file_path: Path to the source CSV file.
        """
        self.file_path = Path(file_path)
        self.df: Optional[pd.DataFrame] = None

    def _try_read_csv(self) -> pd.DataFrame:
        """
        Internal helper: Attempts to read CSV with multiple encodings.
        Global Superstore often requires 'latin1'.
        """
        encodings = ["utf-8", "latin1", "iso-8859-1", "cp1252"]

        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV not found at {self.file_path}")

        for enc in encodings:
            try:
                logger.info(f"Attempting read with encoding='{enc}'...")
                df = pd.read_csv(self.file_path, encoding=enc)
                logger.info(f"Success! Read {len(df)} rows.")
                return df
            except UnicodeDecodeError:
                logger.warning(f"Encoding '{enc}' failed. Retrying...")
            except Exception as e:
                logger.error(f"Unexpected error reading {self.file_path}: {e}")
                raise e

        raise ValueError(f"Could not read {self.file_path} with any standard encoding.")

    def _normalize_columns(self):
        """
        Internal helper: Strips whitespace from column headers.
        """
        if self.df is not None:
            self.df.columns = [str(c).strip() for c in self.df.columns]

    def validate_schema(self, required: List[str] = None) -> bool:
        """
        Checks if the DataFrame contains the required columns.

        Args:
            required: List of column names to check. Defaults to class REQUIRED_COLUMNS.

        Returns:
            bool: True if valid, False if columns are missing.
        """
        if self.df is None:
            logger.warning("No data loaded to validate.")
            return False

        req = required or self.REQUIRED_COLUMNS
        missing = [c for c in req if c not in self.df.columns]

        if missing:
            logger.error(f"Schema Validation Failed! Missing columns: {missing}")
            return False

        logger.info("Schema validation passed.")
        return True

    def ensure_datetime(self, date_cols: List[str] = None):
        """
        Converts specified columns to datetime objects.

        Args:
            date_cols: List of column names to convert.
        """
        if self.df is None:
            return

        cols = date_cols or ["Order Date", "Ship Date"]
        for c in cols:
            if c in self.df.columns:
                # coerce errors=turn unparseable data into NaT
                self.df[c] = pd.to_datetime(self.df[c], errors="coerce")

                # Report on data quality
                nat_count = self.df[c].isna().sum()
                if nat_count > 0:
                    logger.warning(
                        f"Column '{c}' has {nat_count} invalid/missing dates."
                    )

    def clean_data(self) -> pd.DataFrame:
        """
        Main pipeline execution method.
        Loads, normalizes, converts dates, and validates schema.

        Returns:
            pd.DataFrame: The cleaned dataframe.
        """
        # 1. Load
        self.df = self._try_read_csv()

        # 2. Normalize Headers
        self._normalize_columns()

        # 3. Convert Dates
        self.ensure_datetime(["Order Date", "Ship Date"])

        # 4. Drop rows with missing critical dates (essential for Time Series)
        if "Order Date" in self.df.columns:
            original_len = len(self.df)
            self.df = self.df.dropna(subset=["Order Date"])
            dropped = original_len - len(self.df)
            if dropped > 0:
                logger.info(f"Dropped {dropped} rows with missing Order Date.")

        # 5. Validate
        self.validate_schema()

        # 6. Add helper time columns (useful for all downstream agents)
        self.df["Order Year"] = self.df["Order Date"].dt.year
        self.df["Order Month"] = self.df["Order Date"].dt.month

        return self.df

    def basic_preview(self):
        """
        Prints a quick summary of the loaded data.
        """
        if self.df is not None:
            print("--- Data Preview ---")
            print(f"Shape: {self.df.shape}")
            print(f"Columns: {list(self.df.columns)}")
            print("Head:")
            print(self.df.head(3))
        else:
            print("No data loaded.")

    def save_snapshot(self, output_path: str):
        """
        Saves the current DataFrame to Parquet format.

        Args:
            output_path: Destination file path (e.g., 'data/processed/file.parquet')
        """
        if self.df is None:
            logger.warning("Cannot save snapshot: DataFrame is None.")
            return

        try:
            out_p = Path(output_path)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            self.df.to_parquet(out_p, index=False)
            logger.info(f"Snapshot saved successfully to {out_p}")
        except Exception as e:
            logger.error(f"Failed to save snapshot: {e}")


# End of Class
