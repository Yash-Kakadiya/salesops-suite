"""
agents/data_ingestor.py

Robust ingestion utilities for SalesOps Suite.

Features:
- Load CSV / Parquet with safe defaults and encoding fallback
- Normalize column names
- Schema validation with helpful errors
- Date parsing helpers (common columns: 'Order Date', 'Ship Date')
- Preview utility returning JSON-serializable summaries
- Save cleaned snapshot (parquet/csv)
- Lightweight CLI for quick tests

Usage (example):
    python agents/data_ingestor.py --path data/raw/superstore.csv --sample 100 --preview
"""

from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import json
import logging
import sys
from datetime import datetime

# Logger setup
logger = logging.getLogger("salesops.data_ingestor")
if not logger.handlers:
    ch = logging.StreamHandler(stream=sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)


def _normalize_columns(df: pd.DataFrame, inplace: bool = True) -> pd.DataFrame:
    """
    Normalize dataframe column names: strip(), replace newlines, and keep original case.
    """
    cols = [c.strip().replace("\n", " ").replace("\r", "") for c in df.columns]
    if inplace:
        df.columns = cols
        return df
    else:
        new = df.copy()
        new.columns = cols
        return new


def _try_read_csv(path: Path, **kwargs) -> pd.DataFrame:
    """
    Try reading CSV with sensible defaults. Falls back to latin1 encoding if utf-8 fails.
    """
    base_kwargs = dict(low_memory=False, parse_dates=False)
    base_kwargs.update(kwargs)
    try:
        logger.info(f"Reading CSV (utf-8): {path}")
        return pd.read_csv(path, encoding="utf-8", **base_kwargs)
    except (UnicodeDecodeError, ValueError) as e:
        logger.warning(f"utf-8 read failed for {path}: {e}. Trying latin1.")
        try:
            return pd.read_csv(path, encoding="latin1", **base_kwargs)
        except Exception as e2:
            logger.error(f"Failed to read CSV {path} with latin1: {e2}")
            raise


def _read_parquet(path: Path, **kwargs) -> pd.DataFrame:
    logger.info(f"Reading Parquet: {path}")
    return pd.read_parquet(path, **kwargs)


def load_table(
    path: str,
    sample_n: Optional[int] = None,
    force_dtype: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Load a CSV or Parquet file into a pandas DataFrame.

    Args:
        path: Path to CSV or Parquet file
        sample_n: If provided, return a random sample of this many rows (seeded)
        force_dtype: optional column dtype mapping to pass to pandas reader
        **kwargs: forwarded to pandas readers (e.g., usecols)

    Returns:
        pd.DataFrame
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{p} not found")

    suffix = p.suffix.lower()
    read_kwargs = {}
    if force_dtype:
        read_kwargs["dtype"] = force_dtype
    read_kwargs.update(kwargs)

    if suffix in [".csv", ".tsv", ".txt"]:
        df = _try_read_csv(p, **read_kwargs)
    elif suffix in [".parquet", ".pq"]:
        df = _read_parquet(p, **read_kwargs)
    else:
        # attempt csv then parquet
        try:
            df = _try_read_csv(p, **read_kwargs)
        except Exception:
            df = _read_parquet(p, **read_kwargs)

    # normalize columns
    df = _normalize_columns(df, inplace=False)

    # sampling
    if (
        sample_n is not None
        and isinstance(sample_n, int)
        and sample_n > 0
        and len(df) > sample_n
    ):
        logger.info(f"Sampling {sample_n} rows from {len(df)}")
        df = df.sample(n=sample_n, random_state=42).reset_index(drop=True)

    logger.info(f"Loaded DataFrame shape={df.shape}")
    return df


def validate_schema(
    df: pd.DataFrame,
    required_cols: List[str],
    optional_cols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Validate required columns exist. Return a validation report dict.
    Raises ValueError if required columns are missing.

    Report format:
    {
        "n_rows": int,
        "n_cols": int,
        "missing_required": [...],
        "optional_missing": [...],
        "extra_columns": [...]
    }
    """
    optional_cols = optional_cols or []
    cols = list(df.columns)
    missing_required = [c for c in required_cols if c not in cols]
    missing_optional = [c for c in optional_cols if c not in cols]
    extra_columns = [c for c in cols if c not in required_cols + optional_cols]

    report = {
        "n_rows": len(df),
        "n_cols": len(cols),
        "missing_required": missing_required,
        "missing_optional": missing_optional,
        "extra_columns": extra_columns,
    }

    if missing_required:
        logger.error(
            f"Schema validation failed. Missing required columns: {missing_required}"
        )
        raise ValueError(f"Missing required columns: {missing_required}")

    logger.info("Schema validation passed")
    return report


def ensure_datetime(
    df: pd.DataFrame, col: str, fmt: Optional[str] = None, inplace: bool = True
) -> pd.DataFrame:
    """
    Convert column to datetime, coercing errors to NaT. Returns df.
    """
    if col not in df.columns:
        raise KeyError(f"{col} not found in dataframe")
    converted = pd.to_datetime(df[col], format=fmt, errors="coerce")
    df[col] = converted if inplace else converted.copy()
    n_null = df[col].isna().sum()
    if n_null > 0:
        logger.warning(f"{n_null} NaT values in column {col} after datetime conversion")
    return df


def basic_preview(df: pd.DataFrame, n_head: int = 5) -> Dict[str, Any]:
    """
    Return a JSON-serializable preview:
    - head rows (records)
    - dtypes
    - shape
    - numeric summary (describe)
    """
    head = df.head(n_head).to_dict(orient="records")
    dtypes = df.dtypes.apply(lambda x: str(x)).to_dict()
    numeric_summary = {}
    try:
        numeric_summary = df.describe(include=[pd.np.number]).to_dict()
    except Exception:
        # fallback to pandas 2.x (avoid pd.np deprecation)
        numeric_summary = df.select_dtypes(include=["number"]).describe().to_dict()

    preview = {
        "head": head,
        "dtypes": dtypes,
        "shape": df.shape,
        "numeric_summary": numeric_summary,
    }
    logger.info(f"Generated preview for shape={df.shape}")
    return preview


def save_snapshot(
    df: pd.DataFrame,
    out_dir: str = "data/processed",
    filename: Optional[str] = None,
    to_parquet: bool = True,
) -> str:
    """
    Save cleaned DataFrame snapshot. Returns path string.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if filename is None:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        filename = f"snapshot_{ts}"
    dest = out / (f"{filename}.parquet" if to_parquet else f"{filename}.csv")
    if to_parquet:
        df.to_parquet(dest, index=False)
    else:
        df.to_csv(dest, index=False)
    logger.info(f"Saved snapshot to {dest}")
    return str(dest)


# CLI helper
def _parse_args(argv: Optional[List[str]] = None) -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        prog="data_ingestor", description="SalesOps data ingestion CLI"
    )
    parser.add_argument("--path", required=True, help="Path to CSV/Parquet file")
    parser.add_argument(
        "--sample", type=int, default=None, help="Sample N rows for quick preview"
    )
    parser.add_argument(
        "--preview", action="store_true", help="Print JSON preview to stdout"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run schema validation with default required columns",
    )
    parser.add_argument(
        "--datetime-cols",
        nargs="*",
        default=["Order Date", "Ship Date"],
        help="Columns to parse as datetime",
    )
    parser.add_argument(
        "--out", default=None, help="Filename (without ext) to save cleaned snapshot"
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Save snapshot as CSV instead of Parquet",
    )
    return parser.parse_args(argv)


def _default_required_columns() -> List[str]:
    # For the Superstore dataset — adjust if you change dataset
    return [
        "Order ID",
        "Order Date",
        "Ship Date",
        "Customer ID",
        "Sales",
        "Quantity",
        "Discount",
        "Profit",
        "Category",
        "Sub-Category",
        "Region",
    ]


def main(argv: Optional[List[str]] = None):
    args = _parse_args(argv)
    path = args.path
    df = load_table(path, sample_n=args.sample)

    # parse datetimes if present
    for col in args.datetime_cols:
        if col in df.columns:
            try:
                df = ensure_datetime(df, col)
            except Exception as e:
                logger.warning(f"Failed to parse datetime for {col}: {e}")

    # run validation if requested
    if args.validate:
        required = _default_required_columns()
        try:
            report = validate_schema(df, required_cols=required)
            logger.info(f"Validation report: {report}")
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    # preview
    if args.preview:
        p = basic_preview(df)
        print(json.dumps(p, indent=2, default=str))

    # save snapshot if requested
    if args.out:
        saved = save_snapshot(df, filename=args.out, to_parquet=(not args.no_parquet))
        print("Saved:", saved)


if __name__ == "__main__":
    main()


# ✅ How to test it (exact commands)

# Run a quick preview on your saved Superstore CSV:

# # from repo root (ensure salesops conda env is active)
# python agents/data_ingestor.py --path data/raw/superstore.csv --sample 200 --preview


# You should get a JSON preview printed (head, dtypes, shape, numeric summary).

# Validate schema (uses default required columns for Superstore):

# python agents/data_ingestor.py --path data/raw/superstore.csv --validate


# If validation fails it will raise a friendly error listing missing columns.

# Save a cleaned snapshot (parquet):

# python agents/data_ingestor.py --path data/raw/superstore.csv --out superstore_clean
# # saved to data/processed/superstore_clean.parquet