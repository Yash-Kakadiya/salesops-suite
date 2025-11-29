"""
observability/logger.py
Structured JSON Logger & Tracing Decorator.
"""

import os
import json
import re
import time
import uuid
import logging
import functools
from datetime import datetime, timezone
from typing import Dict
from pathlib import Path
from contextvars import ContextVar

_current_span_id = ContextVar("span_id", default=None)
_current_trace_id = ContextVar("trace_id", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
            "trace_id": _current_trace_id.get(),
            "span_id": _current_span_id.get(),
        }
        if hasattr(record, "props"):
            log_record.update(record.props)
        return json.dumps(log_record)


def to_snake(name):
    """Converts CamelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def get_logger(name: str, output_dir: str = None):
    """Configures a file-based JSON logger."""
    # Determine output dir: Arg > Env > Default
    if not output_dir:
        output_dir = os.getenv("OBSERVABILITY_DIR", "outputs/observability")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Robust filename: TestComponent -> test_component.jsonl
    filename = f"{to_snake(name)}.jsonl"
    target_file = Path(output_dir) / filename

    # Check if we already have a handler for this specific file
    has_file_handler = any(
        isinstance(h, logging.FileHandler)
        and Path(h.baseFilename).resolve() == target_file.resolve()
        for h in logger.handlers
    )

    if not has_file_handler:
        fh = logging.FileHandler(target_file)
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)

    # Ensure console handler exists (once)
    has_stream = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in logger.handlers
    )
    if not has_stream:
        ch = logging.StreamHandler()
        ch.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(ch)

    return logger


# --- Tracing ---


def _write_span(span: Dict):
    """Atomic append to trace file."""
    out_dir = os.getenv("OBSERVABILITY_DIR", "outputs/observability")
    trace_file = Path(out_dir) / "trace_spans.jsonl"

    trace_file.parent.mkdir(parents=True, exist_ok=True)
    with open(trace_file, "a") as f:
        f.write(json.dumps(span) + "\n")


def timeit_span(operation_name: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            parent_span = _current_span_id.get()
            trace_id = _current_trace_id.get() or str(uuid.uuid4())
            span_id = str(uuid.uuid4())

            token_span = _current_span_id.set(span_id)
            token_trace = _current_trace_id.set(trace_id)

            start_ts = datetime.now(timezone.utc).isoformat()
            t0 = time.time()
            status = "OK"
            error_msg = None

            try:
                return func(*args, **kwargs)
            except Exception as e:
                status = "ERROR"
                error_msg = str(e)
                raise e
            finally:
                duration_ms = (time.time() - t0) * 1000
                end_ts = datetime.now(timezone.utc).isoformat()

                span = {
                    "span_id": span_id,
                    "trace_id": trace_id,
                    "parent_span_id": parent_span,
                    "name": operation_name,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_ms": round(duration_ms, 2),
                    "status": status,
                    "error": error_msg,
                    "component": func.__module__,
                }
                _write_span(span)

                _current_span_id.reset(token_span)
                _current_trace_id.reset(token_trace)

        return wrapper

    return decorator
