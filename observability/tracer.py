"""
observability/tracer.py
Lightweight Tracing Helpers.
"""

from .logger import timeit_span, _current_trace_id, _current_span_id


def get_current_trace_id():
    """Returns the active trace ID or None."""
    return _current_trace_id.get()


def get_current_span_id():
    """Returns the active span ID or None."""
    return _current_span_id.get()


# Re-export decorator for clean imports
__all__ = ["timeit_span", "get_current_trace_id", "get_current_span_id"]
