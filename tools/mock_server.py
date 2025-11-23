"""
tools/mock_server.py
Enterprise Mock Server with Atomic Persistence, Chaos Control, and OpenAPI compliance.
Port: 7777 (Default)
"""

import uvicorn
import random
import uuid
import json
import logging
import os
import time
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
from pathlib import Path

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MockServer")

app = FastAPI(title="SalesOps Mock API", version="1.0.0")

# --- Configuration ---
PORT = int(os.getenv("MOCK_SERVER_PORT", 7777))
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = OUTPUT_DIR / "mock_db.json"

SERVER_START_TIME = datetime.now(timezone.utc).isoformat()


# --- Persistence Layer (High Priority: Atomic Writes) ---
def load_db():
    if DB_FILE.exists():
        try:
            with open(DB_FILE, "r") as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} records from {DB_FILE}")
                return data
        except Exception as e:
            logger.error(f"Failed to load DB: {e}")
            return {}
    return {}


def save_db(data):
    """Atomic write: write to temp -> rename."""
    try:
        # Write to temp file in same dir to ensure atomic rename works across filesystems
        with tempfile.NamedTemporaryFile("w", dir=OUTPUT_DIR, delete=False) as tmp:
            json.dump(data, tmp, indent=2)
            tmp_path = tmp.name

        # Atomic replacement
        os.replace(tmp_path, DB_FILE)
    except Exception as e:
        logger.error(f"Failed to save DB: {e}")


# Load state
IDEMPOTENCY_STORE = load_db()

# --- Runtime Configuration ---
CONFIG = {"chaos_enabled": False, "failure_rate": 0.3, "simulate_rate_limit": False}


# --- Models ---
class TicketRequest(BaseModel):
    title: str
    description: Optional[str] = None
    priority: str
    anomaly_id: str
    assignee: Optional[str] = "Auto-Agent"


class EmailRequest(BaseModel):
    recipient: str
    subject: str
    body: str


class ChaosConfig(BaseModel):
    enabled: bool
    failure_rate: float = 0.3
    simulate_rate_limit: bool = False


# --- Middleware ---
def check_chaos():
    if CONFIG["chaos_enabled"] and random.random() < CONFIG["failure_rate"]:
        if CONFIG["simulate_rate_limit"]:
            # Simulate 429 with Retry-After header
            raise HTTPException(
                status_code=429,
                detail="Rate Limit Exceeded",
                headers={"Retry-After": "2"},
            )
        logger.warning("💥 Chaos Monkey: 500 Error")
        raise HTTPException(status_code=500, detail="Simulated Internal Failure")


# --- Endpoints ---


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "port": PORT,
        "uptime_since": SERVER_START_TIME,
        "db_records": len(IDEMPOTENCY_STORE),
        "db_path": str(DB_FILE),
        "config": CONFIG,
    }


@app.get("/ready")
def readiness_check():
    """Returns 200 only if server is ready to accept traffic."""
    if IDEMPOTENCY_STORE is not None:
        return {"status": "ready"}
    raise HTTPException(status_code=503, detail="Initializing")


@app.post("/admin/chaos")
def configure_chaos(config: ChaosConfig):
    """Runtime toggle for chaos engineering."""
    CONFIG["chaos_enabled"] = config.enabled
    CONFIG["failure_rate"] = config.failure_rate
    CONFIG["simulate_rate_limit"] = config.simulate_rate_limit
    logger.info(f"Chaos config updated: {CONFIG}")
    return {"message": "Chaos config updated", "config": CONFIG}


@app.post("/tickets", status_code=201)
def create_ticket(ticket: TicketRequest, idempotency_key: str = Header(...)):
    check_chaos()

    # Idempotency Check
    if idempotency_key in IDEMPOTENCY_STORE:
        logger.info(f"🔄 Replay: {idempotency_key}")
        return IDEMPOTENCY_STORE[idempotency_key]

    ticket_id = f"TICKET-{random.randint(10000, 99999)}"

    response = {
        "ticket_id": ticket_id,
        "status": "created",
        "link": f"https://jira.internal/browse/{ticket_id}",
        "review_url": (
            f"https://jira.internal/review/{ticket_id}"
            if "Review" in ticket.title
            else None
        ),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    # Persist
    IDEMPOTENCY_STORE[idempotency_key] = response
    save_db(IDEMPOTENCY_STORE)

    return response


@app.post("/emails/send", status_code=202)
def send_email(email: EmailRequest, idempotency_key: str = Header(...)):
    check_chaos()

    if idempotency_key in IDEMPOTENCY_STORE:
        return IDEMPOTENCY_STORE[idempotency_key]

    msg_id = str(uuid.uuid4())
    response = {
        "message_id": msg_id,
        "status": "queued",
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }

    IDEMPOTENCY_STORE[idempotency_key] = response
    save_db(IDEMPOTENCY_STORE)

    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
