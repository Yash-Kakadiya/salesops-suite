"""
agents/action_agent.py
Enterprise Action Agent with safe API handling, rich auditing, and PII Firewalls.
"""

import os
import re
import json
import time
import uuid
import hashlib
import logging
import random
import requests
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ActionAgent:

    # Config via Env
    MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:7777")
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", 1.0))

    def __init__(self, output_dir="../outputs/actions"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.action_log = self.output_dir / "actions.jsonl"
        # Thread safety for log writing
        self._lock = threading.Lock()

    def _generate_idempotency_key(self, anomaly_id: str, action_type: str) -> str:
        raw = f"{anomaly_id}:{action_type}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _sanitize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Redacts email addresses from the payload before sending."""
        sanitized = payload.copy()
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        for k, v in sanitized.items():
            if isinstance(v, str) and k in ["description", "body"]:
                sanitized[k] = re.sub(email_pattern, "<REDACTED_EMAIL>", v)
        return sanitized

    def _validate_payload_schema(
        self, action_type: str, payload: Dict[str, Any]
    ) -> Optional[str]:
        """
        Pre-flight validation. Returns error string if invalid, None if valid.
        """
        if action_type == "create_ticket":
            required = ["title", "priority", "anomaly_id"]
        elif action_type == "send_email":
            required = ["recipient", "subject", "body"]
        else:
            return f"Unknown action type: {action_type}"

        missing = [k for k in required if k not in payload]
        if missing:
            return f"Missing required fields: {missing}"
        return None

    def plan_actions(self, enriched_anomaly: Dict[str, Any]) -> List[Dict[str, Any]]:
        actions = []
        anom_id = enriched_anomaly.get("anomaly_id")
        score = enriched_anomaly.get("score", 0)
        conf = enriched_anomaly.get("confidence", "Low")

        # Logic: High Severity -> Ticket
        if score > 3.0 and conf == "High":
            actions.append(
                {
                    "type": "create_ticket",
                    "priority": "High",
                    "payload": {
                        "title": f"Investigate: {anom_id}",
                        "description": enriched_anomaly.get("explanation_full", ""),
                        "priority": "High",
                        "anomaly_id": anom_id,
                        "assignee": "SRE-Team",
                    },
                }
            )
        elif score > 1.5:
            actions.append(
                {
                    "type": "send_email",
                    "priority": "Medium",
                    "payload": {
                        "recipient": "manager@company.com",
                        "subject": f"Alert: {anom_id}",
                        "body": enriched_anomaly.get("explanation_short", ""),
                    },
                }
            )

        if enriched_anomaly.get("needs_human_review", False):
            actions.append(
                {
                    "type": "create_ticket",
                    "priority": "Low",
                    "payload": {
                        "title": f"Review: {anom_id}",
                        "description": "AI flagged for review.",
                        "priority": "Low",
                        "anomaly_id": anom_id,
                        "assignee": "Triage-Queue",
                    },
                }
            )

        for act in actions:
            act["action_id"] = str(uuid.uuid4())
            act["anomaly_id"] = anom_id
            act["idempotency_key"] = self._generate_idempotency_key(
                anom_id, act["type"]
            )

        return actions

    def execute_action(self, action_plan: Dict[str, Any]) -> Dict[str, Any]:
        action_type = action_plan["type"]
        payload = self._sanitize_payload(action_plan["payload"])
        key = action_plan["idempotency_key"]

        # 1. Pre-flight Validation
        val_error = self._validate_payload_schema(action_type, payload)
        if val_error:
            result = {
                "status": "client_error",
                "error": f"Validation Failed: {val_error}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._log_audit(action_plan, result)
            return result

        endpoint = "/tickets" if action_type == "create_ticket" else "/emails/send"
        url = f"{self.MOCK_API_URL}{endpoint}"

        headers = {"Content-Type": "application/json", "Idempotency-Key": key}

        attempts = 0
        start_time = time.time()

        while attempts < self.MAX_RETRIES:
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=5)
                latency = (time.time() - start_time) * 1000

                # 2xx Success
                if resp.status_code in [200, 201, 202]:
                    result = {
                        "status": "success",
                        "http_code": resp.status_code,
                        "response": resp.json(),
                        "attempts": attempts + 1,
                        "latency_ms": round(latency, 2),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    self._log_audit(action_plan, result)
                    return result

                # 429 Rate Limit (Respect Retry-After)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2))
                    jitter = random.uniform(0.1, 0.5)
                    wait_time = retry_after + jitter
                    logger.warning(f"Rate Limited (429). Waiting {wait_time:.2f}s.")
                    time.sleep(wait_time)
                    attempts += 1
                    continue

                # 4xx Client Error (Fail Fast)
                if 400 <= resp.status_code < 500:
                    result = {
                        "status": "client_error",
                        "http_code": resp.status_code,
                        "error": resp.text,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    self._log_audit(action_plan, result)
                    return result

                # 5xx Server Error (Retry)
                logger.warning(f"Server Error {resp.status_code}. Retrying...")

            except requests.RequestException as e:
                logger.warning(f"Network Error: {e}")

            # Exponential Backoff + Jitter
            attempts += 1
            jitter = random.uniform(0, 0.3)
            wait = (self.RETRY_BACKOFF * (2 ** (attempts - 1))) + jitter
            time.sleep(wait)

        # Failure
        result = {
            "status": "failed",
            "reason": "Max retries exceeded",
            "attempts": attempts,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._log_audit(action_plan, result)
        return result

    def _log_audit(self, plan: Dict, result: Dict):
        """Rich Audit Logging with Thread Safety."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action_id": plan["action_id"],
            "anomaly_id": plan["anomaly_id"],
            "type": plan["type"],
            "idempotency_key": plan["idempotency_key"],
            "meta": {
                "attempts": result.get("attempts", 0),
                "http_code": result.get("http_code"),
                "latency_ms": result.get("latency_ms"),
            },
            "result": result,
        }
        with self._lock:
            with open(self.action_log, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def run_batch(self, anomalies):
        results = []
        for anom in anomalies:
            plans = self.plan_actions(anom)
            for p in plans:
                res = self.execute_action(p)
                results.append(res)
        return results
