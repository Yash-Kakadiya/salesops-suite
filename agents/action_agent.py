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

# Observability
from observability.logger import timeit_span, get_logger
from observability.metrics import ACTIONS_TOTAL

logger = get_logger("ActionAgent")


class ActionAgent:

    MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:7777")
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", 1.0))

    def __init__(self, output_dir="../outputs/actions"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.action_log = self.output_dir / "actions.jsonl"
        self._lock = threading.Lock()

    def _generate_idempotency_key(self, anomaly_id: str, action_type: str) -> str:
        raw = f"{anomaly_id}:{action_type}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _sanitize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = payload.copy()
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        for k, v in sanitized.items():
            if isinstance(v, str) and k in ["description", "body"]:
                sanitized[k] = re.sub(email_pattern, "<REDACTED_EMAIL>", v)
        return sanitized

    def _validate_payload_schema(
        self, action_type: str, payload: Dict[str, Any]
    ) -> Optional[str]:
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

    @timeit_span("action.plan")
    def plan_actions(self, enriched_anomaly: Dict[str, Any]) -> List[Dict[str, Any]]:
        actions = []
        anom_id = enriched_anomaly.get("anomaly_id")
        score = enriched_anomaly.get("score", 0)
        conf = enriched_anomaly.get("confidence", "Low")

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

    @timeit_span("action.execute")
    def execute_action(self, action_plan: Dict[str, Any]) -> Dict[str, Any]:
        action_type = action_plan["type"]
        payload = self._sanitize_payload(action_plan["payload"])
        key = action_plan["idempotency_key"]

        val_error = self._validate_payload_schema(action_type, payload)
        if val_error:
            result = {
                "status": "client_error",
                "error": f"Validation Failed: {val_error}",
            }
            self._log_audit(action_plan, result)
            ACTIONS_TOTAL.labels(type=action_type, status="client_error").inc()
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
                    ACTIONS_TOTAL.labels(type=action_type, status="success").inc()
                    return result

                if 400 <= resp.status_code < 500:
                    if resp.status_code == 429:
                        wait = int(resp.headers.get("Retry-After", 2))
                        logger.warning(f"Rate Limited. Waiting {wait}s.")
                        time.sleep(wait)
                        attempts += 1
                        continue

                    result = {
                        "status": "client_error",
                        "http_code": resp.status_code,
                        "error": resp.text,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    self._log_audit(action_plan, result)
                    ACTIONS_TOTAL.labels(type=action_type, status="client_error").inc()
                    return result

                logger.warning(f"Server Error {resp.status_code}. Retrying...")

            except requests.RequestException as e:
                logger.warning(f"Network Error: {e}")

            attempts += 1
            jitter = random.uniform(0, 0.3)
            wait = (self.RETRY_BACKOFF * (2 ** (attempts - 1))) + jitter
            time.sleep(wait)

        result = {
            "status": "failed",
            "reason": "Max retries exceeded",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._log_audit(action_plan, result)
        ACTIONS_TOTAL.labels(type=action_type, status="failed").inc()
        return result

    def _log_audit(self, plan: Dict, result: Dict):
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
