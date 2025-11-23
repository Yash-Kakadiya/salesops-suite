"""
agents/anomaly_llm_agent.py

Production-Grade Anomaly Explainer Agent.
Features:
- Strict JSON Schema Validation (Auto-fixes list/string hallucinations)
- Robust Error Handling (Retries, Backoff, Circuit Breaker)
- Full Observability (Audit Logs + Raw Responses + Token Est.)
- Cost/Rate Limiting
- PII Redaction
"""

import os
import time
import json
import random
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional
from dotenv import load_dotenv

# ADK Imports
from google.adk.models.google_llm import Gemini
from google.genai import types

# Configure Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AnomalyExplainerAgent:
    """
    Enterprise-grade agent for explaining anomalies.
    """

    # Configuration Constants
    MAX_RETRIES = 3
    BASE_DELAY = 2.0
    MAX_DELAY = 30.0
    CIRCUIT_BREAKER_THRESHOLD = 5
    BATCH_DELAY = 1.0
    MAX_PROMPT_CHARS = 7777
    EXPLANATION_VERSION = "1.0"

    # Schema Definition
    REQUIRED_KEYS = [
        "explanation_short",
        "explanation_full",
        "suggested_actions",
        "confidence",
        "needs_human_review",
    ]

    def __init__(
        self,
        model_name: str = "gemini-2.5-flash-lite",
        dry_run: bool = False,
    ):
        load_dotenv()
        if "GOOGLE_API_KEY" not in os.environ and not dry_run:
            logger.warning("GOOGLE_API_KEY missing! Agent will fail unless in dry_run.")

        self.model_name = model_name
        self.dry_run = dry_run

        if not self.dry_run:
            self.model = Gemini(model=model_name)

        # Observability Paths
        self.audit_dir = Path("../outputs/observability")
        self.response_dir = self.audit_dir / "responses"
        self.audit_file = self.audit_dir / "llm_calls.jsonl"

        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.response_dir.mkdir(parents=True, exist_ok=True)

    def _redact_pii(self, text: str) -> str:
        """Redacts potential PII using heuristics."""
        if not text:
            return ""
        t = str(text)
        if "CUST-" in t or "@" in t or (t.isdigit() and len(t) > 10):
            h = hashlib.md5(t.encode()).hexdigest()[:6]
            return f"REDACTED_{h}"
        return t

    def _truncate_context(self, context: Dict[str, Any]) -> str:
        """Formats and truncates context to fit token budget."""
        compact = []
        for k, v in context.items():
            val_str = f"{v:.2f}" if isinstance(v, float) else str(v)
            compact.append(f"{k}: {val_str}")

        # Improvement 3: Use newlines for better LLM readability
        full_str = "\n".join(compact)

        if len(full_str) > 2000:
            return full_str[:2000] + "...(truncated)"
        return full_str

    def _construct_prompt(self, record: Dict[str, Any]) -> str:
        """Constructs prompt with explicit schema instructions."""
        entity = self._redact_pii(record.get("entity_id", "Unknown"))
        context_str = self._truncate_context(record.get("context", {}))

        prompt = f"""
You are a Senior SalesOps Analyst. Analyze this sales anomaly.

DATA CONTEXT:
- Entity: {entity} ({record.get('level', 'global')})
- Metric: {record.get('metric', 'Sales')}
- Value: {record.get('value', 0):,.2f}
- Expected: {record.get('expected', 0):,.2f}
- Score: {record.get('score', 0):.2f}

STATISTICAL CONTEXT:
{context_str}

OUTPUT FORMAT:
Return valid JSON with these exact keys:
{{
    "explanation_short": "1 sentence summary",
    "explanation_full": "2-3 sentence detailed analysis",
    "suggested_actions": ["Action 1", "Action 2"],
    "confidence": "High/Medium/Low",
    "needs_human_review": boolean
}}

CONSTRAINT:
- Rely ONLY on provided numbers.
- Do NOT invent external events.
- Output pure JSON (no markdown).
"""
        return prompt.strip()[: self.MAX_PROMPT_CHARS]

    def _save_audit(self, record_id, prompt, response_obj, latency, status, error=None):
        """Saves structured audit logs and raw response files."""
        ts = datetime.now(timezone.utc).isoformat()
        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()

        # Improvement 4: Estimate tokens
        est_tokens = len(prompt) // 4

        # 1. Save Raw Response (for debugging)
        raw_file = self.response_dir / f"{prompt_hash}.json"
        try:
            with open(raw_file, "w") as f:
                json.dump(
                    {
                        "id": record_id,
                        "timestamp": ts,
                        "prompt": prompt,
                        "response": response_obj,
                        "error": str(error) if error else None,
                    },
                    f,
                    indent=2,
                )
        except Exception:
            pass

        # 2. Append to JSONL Audit Log
        entry = {
            "timestamp": ts,
            "anomaly_id": record_id,
            "prompt_hash": prompt_hash,
            "model": self.model_name,
            "latency_ms": round(latency * 1000, 2),
            "status": status,
            "est_tokens": est_tokens,
            "error_type": type(error).__name__ if error else None,
        }
        try:
            with open(self.audit_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.error(f"Audit write failed: {e}")

    def _validate_response_schema(self, data: Dict) -> Dict:
        """Ensures all required keys exist and types are correct."""
        validated = data.copy()
        missing = []

        for key in self.REQUIRED_KEYS:
            # Check existence
            if key not in validated:
                missing.append(key)
                # Defaults
                if key == "suggested_actions":
                    validated[key] = []
                elif key == "needs_human_review":
                    validated[key] = True
                else:
                    validated[key] = "N/A (Schema Error)"

            # Improvement 1: Enforce List Type for actions
            if key == "suggested_actions":
                val = validated[key]
                if isinstance(val, str):
                    validated[key] = [val]  # Fix hallucinated string
                elif not isinstance(val, list):
                    validated[key] = []  # Fallback for bad types

        if missing:
            logger.warning(f"Schema validation warning. Missing keys: {missing}")
            validated["schema_error"] = f"Missing: {','.join(missing)}"

        return validated

    def _call_llm_safe(self, prompt: str) -> Tuple[Dict[str, Any], float]:
        """Robust LLM Caller. Always returns (dict, latency)."""
        if self.dry_run:
            return {
                "explanation_short": "[DRY RUN]",
                "explanation_full": "Mock explanation.",
                "suggested_actions": ["Mock Action"],
                "confidence": "High",
                "needs_human_review": False,
            }, 0.0

        attempts = 0
        last_error = None

        while attempts < self.MAX_RETRIES:
            try:
                start_time = time.time()

                # Call API
                response = self.model.api_client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config={"response_mime_type": "application/json"},
                )
                latency = time.time() - start_time

                # Adapter
                if hasattr(response, "text"):
                    text_resp = response.text
                elif hasattr(response, "output"):
                    text_resp = str(response.output)
                else:
                    text_resp = str(response)

                # Clean Markdown
                clean_text = text_resp.strip()
                if clean_text.startswith("```"):
                    lines = clean_text.splitlines()
                    if lines[0].startswith("```"):
                        lines = lines[1:]
                    if lines and lines[-1].strip() == "```":
                        lines = lines[:-1]
                    clean_text = "\n".join(lines)

                # Parse & Validate
                parsed = json.loads(clean_text)
                validated = self._validate_response_schema(parsed)

                return validated, latency

            except Exception as e:
                attempts += 1
                last_error = e

                err_str = str(e)
                if "400" in err_str or "401" in err_str or "403" in err_str:
                    logger.error(f"Fatal Error: {e}")
                    raise e

                wait = min(self.MAX_DELAY, self.BASE_DELAY * (2 ** (attempts - 1)))
                time.sleep(wait + random.uniform(0, 0.5))

        raise last_error or Exception("Max retries exceeded")

    def batch_explain(self, anomalies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Main pipeline with circuit breaker."""
        results = []
        failures = 0
        circuit_open = False

        for i, rec in enumerate(anomalies):
            # Improvement 5: Explicit skipped flags
            if circuit_open:
                skipped_rec = rec.copy()
                skipped_rec["error"] = "SKIPPED"
                skipped_rec["skipped"] = True
                skipped_rec["skipped_reason"] = "Circuit Breaker Tripped"
                results.append(skipped_rec)
                continue

            anomaly_id = rec.get("anomaly_id", f"row_{i}")
            prompt = self._construct_prompt(rec)

            try:
                if not self.dry_run:
                    time.sleep(self.BATCH_DELAY)

                data, latency = self._call_llm_safe(prompt)
                self._save_audit(anomaly_id, prompt, data, latency, "SUCCESS")

                # Enrich
                enriched = rec.copy()
                enriched.update(data)

                # Improvement 2: Versioning in Meta
                enriched["meta"] = {
                    "model": self.model_name,
                    "latency_ms": int(latency * 1000),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "version": self.EXPLANATION_VERSION,
                }
                results.append(enriched)
                failures = 0

            except Exception as e:
                logger.error(f"Failed {anomaly_id}: {e}")
                self._save_audit(anomaly_id, prompt, None, 0, "FAILED", error=e)

                err_rec = rec.copy()
                err_rec["error"] = str(e)
                results.append(err_rec)

                failures += 1
                if failures >= self.CIRCUIT_BREAKER_THRESHOLD:
                    logger.critical("Circuit Breaker Tripped!")
                    circuit_open = True

        return results
