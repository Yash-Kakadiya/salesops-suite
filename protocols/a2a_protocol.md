# SalesOps Agent-to-Agent (A2A) Protocol v1.1

This document defines the message exchange format used by the SalesOps Coordinator to orchestrate independent agents.

## 1. Message Envelope Schema
All messages passed between agents MUST adhere to this JSON schema:

```json
{
  "$schema": "[http://salesops.internal/schemas/a2a-envelope-v1.json](http://salesops.internal/schemas/a2a-envelope-v1.json)",
  "type": "object",
  "required": ["message_id", "sender", "type", "payload"],
  "properties": {
    "message_id": { "type": "string", "format": "uuid" },
    "conversation_id": { "type": "string" },
    "sender": { "type": "string" },
    "recipient": { "type": "string" },
    "type": { "enum": ["REQUEST", "RESPONSE", "EVENT", "ERROR"] },
    "in_reply_to": { "type": "string", "description": "UUID of request message" },
    "status": { "enum": ["success", "failed", "partial"] },
    "payload": { "type": "object" },
    "created_at": { "type": "string", "format": "date-time" }
  }
}
```

## 2. Message Types

### A. REQUEST (Coordinator -> Agent)
Used to trigger an agent's core logic.

**task:** Specific operation (e.g., ```detect_anomalies```, ```explain_batch```).

**payload:** Arguments required for the task.

Example:
```json
{
  "message_id": "abc-123",
  "type": "REQUEST",
  "task": "explain_batch",
  "payload": { "anomalies": [...] }
}
```

---

### B. RESPONSE (Agent -> Coordinator)
Used to return results.

**payload:** The output data (e.g., list of anomalies).

**status:** ```success``` or ```partial_success```.

Example:
```json
{
  "message_id": "def-456",
  "in_reply_to": "abc-123",
  "type": "RESPONSE",
  "status": "success",
  "payload": { "enriched": [...] }
}
```
---

### C. ERROR (Agent -> Coordinator)
Used when an agent fails.

**payload:** ```{"error": "details", "code": 500}```.

## 3. Flow Definitions
The Coordinator executes a "Flow" defined as a **DAG (Directed Acyclic Graph)**.

### Sequential Flow (Pipeline)
**Ingestor:** Produces ```data_snapshot_path```.

**Detector:** Consumes ```data_snapshot_path``` -> Produces ```anomaly_payload```.

**Explainer:** Consumes ```anomaly_payload``` -> Produces ```enriched_anomalies```.

**Actor:** Consumes ```enriched_anomalies``` -> Produces ```action_results```.

---

### Parallel Fan-Out
The Coordinator splits the ```anomaly_payload``` into chunks.

Multiple ```ExplainerAgent``` instances process chunks in parallel threads.

The Coordinator aggregates the results into a single ```enriched_anomalies``` list.

## 🏃 How to Run
```bash
# Production CLI
python main.py --data data/raw/superstore.csv --workers 4

# Testing
pytest tests/test_a2a_coordinator.py
```
## 4. Data Payloads

### Anomaly Payload (From Detector)
```json
{
  "anomaly_id": "zscore_Global_2014-03-18",
  "metric": "Sales",
  "value": 24739.75,
  "expected": 585.69,
  "score": 53.08,
  "context": { "window_mean": 585.69, "window_std": 455.05 }
}
```
### Enriched Payload (From Explainer)
```json
{
  "anomaly_id": "...",
  "explanation_short": "Massive spike due to Technology bulk order.",
  "suggested_actions": ["Check inventory", "Email Regional Mgr"],
  "confidence": "High",
  "meta": { "model": "gemini-2.0-flash", "latency_ms": 1200 }
}
```