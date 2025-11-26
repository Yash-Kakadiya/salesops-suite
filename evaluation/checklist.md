# Capstone Evaluation Checklist ✅

## 1. Core Capabilities
- [ ] **Ingestion:** Can load `superstore.csv` without errors?
- [ ] **Detection:** Does `AnomalyStatAgent` produce a JSON payload?
- [ ] **Explanation:** Does `AnomalyExplainerAgent` use RAG and return structured JSON?
- [ ] **Action:** Does `ActionAgent` create tickets in the Mock Server?
- [ ] **Orchestration:** Does `main.py` run end-to-end?

## 2. Enterprise Features
- [ ] **Observability:** Are `trace_spans.jsonl` and `llm_calls.jsonl` generated?
- [ ] **Reliability:** Do we handle API 500 errors with retries?
- [ ] **Safety:** Is PII (email/phone) redacted in logs and memory?
- [ ] **Idempotency:** Do duplicate runs prevent duplicate tickets?

## 3. Bonus Points
- [ ] **Gemini:** Used for explanation?
- [ ] **Memory:** Implemented Vector Search (RAG)?
- [ ] **Video:** 3-minute demo recorded?