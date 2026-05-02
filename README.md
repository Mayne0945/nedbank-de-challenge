# Nedbank N*ovation Data & Analytics Masters — Data Engineering Track

**Candidate:** Tshifhiwa Gift Mayne
**Stages completed:** 1 · 2 · 3
**Runtime:** Stage 1 ~4.5 min · Stage 2 ~9.4 min · Stage 3 ~4 min (streaming)

---

## Overview

A medallion pipeline (Bronze → Silver → Gold) that ingests three source files from a bank-fintech integration scenario, cleans and validates the data, produces a dimensional model, and processes a real-time transaction event stream — all running fully offline inside Docker.

```
accounts.csv        ─┐
transactions.jsonl  ─┼──► Bronze ──► Silver ──► Gold  ──► dq_report.json
customers.csv       ─┘    (raw)    (clean)   (dims+fact)

/data/stream/*.jsonl ──► stream_gold/current_balances
                     └──► stream_gold/recent_transactions
```

---

## Repository Structure

```
starter_kit/
├── Dockerfile                        # Extends base image; Delta JARs baked at build time
├── entrypoint.sh                     # Pre-creates output dirs; fixes hostname for --network none
├── warmup.py                         # JAR pre-download script (build-time only)
├── requirements.txt
├── pipeline/
│   ├── run_all.py                    # Orchestrator — Bronze→Silver→Gold→DQ Report→Stream
│   ├── ingest.py                     # Bronze layer ingestion
│   ├── transform.py                  # Silver: type casting, dedup, DQ flagging
│   ├── provision.py                  # Gold dimensional model
│   ├── stream_ingest.py              # Stage 3 polling loop + Delta MERGE
│   ├── dq_reporter.py                # Writes dq_report.json (Stage 2+)
│   └── spark_utils.py                # Shared SparkSession factory (offline-capable)
├── config/
│   ├── pipeline_config.yaml          # All paths and Spark settings
│   └── dq_rules.yaml                 # DQ detection and handling rules
└── adr/
    └── stage3_adr.md                 # Architecture Decision Record
```

---

## How to Run

```bash
# Build
docker build -t candidate-submission:latest starter_kit/

# Run (batch + streaming)
docker run \
  -v /path/to/data:/data/input \
  -v /path/to/config:/data/config \
  -v /path/to/output:/data/output \
  -v /path/to/stream:/data/stream \
  --network none -m 4g --cpus="2" \
  candidate-submission:latest
```

The pipeline detects `/data/stream/` automatically — if present and non-empty, streaming runs after batch completes.

---

## Stage 1 — Core Batch Pipeline

Ingests `accounts.csv`, `customers.csv`, `transactions.jsonl` through Bronze → Silver → Gold.

**Validation results:**
- Q1 Transaction types: ✅ 4 rows (CREDIT / DEBIT / FEE / REVERSAL)
- Q2 Unlinked accounts: ✅ 0
- Q3 Province distribution: ✅ 9 SA provinces

---

## Stage 2 — Stress Test + Data Quality

3× volume (3M transactions), 6 DQ categories, `merchant_subcategory` field, `dq_report.json`.

| Issue Code | Records | % | Handling |
|---|---|---|---|
| `DATE_FORMAT` | 119,415 | 3.98% | NORMALISE |
| `ORPHANED_ACCOUNT` | 74,679 | 2.49% | FLAG |
| `CURRENCY_VARIANT` | 29,854 | 1.00% | NORMALISE |
| `TYPE_MISMATCH` | 9,957 | 0.33% | NORMALISE |
| `NULL_REQUIRED` | 1,500 | 0.50% | QUARANTINE |

Total flagged: **233,905 / 3,000,000** (7.80%). Gold `fact_transactions`: 2,925,321 rows.

---

## Stage 3 — Streaming Extension

Polls `/data/stream/` for JSONL micro-batch files in chronological order. Updates two stream_gold tables via Delta MERGE.

| Table | Semantics |
|---|---|
| `current_balances` | Upsert — 1 row per `account_id` |
| `recent_transactions` | Last 50 per account, keyed on `(account_id, transaction_id)` |

All 12 stream files processed. Self-terminating after 90s quiesce. SLA met on all batches.

---

## Architecture Decisions

**Config-driven design** — All paths, Spark settings, and DQ rules live in YAML. No infrastructure string literals in `.py` files.

**Surrogate keys** — `sha2(key, 256)` truncated to 15 hex chars cast to BIGINT. Deterministic and stable across re-runs. `row_number()` rejected due to non-deterministic sort order across Spark versions.

**Offline JAR strategy** — `warmup.py` pre-downloads Delta JARs during `docker build` and copies them into PySpark's `jars/` directory. Zero network calls at runtime — required for air-gapped scoring environments.

**Partitioning** — `fact_transactions` partitioned by `transaction_date` for analytics query performance. Dimensions not partitioned (too small to benefit).

**DQ flagging before normalisation** — Issue codes detected against raw source values before casting. Preserves accurate counts in `dq_report.json`.

**Orphan handling** — Orphaned transactions flagged (`dq_flag=ORPHANED_ACCOUNT`) and retained in Silver for auditability. Excluded from Gold via inner join on `dim_accounts`.

**Streaming** — Directory polling as specified. ADR in `adr/stage3_adr.md` documents trade-offs vs event-driven architecture.

---

## Validation Summary

| Check | Stage 1 | Stage 2 | Stage 3 |
|---|---|---|---|
| Exit code | 0 ✅ | 0 ✅ | 0 ✅ |
| Validation queries | Pass ✅ | Pass ✅ | N/A |
| DQ report | N/A | ✅ | ✅ |
| Stream files | N/A | N/A | 12/12 ✅ |
| Offline (--network none) | ✅ | ✅ | ✅ |
| Within time limit | 4.5 / 30 min ✅ | 9.4 / 30 min ✅ | 4 min ✅ |

---

## Known Trade-offs

**Overwrite mode:** `mode("overwrite")` rather than Delta `MERGE` for batch layers — idempotent and faster. A production incremental pipeline would use MERGE for late-arriving data.

**Separate SparkSessions:** Each stage initialises its own session for independent debuggability. A single long-lived session would be more efficient in production.

**No Z-Ordering:** At 1-3M rows the write overhead outweighs read benefit. Revisit at 100M+ rows.