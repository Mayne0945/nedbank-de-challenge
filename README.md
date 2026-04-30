# Nedbank N*ovation Data & Analytics Masters — Data Engineering Track

**Candidate:** Tshifhiwa Gift Mayne
**Stage:** 1 (Batch Pipeline)
**Pipeline runtime:** ~4.5 minutes on 2 vCPU / 4 GB RAM

---

## Overview

This pipeline implements a medallion architecture (Bronze → Silver → Gold) that ingests three source files from a bank-fintech data integration scenario, cleans and validates the data, and produces a dimensional model ready for analytics and AI/ML workloads.

```
accounts.csv        ─┐
transactions.jsonl  ─┼──► Bronze ──► Silver ──► Gold
customers.csv       ─┘    (raw)    (clean)   (dimensional)
```

---

## Repository Structure

```
starter_kit/
├── Dockerfile                        # Extends base image; bakes Delta JARs at build time
├── warmup.py                         # JAR pre-download script (build-time only)
├── requirements.txt
├── pipeline/
│   ├── run_all.py                    # Orchestrator — Bronze → Silver → Gold in sequence
│   ├── ingest.py                     # Bronze layer ingestion
│   ├── transform.py                  # Silver layer transformation
│   ├── provision.py                  # Gold layer dimensional model
│   └── spark_utils.py                # Shared SparkSession factory
├── config/
│   ├── pipeline_config.yaml          # All paths, Spark settings — no hardcoded values
│   └── dq_rules.yaml                 # DQ rules config (Stage 2+)
└── adr/
    └── stage3_adr.md                 # Architecture Decision Record for Stage 3 streaming
```

---

## How to Run

### Prerequisites
- Docker
- The base image built locally: `nedbank-de-challenge/base:1.0`

### Build
```bash
docker build -t candidate-submission:latest starter_kit/
```

The build pre-downloads and bakes the Delta Lake JARs into the image so the pipeline runs fully offline — no internet access required at runtime.

### Run
```bash
docker run \
  -v /path/to/data:/data/input \
  -v /path/to/config:/data/config \
  -v /path/to/output:/data/output \
  --network none \
  -m 4g --cpus="2" \
  candidate-submission:latest
```

### Validate Gold Output (DuckDB)
```bash
python3 -c "
import duckdb
con = duckdb.connect()
con.execute('INSTALL delta; LOAD delta;')
gold = '/path/to/output/gold'
print(con.execute(f'SELECT transaction_type, COUNT(*) FROM delta_scan(\"{gold}/fact_transactions\") GROUP BY 1 ORDER BY 1').df())
print(con.execute(f'SELECT COUNT(*) AS unlinked FROM delta_scan(\"{gold}/dim_accounts\") a LEFT JOIN delta_scan(\"{gold}/dim_customers\") c ON a.customer_id = c.customer_id WHERE c.customer_id IS NULL').df())
print(con.execute(f'SELECT c.province, COUNT(DISTINCT a.account_id) FROM delta_scan(\"{gold}/dim_accounts\") a JOIN delta_scan(\"{gold}/dim_customers\") c ON a.customer_id = c.customer_id GROUP BY 1 ORDER BY 1').df())
"
```

---

## Architecture Decisions

### 1. Medallion Layers

**Bronze — raw as-arrived:** Source data is written unmodified with a single consistent `ingestion_timestamp` applied to all rows in the run. Using `lit(run_timestamp)` rather than `current_timestamp()` ensures every row gets the same value regardless of which executor processes it.

**Silver — typed and validated:** All columns are explicitly cast using the data dictionary — no inferred types from Bronze. This catches type mismatches at the transformation layer, not silently in Gold. Deduplication uses `dropDuplicates([primary_key])` — no `.count()` calls, no row-level loops.

**Gold — dimensional model:** Star schema with `fact_transactions` at the centre, joined to `dim_accounts` and `dim_customers`. Dimension tables are built first and written to Delta before the fact table join — this ensures the fact table joins against committed, consistent state rather than an in-memory plan that could diverge.

### 2. Surrogate Key Strategy

Surrogate keys use `sha2(natural_key, 256)` truncated to 15 hex characters and cast to BIGINT — stable across pipeline re-runs on the same input data. `row_number()` was rejected because sort stability across Spark versions is not guaranteed, which would produce different keys on re-runs.

### 3. Config-Driven Design

All file paths, column names, Spark settings, and DQ thresholds live in `pipeline_config.yaml`. No string literals representing infrastructure concerns appear in `.py` files. This means schema changes, path changes, and threshold adjustments at Stage 2 require only config edits — not code changes.

### 4. Offline JAR Strategy

The scoring environment has no internet access. The `configure_spark_with_delta_pip` pattern triggers Ivy to download JARs from Maven Central at runtime, which fails in an air-gapped container. Instead, `warmup.py` runs during `docker build` to download the JARs and copy them into PySpark's own `jars/` directory — Spark loads everything in that directory automatically. Runtime requires zero network access.

### 5. Partitioning Strategy

`fact_transactions` is partitioned by `transaction_date`. Analytics and compliance queries typically filter on date ranges — partitioning enables partition pruning and avoids full table scans. `dim_accounts` and `dim_customers` are not partitioned: at 80-100K rows, the overhead of partitioning outweighs the benefit.

### 6. Referential Integrity

The Silver layer validates that every `accounts.customer_ref` maps to a known `customers.customer_id` using an inner join — orphaned accounts are excluded before Gold provisioning. This guarantees that Validation Query 2 (zero unlinked accounts) passes without defensive logic in Gold.

Transactions are checked against Silver accounts using a left join with a `_is_valid` flag column — no `.collect()`, fully distributed. Orphaned transactions are flagged with `dq_flag = ORPHANED_ACCOUNT` rather than dropped, preserving them in Silver for auditability.

### 7. Stage 2 Forward-Compatibility

The following patterns are in place now, requiring no code changes at Stage 2:

- `parse_date()` uses a `coalesce` chain: `yyyy-MM-dd` → `dd/MM/yyyy` → Unix epoch. All three Stage 2 date format variants are handled.
- `normalise_currency()` maps all South African Rand variants (`R`, `rands`, `710`, `zar`) to `"ZAR"`.
- `merchant_subcategory` is added as a NULL column if absent from the source JSON — the Silver and Gold schemas are consistent across all stages.
- `dq_flag` exists in Silver transactions (all NULL for Stage 1 clean data) — ready for Stage 2 DQ injection.

---

## Validation Results (Stage 1)

| Query | Expected | Actual | Status |
|---|---|---|---|
| Q1 — Transaction types | 4 rows | CREDIT/DEBIT/FEE/REVERSAL | ✅ Pass |
| Q2 — Unlinked accounts | 0 | 0 | ✅ Pass |
| Q3 — Province distribution | 9 rows | All 9 SA provinces | ✅ Pass |

---

## Known Trade-offs

**Overwrite mode on re-runs:** The pipeline uses `mode("overwrite")` rather than Delta `MERGE`. For Stage 1 batch processing this is correct — idempotent, simpler, and faster. A production incremental pipeline would use MERGE to handle late-arriving data.

**Single SparkSession across stages:** Each stage (`ingest`, `transform`, `provision`) initialises its own SparkSession. In a long-running production pipeline these would share a session. The separation here is intentional — each script is independently runnable for debugging, and the 30-minute time limit is comfortably met.

**No Z-Ordering on Gold tables:** At 1M rows, Z-Ordering adds write overhead without meaningful read benefit. The validation queries perform full aggregations — Z-Ordering only helps selective equality/range filters on specific columns. This trade-off would be revisited at 100M+ rows.

---

## Stage 2 and 3 Readiness

Stage 2 (3× volume, DQ injection, schema change) is handled by:
- Config-driven DQ rules in `dq_rules.yaml`
- `dq_report.json` output path already defined in `pipeline_config.yaml`
- All DQ detection logic designed as column expressions, not row-level loops

Stage 3 (streaming) scaffold is in `pipeline/stream_ingest.py` with the interface contract defined. Architecture Decision Record is in `adr/stage3_adr.md`.