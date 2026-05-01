# Architecture Decision Record — Stage 3 Streaming Extension

**Project:** Nedbank N*ovation DE Challenge — Data Engineering Track
**Candidate:** Tshifhiwa Gift Mayne
**Stage:** 3
**Date:** 2026-05-02

---

## 1. How Stage 1 Architecture Facilitated or Hindered the Streaming Extension

### What helped

**Config-driven design** was the single biggest enabler. Every path, setting, and DQ rule lives in `pipeline_config.yaml` and `dq_rules.yaml`. The streaming extension reads the same config file, so it inherits the correct output paths, Spark settings, and DQ handling rules without duplication.

**`spark_utils.py` as a shared factory** meant the streaming layer got a correctly configured SparkSession — including the `timeParserPolicy=CORRECTED` fix, `127.0.0.1` binding, and Delta extensions — without any extra setup. Roughly 80% of `spark_utils.py` survived intact into Stage 3.

**`parse_date()` coalesce chain** and `normalise_currency()` in `transform.py` were reused directly in `stream_ingest.py`. The streaming events share the same schema and quality variance as Stage 2 batch data, so the same normalisation logic applied without modification.

**`entrypoint.sh` directory pre-creation** already handled the `--read-only` container constraint. Adding `stream_gold/` subdirectories was a two-line change.

### What created friction

**Three separate SparkSessions** (one per batch stage + one for streaming) create startup overhead. Each session initialises the JVM, loads Delta extensions, and reads the config. In a production deployment these would share a long-lived session. The separation was intentional for Stage 1 debuggability, but created measurable overhead at Stage 3 volume.

**`provision.py` writes dimensions first, then re-reads from Delta before building fact.** This is correct for batch consistency but means the streaming layer must wait for the full batch pipeline to complete before `dim_accounts` is available to seed `current_balances`. This sequencing constraint was not visible in Stage 1.

**No shared state abstraction.** The batch pipeline and streaming pipeline share `/data/output/` by convention, not by contract. There is no explicit interface between `run_all.py` and `stream_ingest.py` — the streaming layer assumes the Gold tables exist at a hardcoded path. A cleaner design would pass the Gold path explicitly.

### Code survival rate

- `spark_utils.py`: ~95% reused unchanged
- `parse_date()` / `normalise_currency()`: 100% reused unchanged
- `entrypoint.sh`: ~80% reused, +2 directory entries
- `run_all.py`: ~70% reused, +streaming dispatch block
- `stream_ingest.py`: new module (~320 lines), but ~40% of patterns directly ported from `transform.py` and `provision.py`

---

## 2. Design Decisions I Would Change in Hindsight

**`ingest.py` row counting with `.count()` in `validate_bronze()`** — this was the first anti-pattern flagged in code review. Even after removing most counts, the Bronze validation still calls `.count()` twice per source. With Stage 3 streaming adding a third Spark lifecycle on top, every unnecessary scan compounds. I would replace Bronze validation with schema assertion only — no count.

**Three independent `run_transformation()` / `run_provisioning()` functions that each call `load_config()` and `get_spark()`** — in Stage 1 this was a clean separation of concerns, but it means three JVM starts per container run. With streaming added as a fourth, the startup cost is ~40 seconds across the pipeline. A single-session orchestrator with explicit stage functions would have been faster.

**`fact_transactions` partitioned only by `transaction_date`** — this was the right call for the batch validation queries, but the streaming `recent_transactions` table joins back to batch accounts by `account_id`. If `dim_accounts` had been Z-Ordered on `account_id`, the streaming broadcast join against it would have been faster at 298K rows.

**`dq_reporter.py` calling `.count()` on Silver transactions** — the DQ report reads back from Silver just to count flagged records. With 3M rows this is a full scan. The counts should have been accumulated during the transform stage and passed as a dictionary to the reporter, not re-derived by reading Silver.

---

## 3. Day-1 Architecture With Stage 3 Visibility

If I had known Stage 3 was coming, I would have designed a **unified pipeline entry point with explicit stage contracts**:

### Ingestion patterns

Instead of three separate entry point scripts (`ingest.py`, `transform.py`, `provision.py`), I would have built a single `Pipeline` class that holds the SparkSession and passes it between stages. Each stage would be a method, not an independent process. This eliminates JVM startup overhead and enables shared state (e.g. passing row counts between stages rather than re-reading from Delta).

```python
class NedbanPipeline:
    def __init__(self, cfg):
        self.spark = get_spark(cfg)
        self.cfg = cfg
        self.metrics = {}  # shared accumulator for DQ counts, row counts

    def run_bronze(self): ...
    def run_silver(self): ...
    def run_gold(self): ...
    def run_stream(self): ...
    def write_dq_report(self): ...
```

### State management for streaming

I would have introduced a `StateStore` abstraction on Day 1 — a thin wrapper around Delta merge that encapsulates upsert semantics:

```python
class DeltaStateStore:
    def upsert(self, new_df, merge_keys, update_expr): ...
    def evict_beyond(self, partition_col, order_col, limit): ...
```

Both `current_balances` and `recent_transactions` are instances of this pattern. Building it once and reusing it for both tables would have reduced `stream_ingest.py` by ~80 lines.

### Output structure

I would have added `stream_gold/` to the output specification from Day 1, even if empty, so the directory pre-creation and Delta table initialisation were not afterthoughts. The scoring system checks for `_delta_log/` presence — initialising empty Delta tables in the Dockerfile warmup step would guarantee they exist before the first streaming event arrives.

### Entry point design

A single `run_all.py` that conditionally activates streaming based on whether `/data/stream/` exists — exactly what we ended up with — but designed with this in mind from the start rather than retrofitted. The conditional dispatch pattern we used works well; it just should have been in the original design.

---

*End of ADR — Stage 3 Streaming Extension.*