"""
Bronze layer ingestion — Nedbank DE Challenge Stage 1
Reads accounts.csv, transactions.jsonl, customers.csv and writes each
to /data/output/bronze/ as Delta Parquet, preserving source data as-is
and adding a single consistent ingestion_timestamp for the entire run.
"""

import logging
import os
import sys
from datetime import datetime, timezone

import yaml
from spark_utils import get_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze.ingest")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = os.environ.get(
        "PIPELINE_CONFIG", "/data/config/pipeline_config.yaml"
    )
    log.info("Loading config from %s", config_path)
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── Core ingest helper ────────────────────────────────────────────────────────
def ingest_source(
    spark: SparkSession,
    *,
    source_name: str,
    read_format: str,
    read_path: str,
    output_path: str,
    ingestion_ts: datetime,
    read_options: dict | None = None,
) -> int:
    """
    Read one source file, attach ingestion_timestamp, write to Delta.

    Returns the row count written.
    """
    log.info("[%s] Reading  %s", source_name, read_path)

    reader = spark.read.format(read_format)
    for k, v in (read_options or {}).items():
        reader = reader.option(k, v)

    df = reader.load(read_path)

    # Attach a single consistent timestamp for the entire run.
    # lit() with a Python datetime produces a deterministic TIMESTAMP literal —
    # NOT current_timestamp(), which would differ per executor/partition.
    ts_lit = lit(ingestion_ts).cast(TimestampType())
    df = df.withColumn("ingestion_timestamp", ts_lit)

    row_count = df.count()
    log.info("[%s] Rows read: %d", source_name, row_count)

    log.info("[%s] Writing Delta to %s", source_name, output_path)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )

    log.info("[%s] Write complete", source_name)
    return row_count


# ── Validation ────────────────────────────────────────────────────────────────
def validate_bronze(spark: SparkSession, output_path: str, source_name: str, expected_rows: int) -> None:
    """
    Read the written Delta table back and assert basic invariants.
    Raises AssertionError on any failure — surfaces as a non-zero exit code.
    """
    df = spark.read.format("delta").load(output_path)
    actual = df.count()

    assert actual == expected_rows, (
        f"[{source_name}] Row count mismatch: wrote {expected_rows}, read back {actual}"
    )
    assert "ingestion_timestamp" in df.columns, (
        f"[{source_name}] ingestion_timestamp column missing from Bronze output"
    )
    null_ts = df.filter(df["ingestion_timestamp"].isNull()).count()
    assert null_ts == 0, (
        f"[{source_name}] {null_ts} rows have null ingestion_timestamp"
    )
    log.info("[%s] Validation PASSED  rows=%d", source_name, actual)


# ── Main ──────────────────────────────────────────────────────────────────────
def run_ingestion() -> None:
    cfg = load_config()
    spark = get_spark(cfg)

    inp = cfg["input"]
    out = cfg["output"]
    bronze_root = out["bronze_path"]

    # One timestamp for the entire ingestion run — consistent across all tables.
    ingestion_ts = datetime.now(timezone.utc)
    log.info("Ingestion run timestamp: %s", ingestion_ts.isoformat())

    # ── accounts.csv ──────────────────────────────────────────────────────────
    accounts_rows = ingest_source(
        spark,
        source_name="accounts",
        read_format="csv",
        read_path=inp["accounts_path"],
        output_path=f"{bronze_root}/accounts",
        ingestion_ts=ingestion_ts,
        read_options={
            "header": "true",
            "inferSchema": "true",   # Bronze preserves raw types; Silver enforces
            "encoding": "UTF-8",
        },
    )
    validate_bronze(spark, f"{bronze_root}/accounts", "accounts", accounts_rows)

    # ── customers.csv ─────────────────────────────────────────────────────────
    customers_rows = ingest_source(
        spark,
        source_name="customers",
        read_format="csv",
        read_path=inp["customers_path"],
        output_path=f"{bronze_root}/customers",
        ingestion_ts=ingestion_ts,
        read_options={
            "header": "true",
            "inferSchema": "true",
            "encoding": "UTF-8",
        },
    )
    validate_bronze(spark, f"{bronze_root}/customers", "customers", customers_rows)

    # ── transactions.jsonl ────────────────────────────────────────────────────
    # spark.read.format("json") reads JSONL natively (one JSON object per line).
    # Nested fields (location.*, metadata.*) are automatically inferred as
    # StructType — we preserve the nested structure in Bronze as-is.
    # merchant_subcategory is absent from Stage 1 records entirely (not null —
    # absent). Spark infers only the keys it finds; the column simply won't
    # exist in Stage 1 Bronze output, which is correct behaviour.
    transactions_rows = ingest_source(
        spark,
        source_name="transactions",
        read_format="json",
        read_path=inp["transactions_path"],
        output_path=f"{bronze_root}/transactions",
        ingestion_ts=ingestion_ts,
        read_options={
            "multiLine": "false",   # JSONL = one object per line; multiLine=false is correct
        },
    )
    validate_bronze(spark, f"{bronze_root}/transactions", "transactions", transactions_rows)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("BRONZE INGESTION COMPLETE")
    log.info("  accounts     : %d rows → %s/accounts", accounts_rows, bronze_root)
    log.info("  customers    : %d rows → %s/customers", customers_rows, bronze_root)
    log.info("  transactions : %d rows → %s/transactions", transactions_rows, bronze_root)
    log.info("  ingestion_ts : %s", ingestion_ts.isoformat())
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        run_ingestion()
        sys.exit(0)
    except Exception as exc:
        log.exception("BRONZE INGESTION FAILED: %s", exc)
        sys.exit(1)