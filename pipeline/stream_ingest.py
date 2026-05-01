"""
Stage 3 streaming ingestion — Nedbank DE Challenge
Processes micro-batch JSONL files from /data/stream/ in filename order.
Updates stream_gold/current_balances (upsert) and
stream_gold/recent_transactions (last-50 per account merge).
All output written as Delta Parquet.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    TimestampType,
)
from delta.tables import DeltaTable

from spark_utils import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("stream.ingest")

# ── Constants ─────────────────────────────────────────────────────────────────
STREAM_DIR          = "/data/stream"
CB_PATH             = "/data/output/stream_gold/current_balances"
RT_PATH             = "/data/output/stream_gold/recent_transactions"
PROCESSED_FILE      = "/tmp/stream_processed.txt"
POLL_INTERVAL_SECS  = 30
QUIESCE_SECS        = 90    # exit after this many seconds with no new files
MAX_RECENT          = 50    # retain last N transactions per account


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── Processed file tracking ───────────────────────────────────────────────────
def load_processed() -> set:
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE) as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_processed(filename: str) -> None:
    with open(PROCESSED_FILE, "a") as f:
        f.write(filename + "\n")


# ── Parse one stream file ──────────────────────────────────────────────────────
def parse_stream_file(spark: SparkSession, filepath: str):
    """
    Read a JSONL stream file and return a cleaned DataFrame.
    Schema is identical to Stage 2 transactions.
    """
    df = spark.read.json(filepath)

    # Flatten nested structs
    if "location" in df.columns:
        df = df.withColumn("location_province", F.col("location.province").cast(StringType())) \
               .drop("location")
    else:
        df = df.withColumn("location_province", F.lit(None).cast(StringType()))

    if "metadata" in df.columns:
        df = df.drop("metadata")

    # Handle merchant_subcategory absent/null
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast(StringType()))

    # Combine transaction_date + transaction_time → transaction_timestamp
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ",
                F.col("transaction_date").cast(StringType()),
                F.col("transaction_time").cast(StringType())
            ),
            "yyyy-MM-dd HH:mm:ss",
        ).cast(TimestampType()),
    )

    # Normalise currency and cast amount
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2))) \
           .withColumn("currency",
               F.when(F.upper(F.trim(F.col("currency").cast(StringType()))).isin(
                   "ZAR", "R", "RANDS"), F.lit("ZAR"))
               .when(F.trim(F.col("currency").cast(StringType())) == "710", F.lit("ZAR"))
               .otherwise(F.lit("ZAR"))
           ) \
           .withColumn("transaction_id", F.col("transaction_id").cast(StringType())) \
           .withColumn("account_id",     F.col("account_id").cast(StringType())) \
           .withColumn("transaction_type", F.col("transaction_type").cast(StringType())) \
           .withColumn("channel", F.col("channel").cast(StringType()) if "channel" in df.columns else F.lit(None).cast(StringType()))

    # Deduplicate on transaction_id within this micro-batch
    df = df.dropDuplicates(["transaction_id"])

    return df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("account_id").isNotNull() &
        F.col("amount").isNotNull()
    )


# ── Update current_balances ───────────────────────────────────────────────────
def update_current_balances(spark: SparkSession, events_df, updated_at: datetime) -> None:
    """
    Upsert current_balances: one row per account_id.
    Balance delta = sum(CREDIT + REVERSAL) - sum(DEBIT + FEE) for the batch.
    On first encounter for an account, seed balance from dim_accounts if available.
    """
    updated_at_lit = F.lit(updated_at.isoformat()).cast(TimestampType())

    # Compute net balance delta and latest timestamp per account in this batch
    batch_agg = events_df.groupBy("account_id").agg(
        F.sum(
            F.when(F.col("transaction_type").isin("CREDIT", "REVERSAL"),  F.col("amount"))
             .when(F.col("transaction_type").isin("DEBIT", "FEE"),       -F.col("amount"))
             .otherwise(F.lit(0).cast(DecimalType(18, 2)))
        ).alias("balance_delta"),
        F.max("transaction_timestamp").alias("last_transaction_timestamp"),
    )

    batch_agg = batch_agg \
        .withColumn("updated_at", updated_at_lit) \
        .withColumn("balance_delta", F.col("balance_delta").cast(DecimalType(18, 2)))

    os.makedirs(CB_PATH, exist_ok=True)

    if DeltaTable.isDeltaTable(spark, CB_PATH):
        cb_table = DeltaTable.forPath(spark, CB_PATH)
        (
            cb_table.alias("existing")
            .merge(
                batch_agg.alias("updates"),
                "existing.account_id = updates.account_id"
            )
            .whenMatchedUpdate(set={
                "current_balance":          "existing.current_balance + updates.balance_delta",
                "last_transaction_timestamp": "updates.last_transaction_timestamp",
                "updated_at":               "updates.updated_at",
            })
            .whenNotMatchedInsert(values={
                "account_id":               "updates.account_id",
                "current_balance":          "updates.balance_delta",
                "last_transaction_timestamp": "updates.last_transaction_timestamp",
                "updated_at":               "updates.updated_at",
            })
            .execute()
        )
    else:
        # First write — seed from batch dim_accounts current_balance where available
        gold_path = "/data/output/gold"
        try:
            dim_accts = spark.read.format("delta").load(f"{gold_path}/dim_accounts") \
                .select(
                    F.col("account_id"),
                    F.col("current_balance").alias("seed_balance"),
                )
            batch_agg = batch_agg.join(
                F.broadcast(dim_accts), on="account_id", how="left"
            ).withColumn(
                "current_balance",
                F.coalesce(F.col("seed_balance"), F.lit(0).cast(DecimalType(18, 2)))
                + F.col("balance_delta")
            ).drop("seed_balance", "balance_delta")
        except Exception:
            batch_agg = batch_agg.withColumn(
                "current_balance", F.col("balance_delta")
            ).drop("balance_delta")

        batch_agg.select(
            "account_id", "current_balance", "last_transaction_timestamp", "updated_at"
        ).write.format("delta").mode("overwrite").save(CB_PATH)

    log.info("[stream] current_balances updated — %d accounts", batch_agg.count())


# ── Update recent_transactions ────────────────────────────────────────────────
def update_recent_transactions(spark: SparkSession, events_df, updated_at: datetime) -> None:
    """
    Merge new events into recent_transactions (keyed on account_id + transaction_id).
    After each merge, evict rows beyond position 50 per account.
    """
    updated_at_lit = F.lit(updated_at.isoformat()).cast(TimestampType())

    new_rows = events_df.select(
        F.col("account_id"),
        F.col("transaction_id"),
        F.col("transaction_timestamp"),
        F.col("amount").cast(DecimalType(18, 2)),
        F.col("transaction_type"),
        F.col("channel"),
        updated_at_lit.alias("updated_at"),
    )

    os.makedirs(RT_PATH, exist_ok=True)

    if DeltaTable.isDeltaTable(spark, RT_PATH):
        rt_table = DeltaTable.forPath(spark, RT_PATH)
        (
            rt_table.alias("existing")
            .merge(
                new_rows.alias("updates"),
                "existing.account_id = updates.account_id AND "
                "existing.transaction_id = updates.transaction_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_rows.write.format("delta").mode("overwrite").save(RT_PATH)

    # ── Evict rows beyond 50 per account ──────────────────────────────────────
    # Read full table, rank by timestamp desc per account, delete rank > 50.
    from pyspark.sql import Window
    rt_table = DeltaTable.forPath(spark, RT_PATH)
    full_df = rt_table.toDF()

    w = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())
    ranked = full_df.withColumn("_rn", F.row_number().over(w))
    to_delete = ranked.filter(F.col("_rn") > MAX_RECENT).select("account_id", "transaction_id")

    if to_delete.count() > 0:
        rt_table.alias("rt").merge(
            to_delete.alias("del"),
            "rt.account_id = del.account_id AND rt.transaction_id = del.transaction_id"
        ).whenMatchedDelete().execute()
        log.info("[stream] Evicted %d rows beyond top-%d per account", to_delete.count(), MAX_RECENT)


# ── Process one file ──────────────────────────────────────────────────────────
def process_file(spark: SparkSession, filepath: str) -> None:
    filename = os.path.basename(filepath)
    log.info("[stream] Processing %s", filename)

    events_df = parse_stream_file(spark, filepath)
    event_count = events_df.count()
    log.info("[stream] %s — %d events parsed", filename, event_count)

    if event_count == 0:
        log.warning("[stream] %s — no valid events, skipping", filename)
        return

    updated_at = datetime.now(timezone.utc)
    update_current_balances(spark, events_df, updated_at)
    update_recent_transactions(spark, events_df, updated_at)
    mark_processed(filename)
    log.info("[stream] %s — done", filename)


# ── Main polling loop ─────────────────────────────────────────────────────────
def run_stream_ingestion() -> None:
    cfg = load_config()
    spark = get_spark(cfg)

    os.makedirs("/data/output/stream_gold", exist_ok=True)

    processed = load_processed()
    last_new_file_time = time.time()

    log.info("[stream] Starting polling loop  dir=%s  quiesce=%ds", STREAM_DIR, QUIESCE_SECS)

    while True:
        # List all JSONL files in stream dir, sorted lexicographically (= chronologically)
        try:
            all_files = sorted([
                f for f in os.listdir(STREAM_DIR)
                if f.endswith(".jsonl")
            ])
        except FileNotFoundError:
            log.warning("[stream] %s not found — waiting", STREAM_DIR)
            time.sleep(POLL_INTERVAL_SECS)
            continue

        new_files = [f for f in all_files if f not in processed]

        if new_files:
            last_new_file_time = time.time()
            for filename in new_files:
                filepath = os.path.join(STREAM_DIR, filename)
                try:
                    process_file(spark, filepath)
                except Exception as exc:
                    log.exception("[stream] Failed to process %s: %s", filename, exc)
                    mark_processed(filename)
                processed.add(filename)  # update in-memory set so next cycle skips it
        else:
            idle_secs = time.time() - last_new_file_time
            log.info("[stream] No new files  idle=%.0fs / quiesce=%ds", idle_secs, QUIESCE_SECS)
            if idle_secs >= QUIESCE_SECS:
                log.info("[stream] Quiesce timeout reached — exiting polling loop")
                break
            time.sleep(POLL_INTERVAL_SECS)

    log.info("[stream] Streaming ingestion complete")
    log.info("[stream]   Files processed: %d", len(load_processed()))


if __name__ == "__main__":
    try:
        run_stream_ingestion()
        sys.exit(0)
    except Exception as exc:
        log.exception("STREAM INGESTION FAILED: %s", exc)
        sys.exit(1)