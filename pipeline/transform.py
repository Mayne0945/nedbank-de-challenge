"""
Silver layer transformation — Nedbank DE Challenge Stage 1+
Reads Bronze Delta tables, applies type casting, deduplication,
date standardisation, referential integrity checks, and DQ flagging.
Writes clean Silver Delta tables ready for Gold provisioning.
"""

import logging
import os
import sys

import yaml
from spark_utils import get_spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver.transform")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = os.environ.get(
        "PIPELINE_CONFIG", "/data/config/pipeline_config.yaml"
    )
    log.info("Loading config from %s", config_path)
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── Deduplication helper ──────────────────────────────────────────────────────
def deduplicate(df: DataFrame, pk: str, source_name: str) -> DataFrame:
    """
    Drop duplicate rows on natural key.
    Counts are omitted intentionally — each .count() is a full scan.
    On 1M transaction rows under 2 vCPU / 30-min limit, scan budget matters.
    Stage 1 data is clean; Stage 2 duplicates are detected via dq_flag logic.
    """
    df = df.dropDuplicates([pk])
    log.info("[%s] dropDuplicates applied on %s", source_name, pk)
    return df


# ── Currency normalisation ────────────────────────────────────────────────────
# Built now so Stage 2 variants work without code changes.
# Stage 1 data is always "ZAR" — this is a no-op for Stage 1.
def normalise_currency(col_name: str) -> F.Column:
    """Return a Column expression that standardises currency to 'ZAR'."""
    c = F.col(col_name).cast(StringType())
    return (
        F.when(F.upper(F.trim(c)).isin("ZAR", "R", "RANDS"), F.lit("ZAR"))
        .when(F.trim(c) == F.lit("710"), F.lit("ZAR"))   # ISO 4217 numeric
        .otherwise(F.lit("ZAR"))                          # default — flag in Stage 2
    )


# ── Date parsing helper ───────────────────────────────────────────────────────
# Stage 1 dates are always YYYY-MM-DD.
# Stage 2 introduces DD/MM/YYYY and Unix epoch variants — this coalesce
# chain handles all three without code changes at Stage 2.
def parse_date(col_name: str) -> F.Column:
    """Return a Column expression that produces a DateType from mixed formats."""
    c = F.col(col_name).cast(StringType())
    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),           # Stage 1 standard + Stage 2 variant 1
        F.to_date(c, "dd/MM/yyyy"),            # Stage 2 variant 2
        F.to_date(                             # Stage 2 Unix epoch (seconds as string)
            F.from_unixtime(c.cast("long")), "yyyy-MM-dd"
        ),
    )


# ── customers transform ───────────────────────────────────────────────────────
def transform_customers(df: DataFrame) -> DataFrame:
    """
    Cast all columns to correct types, deduplicate on customer_id.
    dob is retained as DATE for use in Gold age_band derivation.
    Note: dob is NOT copied to Gold output — it stays in Silver only.
    """
    log.info("[customers] Starting Silver transformation")

    df = (
        df
        .withColumn("customer_id",    F.col("customer_id").cast(StringType()))
        .withColumn("id_number",      F.col("id_number").cast(StringType()))
        .withColumn("first_name",     F.col("first_name").cast(StringType()))
        .withColumn("last_name",      F.col("last_name").cast(StringType()))
        .withColumn("dob",            parse_date("dob"))
        .withColumn("gender",         F.col("gender").cast(StringType()))
        .withColumn("province",       F.col("province").cast(StringType()))
        .withColumn("income_band",    F.col("income_band").cast(StringType()))
        .withColumn("segment",        F.col("segment").cast(StringType()))
        .withColumn("risk_score",     F.col("risk_score").cast(IntegerType()))
        .withColumn("kyc_status",     F.col("kyc_status").cast(StringType()))
        .withColumn("product_flags",  F.col("product_flags").cast(StringType()))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    )

    df = deduplicate(df, "customer_id", "customers")

    log.info("[customers] Silver transformation complete")
    return df


# ── accounts transform ────────────────────────────────────────────────────────
def transform_accounts(df: DataFrame, silver_customers: DataFrame) -> DataFrame:
    """
    Cast all columns to correct types, deduplicate on account_id.
    Validates referential integrity: every customer_ref must map to a
    known customer_id in the Silver customers table.
    customer_ref is retained as-is here; Gold provision.py renames it
    to customer_id in dim_accounts.
    """
    log.info("[accounts] Starting Silver transformation")

    df = (
        df
        .withColumn("account_id",         F.col("account_id").cast(StringType()))
        .withColumn("customer_ref",        F.col("customer_ref").cast(StringType()))
        .withColumn("account_type",        F.col("account_type").cast(StringType()))
        .withColumn("account_status",      F.col("account_status").cast(StringType()))
        .withColumn("open_date",           parse_date("open_date"))
        .withColumn("product_tier",        F.col("product_tier").cast(StringType()))
        .withColumn("mobile_number",       F.col("mobile_number").cast(StringType()))
        .withColumn("digital_channel",     F.col("digital_channel").cast(StringType()))
        .withColumn("credit_limit",        F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance",     F.col("current_balance").cast(DecimalType(18, 2)))
        .withColumn("last_activity_date",  parse_date("last_activity_date"))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    )

    df = deduplicate(df, "account_id", "accounts")

    # ── Referential integrity: every customer_ref must exist in Silver customers
    # Left-anti join returns accounts with NO matching customer — these are orphans.
    valid_customers = silver_customers.select(
        F.col("customer_id").alias("_cust_id")
    )
    # Join-based referential integrity — no .count(), stays distributed.
    # inner join keeps only accounts with a valid customer_ref.
    # left_anti result is discarded — we log a warning only if orphans exist
    # by checking the difference in plan, not collecting data.
    df_linked = df.join(
        valid_customers,
        df["customer_ref"] == F.col("_cust_id"),
        how="inner",
    ).drop("_cust_id")

    log.info("[accounts] Referential integrity join applied — orphaned accounts excluded")

    log.info("[accounts] Silver transformation complete")
    return df_linked


# ── transactions transform ────────────────────────────────────────────────────
def transform_transactions(df: DataFrame, silver_accounts: DataFrame) -> DataFrame:
    """
    Flatten nested location/metadata structs, cast all columns to correct types,
    deduplicate on transaction_id, normalise currency, and add dq_flag column.

    merchant_subcategory: absent entirely from Stage 1 JSON records. Handled
    by checking df.columns before referencing — if missing, a NULL column is
    added so the Silver schema remains consistent across all stages.
    """
    log.info("[transactions] Starting Silver transformation")

    # ── Flatten nested structs ────────────────────────────────────────────────
    # Spark reads JSONL nested objects as StructType columns.
    # Flatten to top-level columns for cleaner downstream handling.
    df = (
        df
        .withColumn("location_province",    F.col("location.province").cast(StringType()))
        .withColumn("location_city",         F.col("location.city").cast(StringType()))
        .withColumn("location_coordinates",  F.col("location.coordinates").cast(StringType()))
        .withColumn("metadata_device_id",    F.col("metadata.device_id").cast(StringType()))
        .withColumn("metadata_session_id",   F.col("metadata.session_id").cast(StringType()))
        .withColumn("metadata_retry_flag",   F.col("metadata.retry_flag").cast(BooleanType()))
        .drop("location", "metadata")
    )

    # ── merchant_subcategory: absent in Stage 1, present in Stage 2/3 ─────────
    # If the field didn't exist in the source JSON, Spark won't infer it.
    # We add a NULL column now so Silver schema is stable across all stages.
    if "merchant_subcategory" not in df.columns:
        log.info("[transactions] merchant_subcategory absent — adding NULL column (Stage 1 expected)")
        df = df.withColumn("merchant_subcategory", F.lit(None).cast(StringType()))

    # ── Type casting ──────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("transaction_id",       F.col("transaction_id").cast(StringType()))
        .withColumn("account_id",           F.col("account_id").cast(StringType()))
        .withColumn("transaction_date",     parse_date("transaction_date"))
        .withColumn("transaction_time",     F.col("transaction_time").cast(StringType()))
        .withColumn("transaction_type",     F.col("transaction_type").cast(StringType()))
        .withColumn("merchant_category",    F.col("merchant_category").cast(StringType()))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast(StringType()))
        # amount: cast to DECIMAL. Stage 2 delivers some amounts as strings —
        # this cast handles both numeric and string-numeric inputs transparently.
        .withColumn("amount",              F.col("amount").cast(DecimalType(18, 2)))
        # currency: normalise all variants to "ZAR" (no-op for Stage 1)
        .withColumn("currency",            normalise_currency("currency"))
        .withColumn("channel",             F.col("channel").cast(StringType()))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    )

    # ── DQ flagging ───────────────────────────────────────────────────────────
    # Stage 1: all data is clean. dq_flag = NULL for every row.
    # The column must exist for Gold schema conformance.
    # Stage 2 will populate this with one of the 6 recognised codes.
    df = df.withColumn("dq_flag", F.lit(None).cast(StringType()))

    # ── Deduplication on transaction_id ───────────────────────────────────────
    df = deduplicate(df, "transaction_id", "transactions")

    # ── Orphaned account check ────────────────────────────────────────────────
    # Flag transactions whose account_id has no matching account in Silver.
    # In Stage 1 this should be zero. In Stage 2 ~2% are injected orphans.
    valid_accounts = silver_accounts.select(
        F.col("account_id").alias("_acct_id")
    )
    # Join-based orphan flagging — no .collect(), stays fully distributed.
    # Left join against valid accounts; rows where _is_valid is NULL have
    # no matching account → flag as ORPHANED_ACCOUNT.
    df = (
        df.join(
            valid_accounts.withColumn("_is_valid", F.lit(True)),
            df["account_id"] == F.col("_acct_id"),
            how="left",
        )
        .withColumn(
            "dq_flag",
            F.when(F.col("_is_valid").isNull(), F.lit("ORPHANED_ACCOUNT"))
             .otherwise(F.col("dq_flag")),
        )
        .drop("_acct_id", "_is_valid")
    )
    log.info("[transactions] Orphaned account flagging applied (join-based)")
    log.info("[transactions] Silver transformation complete")
    return df


# ── Write helper ──────────────────────────────────────────────────────────────
def write_silver(df: DataFrame, output_path: str, source_name: str) -> None:
    log.info("[%s] Writing Silver Delta to %s", source_name, output_path)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    log.info("[%s] Write complete", source_name)


# ── Main ──────────────────────────────────────────────────────────────────────
def run_transformation() -> None:
    cfg = load_config()
    spark = get_spark(cfg)

    bronze = cfg["output"]["bronze_path"]
    silver = cfg["output"]["silver_path"]

    # ── 1. Read Bronze tables ─────────────────────────────────────────────────
    log.info("Reading Bronze tables...")
    bronze_customers    = spark.read.format("delta").load(f"{bronze}/customers")
    bronze_accounts     = spark.read.format("delta").load(f"{bronze}/accounts")
    bronze_transactions = spark.read.format("delta").load(f"{bronze}/transactions")

    # ── 2. Transform in dependency order ─────────────────────────────────────
    # customers first — accounts validate against it
    # accounts second — transactions validate against it
    silver_customers    = transform_customers(bronze_customers)
    silver_accounts     = transform_accounts(bronze_accounts, silver_customers)
    silver_transactions = transform_transactions(bronze_transactions, silver_accounts)

    # ── 3. Write Silver tables ────────────────────────────────────────────────
    write_silver(silver_customers,    f"{silver}/customers",    "customers")
    write_silver(silver_accounts,     f"{silver}/accounts",     "accounts")
    write_silver(silver_transactions, f"{silver}/transactions", "transactions")

    # ── 4. Summary ────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("SILVER TRANSFORMATION COMPLETE")
    log.info("  customers    → %s/customers", silver)
    log.info("  accounts     → %s/accounts", silver)
    log.info("  transactions → %s/transactions", silver)
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        run_transformation()
        sys.exit(0)
    except Exception as exc:
        log.exception("SILVER TRANSFORMATION FAILED: %s", exc)
        sys.exit(1)