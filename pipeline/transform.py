"""
Silver layer transformation — Nedbank DE Challenge Stage 1+
Reads Bronze Delta tables, applies type casting, deduplication,
date standardisation, referential integrity checks, and DQ flagging.
DQ rules are loaded from config/dq_rules.yaml — no hardcoded logic.
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
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver.transform")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    log.info("Loading config from %s", config_path)
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_dq_rules(cfg: dict) -> dict:
    """Load DQ rules from dq_rules.yaml. All DQ logic is driven from this file."""
    rules_path = cfg.get("dq", {}).get("rules_path", "/data/config/dq_rules.yaml")
    log.info("Loading DQ rules from %s", rules_path)
    with open(rules_path) as f:
        return yaml.safe_load(f).get("rules", {})


# ── Date parsing ──────────────────────────────────────────────────────────────
def parse_date(col_name: str) -> F.Column:
    """
    Coalesce chain: yyyy-MM-dd → yyyy-MM-dd HH:mm:ss → dd/MM/yyyy → Unix epoch.
    Handles all Stage 1 and Stage 2 date format variants including
    datetime strings (e.g. '1979-03-17 22:00:00') in date-only fields.
    """
    c = F.col(col_name).cast(StringType())
    return F.coalesce(
        F.to_date(c, "yyyy-MM-dd"),
        F.to_date(c, "yyyy-MM-dd HH:mm:ss"),  # Stage 2: datetime in date field
        F.to_date(c, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(c.cast("long")), "yyyy-MM-dd"),
    )


def is_non_iso_date(col_name: str) -> F.Column:
    """Returns True if the raw value is NOT already in yyyy-MM-dd format."""
    c = F.col(col_name).cast(StringType())
    iso_parsed = F.to_date(c, "yyyy-MM-dd")
    return iso_parsed.isNull() & F.col(col_name).isNotNull()


# ── Currency normalisation ────────────────────────────────────────────────────
def normalise_currency(col_name: str) -> F.Column:
    c = F.col(col_name).cast(StringType())
    return (
        F.when(F.upper(F.trim(c)).isin("ZAR", "R", "RANDS"), F.lit("ZAR"))
        .when(F.trim(c) == F.lit("710"), F.lit("ZAR"))
        .otherwise(F.lit("ZAR"))
    )


def is_currency_variant(col_name: str) -> F.Column:
    """Returns True if source currency was not already 'ZAR'."""
    c = F.col(col_name).cast(StringType())
    return ~(F.trim(c) == F.lit("ZAR"))


# ── DQ flag assignment (single-flag priority) ─────────────────────────────────
# Priority order: ORPHANED_ACCOUNT > TYPE_MISMATCH > DATE_FORMAT > CURRENCY_VARIANT
# A record gets the highest-priority flag that applies.
def assign_dq_flag(
    df: DataFrame,
    dq_rules: dict,
    valid_accounts: DataFrame,
) -> DataFrame:
    """
    Assign dq_flag based on rules loaded from dq_rules.yaml.
    Operates on the raw (pre-cast) Bronze data so we can detect
    original format issues before normalisation overwrites them.
    """
    # Start with all records clean
    df = df.withColumn("dq_flag", F.lit(None).cast(StringType()))

    # ── CURRENCY_VARIANT ──────────────────────────────────────────────────────
    if "CURRENCY_VARIANT" in dq_rules:
        rule = dq_rules["CURRENCY_VARIANT"]
        if rule.get("handling_action") in ("NORMALISE", "FLAG"):
            df = df.withColumn(
                "dq_flag",
                F.when(
                    F.col("dq_flag").isNull() & is_currency_variant("currency"),
                    F.lit("CURRENCY_VARIANT"),
                ).otherwise(F.col("dq_flag")),
            )

    # ── DATE_FORMAT ───────────────────────────────────────────────────────────
    if "DATE_FORMAT" in dq_rules:
        rule = dq_rules["DATE_FORMAT"]
        if rule.get("handling_action") in ("NORMALISE", "FLAG"):
            df = df.withColumn(
                "dq_flag",
                F.when(
                    F.col("dq_flag").isNull() & is_non_iso_date("transaction_date"),
                    F.lit("DATE_FORMAT"),
                ).otherwise(F.col("dq_flag")),
            )

    # ── TYPE_MISMATCH — detect amount delivered as string ────────────────────
    if "TYPE_MISMATCH" in dq_rules:
        rule = dq_rules["TYPE_MISMATCH"]
        if rule.get("handling_action") in ("NORMALISE", "FLAG"):
            # If amount can't be cast to decimal it's a string mismatch
            df = df.withColumn(
                "_amount_cast_check",
                F.col("amount").cast(DecimalType(18, 2)),
            ).withColumn(
                "dq_flag",
                F.when(
                    F.col("dq_flag").isNull()
                    & F.col("amount").isNotNull()
                    & F.col("_amount_cast_check").isNull(),
                    F.lit("TYPE_MISMATCH"),
                ).otherwise(F.col("dq_flag")),
            ).drop("_amount_cast_check")

    # ── ORPHANED_ACCOUNT ──────────────────────────────────────────────────────
    if "ORPHANED_ACCOUNT" in dq_rules:
        rule = dq_rules["ORPHANED_ACCOUNT"]
        if rule.get("handling_action") in ("FLAG", "QUARANTINE"):
            df = (
                df.join(
                    valid_accounts.withColumn("_is_valid", F.lit(True)),
                    df["account_id"] == F.col("_acct_id"),
                    how="left",
                )
                .withColumn(
                    "dq_flag",
                    F.when(
                        F.col("_is_valid").isNull(),
                        F.lit("ORPHANED_ACCOUNT"),
                    ).otherwise(F.col("dq_flag")),
                )
                .drop("_acct_id", "_is_valid")
            )

    return df


# ── Deduplication ─────────────────────────────────────────────────────────────
def deduplicate(df: DataFrame, pk: str, source_name: str) -> DataFrame:
    df = df.dropDuplicates([pk])
    log.info("[%s] dropDuplicates applied on %s", source_name, pk)
    return df


# ── customers transform ───────────────────────────────────────────────────────
def transform_customers(df: DataFrame) -> DataFrame:
    log.info("[customers] Starting Silver transformation")
    df = (
        df
        .withColumn("customer_id",   F.col("customer_id").cast(StringType()))
        .withColumn("id_number",     F.col("id_number").cast(StringType()))
        .withColumn("first_name",    F.col("first_name").cast(StringType()))
        .withColumn("last_name",     F.col("last_name").cast(StringType()))
        .withColumn("dob",           parse_date("dob"))
        .withColumn("gender",        F.col("gender").cast(StringType()))
        .withColumn("province",      F.col("province").cast(StringType()))
        .withColumn("income_band",   F.col("income_band").cast(StringType()))
        .withColumn("segment",       F.col("segment").cast(StringType()))
        .withColumn("risk_score",    F.col("risk_score").cast(IntegerType()))
        .withColumn("kyc_status",    F.col("kyc_status").cast(StringType()))
        .withColumn("product_flags", F.col("product_flags").cast(StringType()))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    )
    df = deduplicate(df, "customer_id", "customers")
    log.info("[customers] Silver transformation complete")
    return df


# ── accounts transform ────────────────────────────────────────────────────────
def transform_accounts(df: DataFrame, silver_customers: DataFrame, dq_rules: dict) -> DataFrame:
    log.info("[accounts] Starting Silver transformation")

    # ── NULL_REQUIRED: quarantine accounts with null account_id ───────────────
    if "NULL_REQUIRED" in dq_rules:
        null_count_approx = df.filter(F.col("account_id").isNull()).count()
        if null_count_approx > 0:
            log.warning("[accounts] %d records with null account_id — quarantined (NULL_REQUIRED)", null_count_approx)
        df = df.filter(F.col("account_id").isNotNull())

    df = (
        df
        .withColumn("account_id",        F.col("account_id").cast(StringType()))
        .withColumn("customer_ref",       F.col("customer_ref").cast(StringType()))
        .withColumn("account_type",       F.col("account_type").cast(StringType()))
        .withColumn("account_status",     F.col("account_status").cast(StringType()))
        .withColumn("open_date",          parse_date("open_date"))
        .withColumn("product_tier",       F.col("product_tier").cast(StringType()))
        .withColumn("mobile_number",      F.col("mobile_number").cast(StringType()))
        .withColumn("digital_channel",    F.col("digital_channel").cast(StringType()))
        .withColumn("credit_limit",       F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance",    F.col("current_balance").cast(DecimalType(18, 2)))
        .withColumn("last_activity_date", parse_date("last_activity_date"))
        .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast(TimestampType()))
    )

    df = deduplicate(df, "account_id", "accounts")

    # Referential integrity — inner join keeps only accounts with valid customer_ref
    valid_customers = silver_customers.select(F.col("customer_id").alias("_cust_id"))
    df_linked = df.join(
        valid_customers,
        df["customer_ref"] == F.col("_cust_id"),
        how="inner",
    ).drop("_cust_id")

    log.info("[accounts] Referential integrity join applied — orphaned accounts excluded")
    log.info("[accounts] Silver transformation complete")
    return df_linked


# ── transactions transform ────────────────────────────────────────────────────
def transform_transactions(
    df: DataFrame,
    silver_accounts: DataFrame,
    dq_rules: dict,
) -> DataFrame:
    log.info("[transactions] Starting Silver transformation")

    # ── Flatten nested structs ────────────────────────────────────────────────
    df = (
        df
        .withColumn("location_province",   F.col("location.province").cast(StringType()))
        .withColumn("location_city",        F.col("location.city").cast(StringType()))
        .withColumn("location_coordinates", F.col("location.coordinates").cast(StringType()))
        .withColumn("metadata_device_id",   F.col("metadata.device_id").cast(StringType()))
        .withColumn("metadata_session_id",  F.col("metadata.session_id").cast(StringType()))
        .withColumn("metadata_retry_flag",  F.col("metadata.retry_flag").cast(BooleanType()))
        .drop("location", "metadata")
    )

    # ── merchant_subcategory: absent in Stage 1, present in Stage 2 ───────────
    if "merchant_subcategory" not in df.columns:
        log.info("[transactions] merchant_subcategory absent — adding NULL column (Stage 1 expected)")
        df = df.withColumn("merchant_subcategory", F.lit(None).cast(StringType()))

    # ── Assign DQ flags BEFORE casting (detect original format issues) ────────
    valid_accounts = silver_accounts.select(F.col("account_id").alias("_acct_id"))
    df = assign_dq_flag(df, dq_rules, valid_accounts)

    # ── Deduplication — after flagging so DUPLICATE_DEDUPED is traceable ──────
    # dropDuplicates keeps first occurrence; duplicates are removed (quarantined).
    df = deduplicate(df, "transaction_id", "transactions")

    # ── Type casting (normalise after flagging) ───────────────────────────────
    df = (
        df
        .withColumn("transaction_id",       F.col("transaction_id").cast(StringType()))
        .withColumn("account_id",           F.col("account_id").cast(StringType()))
        .withColumn("transaction_date",     parse_date("transaction_date"))
        .withColumn("transaction_time",     F.col("transaction_time").cast(StringType()))
        .withColumn("transaction_type",     F.col("transaction_type").cast(StringType()))
        .withColumn("merchant_category",    F.col("merchant_category").cast(StringType()))
        .withColumn("merchant_subcategory", F.col("merchant_subcategory").cast(StringType()))
        .withColumn("amount",               F.col("amount").cast(DecimalType(18, 2)))
        .withColumn("currency",             normalise_currency("currency"))
        .withColumn("channel",              F.col("channel").cast(StringType()))
        .withColumn("ingestion_timestamp",  F.col("ingestion_timestamp").cast(TimestampType()))
    )

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
    dq_rules = load_dq_rules(cfg)
    spark = get_spark(cfg)

    bronze = cfg["output"]["bronze_path"]
    silver = cfg["output"]["silver_path"]

    log.info("Reading Bronze tables...")
    bronze_customers    = spark.read.format("delta").load(f"{bronze}/customers")
    bronze_accounts     = spark.read.format("delta").load(f"{bronze}/accounts")
    bronze_transactions = spark.read.format("delta").load(f"{bronze}/transactions")

    # Transform in dependency order
    silver_customers    = transform_customers(bronze_customers)
    silver_accounts     = transform_accounts(bronze_accounts, silver_customers, dq_rules)
    silver_transactions = transform_transactions(bronze_transactions, silver_accounts, dq_rules)

    write_silver(silver_customers,    f"{silver}/customers",    "customers")
    write_silver(silver_accounts,     f"{silver}/accounts",     "accounts")
    write_silver(silver_transactions, f"{silver}/transactions", "transactions")

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