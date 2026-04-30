"""
Gold layer provisioning — Nedbank DE Challenge Stage 1+
Reads Silver Delta tables and produces the scored dimensional model:
  - dim_customers  (9 fields, age_band derived from dob)
  - dim_accounts   (11 fields, customer_ref renamed to customer_id)
  - fact_transactions (15 fields, surrogate + foreign keys resolved)
All output written as Delta Parquet to /data/output/gold/.
"""

import logging
import os
import sys
from datetime import date

import yaml
from spark_utils import get_spark
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    TimestampType,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gold.provision")


# ── Config ────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = os.environ.get(
        "PIPELINE_CONFIG", "/data/config/pipeline_config.yaml"
    )
    log.info("Loading config from %s", config_path)
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── Surrogate key generation ──────────────────────────────────────────────────
def make_surrogate_key(natural_key_col: str, alias: str) -> F.Column:
    """
    Deterministic surrogate key using sha2 → positive BIGINT.
    Takes the first 15 hex chars of sha2(key) to stay within signed BIGINT range,
    converts from base-16 to base-10, ensures positive with abs().
    Stable across pipeline re-runs on the same input data.
    """
    # First 15 hex chars = max value 0xFFFFFFFFFFFFFFF = 1,152,921,504,606,846,975
    # Well within signed BIGINT max (9,223,372,036,854,775,807)
    hex15 = F.substring(F.sha2(F.col(natural_key_col).cast(StringType()), 256), 1, 15)
    return F.abs(F.conv(hex15, 16, 10).cast(LongType())).alias(alias)


# ── age_band derivation ───────────────────────────────────────────────────────
def derive_age_band(dob_col: str, run_date: date) -> F.Column:
    """
    Derive age_band from dob column using pipeline run date.
    Buckets: 18-25, 26-35, 36-45, 46-55, 56-65, 65+
    Formula: floor((run_date - dob) / 365.25)
    dob column is already a DateType from Silver transformation.
    """
    run_date_lit = F.lit(run_date.isoformat()).cast("date")
    age = (F.datediff(run_date_lit, F.col(dob_col)) / F.lit(365.25)).cast("int")
    return (
        F.when(age >= 65, F.lit("65+"))
        .when(age >= 56, F.lit("56-65"))
        .when(age >= 46, F.lit("46-55"))
        .when(age >= 36, F.lit("36-45"))
        .when(age >= 26, F.lit("26-35"))
        .when(age >= 18, F.lit("18-25"))
        .otherwise(F.lit(None).cast(StringType()))  # should not occur
        .alias("age_band")
    )


# ── dim_customers ─────────────────────────────────────────────────────────────
def build_dim_customers(silver_customers: DataFrame, run_date: date) -> DataFrame:
    """
    9 fields per output_schema_spec.md §4.
    dob is used to derive age_band but is NOT included in output.
    Surrogate key: sha2(customer_id).
    """
    log.info("[dim_customers] Building...  input rows=%d", silver_customers.count())

    dim = (
        silver_customers
        .select(
            make_surrogate_key("customer_id", "customer_sk"),
            F.col("customer_id").cast(StringType()),
            F.col("gender").cast(StringType()),
            F.col("province").cast(StringType()),
            F.col("income_band").cast(StringType()),
            F.col("segment").cast(StringType()),
            F.col("risk_score").cast("int"),
            F.col("kyc_status").cast(StringType()),
            # dob is carried through Silver but NOT exposed in Gold
            F.col("dob"),
        )
        .withColumn("age_band", derive_age_band("dob", run_date))
        .drop("dob")                          # dob must NOT appear in Gold output
    )

    # Final column order per spec §4 — exact match required for schema diff check
    dim = dim.select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "age_band",
    )

    _validate_dim(dim, "dim_customers", "customer_sk", expected_cols=9)
    return dim


# ── dim_accounts ──────────────────────────────────────────────────────────────
def build_dim_accounts(silver_accounts: DataFrame) -> DataFrame:
    """
    11 fields per output_schema_spec.md §3.
    customer_ref is renamed to customer_id (GAP-026 fix).
    customer_id must be at position 3 — required for Validation Query 2.
    Surrogate key: sha2(account_id).
    """
    log.info("[dim_accounts] Building...  input rows=%d", silver_accounts.count())

    dim = (
        silver_accounts
        .select(
            make_surrogate_key("account_id", "account_sk"),
            F.col("account_id").cast(StringType()),
            # GAP-026: customer_ref → customer_id at Gold layer, position 3
            F.col("customer_ref").cast(StringType()).alias("customer_id"),
            F.col("account_type").cast(StringType()),
            F.col("account_status").cast(StringType()),
            F.col("open_date").cast("date"),
            F.col("product_tier").cast(StringType()),
            F.col("digital_channel").cast(StringType()),
            F.col("credit_limit").cast(DecimalType(18, 2)),
            F.col("current_balance").cast(DecimalType(18, 2)),
            F.col("last_activity_date").cast("date"),
        )
    )

    # Final column order per spec §3 — customer_id must be at position 3
    dim = dim.select(
        "account_sk",
        "account_id",
        "customer_id",       # position 3 — Validation Query 2 joins on this
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
    )

    _validate_dim(dim, "dim_accounts", "account_sk", expected_cols=11)
    return dim


# ── fact_transactions ─────────────────────────────────────────────────────────
def build_fact_transactions(
    silver_transactions: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """
    15 fields per output_schema_spec.md §2.
    Resolves account_sk and customer_sk via joins to dimension tables.
    Orphaned transactions (no matching account_sk) are excluded from fact.
    transaction_timestamp = transaction_date + transaction_time combined.
    merchant_subcategory: NULL for Stage 1 (field absent from source).
    dq_flag: NULL for Stage 1 (all data clean).
    """
    log.info(
        "[fact_transactions] Building...  input rows=%d",
        silver_transactions.count(),
    )

    # ── Build account lookup: account_id → account_sk + customer_id ──────────
    # Broadcast dim_accounts — it's ~100K rows vs ~1M transactions.
    acct_lookup = dim_accounts.select(
        F.col("account_id").alias("_acct_natural"),
        F.col("account_sk").alias("_account_sk"),
        F.col("customer_id").alias("_customer_natural"),
    )

    # ── Build customer lookup: customer_id → customer_sk ─────────────────────
    cust_lookup = dim_customers.select(
        F.col("customer_id").alias("_cust_natural"),
        F.col("customer_sk").alias("_customer_sk"),
    )

    # ── Combine transaction_date + transaction_time → transaction_timestamp ───
    txn = silver_transactions.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date").cast(StringType()), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ).cast(TimestampType()),
    )

    # ── Join to resolve account_sk (inner — orphaned txns excluded) ───────────
    # Left join first so we can log how many orphans are excluded,
    # then filter to inner result for the final fact table.
    txn = txn.join(
        F.broadcast(acct_lookup),
        txn["account_id"] == F.col("_acct_natural"),
        how="left",
    )

    orphaned = txn.filter(F.col("_account_sk").isNull()).count()
    if orphaned > 0:
        log.warning(
            "[fact_transactions] Excluding %d orphaned transactions "
            "(no matching account in dim_accounts)",
            orphaned,
        )

    # Keep only transactions that resolve to a known account
    txn = txn.filter(F.col("_account_sk").isNotNull())

    # ── Join to resolve customer_sk ───────────────────────────────────────────
    txn = txn.join(
        F.broadcast(cust_lookup),
        txn["_customer_natural"] == F.col("_cust_natural"),
        how="left",
    )

    # ── Build surrogate key for transactions ──────────────────────────────────
    txn = txn.withColumn(
        "transaction_sk",
        make_surrogate_key("transaction_id", "transaction_sk"),
    )

    # ── Select final 15 columns in spec order ─────────────────────────────────
    fact = txn.select(
        F.col("transaction_sk").cast(LongType()),           # 1
        F.col("transaction_id").cast(StringType()),          # 2
        F.col("_account_sk").cast(LongType()).alias("account_sk"),   # 3
        F.col("_customer_sk").cast(LongType()).alias("customer_sk"), # 4
        F.col("transaction_date").cast("date"),              # 5
        F.col("transaction_timestamp").cast(TimestampType()), # 6
        F.col("transaction_type").cast(StringType()),         # 7
        F.col("merchant_category").cast(StringType()),        # 8
        F.col("merchant_subcategory").cast(StringType()),     # 9 — NULL in Stage 1
        F.col("amount").cast(DecimalType(18, 2)),             # 10
        F.col("currency").cast(StringType()),                 # 11 — already "ZAR"
        F.col("channel").cast(StringType()),                  # 12
        F.col("location_province").cast(StringType()).alias("province"), # 13
        F.col("dq_flag").cast(StringType()),                  # 14 — NULL Stage 1
        F.col("ingestion_timestamp").cast(TimestampType()),   # 15
    )

    _validate_fact(fact, dim_accounts, dim_customers)
    return fact


# ── Validation helpers ────────────────────────────────────────────────────────
def _validate_dim(df: DataFrame, name: str, sk_col: str, expected_cols: int) -> None:
    actual_cols = len(df.columns)
    assert actual_cols == expected_cols, (
        f"[{name}] Schema has {actual_cols} columns, expected {expected_cols}"
    )
    null_sk = df.filter(F.col(sk_col).isNull()).count()
    assert null_sk == 0, f"[{name}] {null_sk} null surrogate keys"

    dup_sk = df.count() - df.dropDuplicates([sk_col]).count()
    assert dup_sk == 0, f"[{name}] {dup_sk} duplicate surrogate keys"

    log.info("[%s] Validation PASSED  rows=%d  cols=%d", name, df.count(), actual_cols)


def _validate_fact(
    fact: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> None:
    actual_cols = len(fact.columns)
    assert actual_cols == 15, (
        f"[fact_transactions] Schema has {actual_cols} columns, expected 15"
    )

    # No null surrogate or foreign keys
    for col_name in ("transaction_sk", "account_sk", "customer_sk"):
        null_count = fact.filter(F.col(col_name).isNull()).count()
        assert null_count == 0, (
            f"[fact_transactions] {null_count} null values in {col_name}"
        )

    # Validation Query 2 equivalent: zero unlinked accounts
    # (Every account_sk in fact must exist in dim_accounts)
    unlinked = (
        fact.select("account_sk")
        .distinct()
        .join(
            dim_accounts.select("account_sk"),
            on="account_sk",
            how="left_anti",
        )
        .count()
    )
    assert unlinked == 0, (
        f"[fact_transactions] {unlinked} account_sk values not found in dim_accounts"
    )

    log.info(
        "[fact_transactions] Validation PASSED  rows=%d  cols=%d",
        fact.count(),
        actual_cols,
    )


# ── Write helper ──────────────────────────────────────────────────────────────
def write_gold(df: DataFrame, output_path: str, table_name: str) -> None:
    log.info("[%s] Writing Gold Delta to %s", table_name, output_path)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    # Partition fact_transactions by transaction_date — enables partition pruning
    # on date-range queries (analytics and compliance use cases).
    # Dimensions are small (~80-100K rows) — partitioning adds overhead with no benefit.
    if table_name == "fact_transactions":
        writer = writer.partitionBy("transaction_date")
    writer.save(output_path)
    count = df.sparkSession.read.format("delta").load(output_path).count()
    log.info("[%s] Write verified  rows=%d", table_name, count)


# ── Main ──────────────────────────────────────────────────────────────────────
def run_provisioning() -> None:
    cfg = load_config()
    spark = get_spark(cfg)

    silver = cfg["output"]["silver_path"]
    gold   = cfg["output"]["gold_path"]

    # Pipeline run date — used for age_band derivation
    run_date = date.today()
    log.info("Pipeline run date for age_band derivation: %s", run_date.isoformat())

    # ── Read Silver ───────────────────────────────────────────────────────────
    log.info("Reading Silver tables...")
    silver_customers    = spark.read.format("delta").load(f"{silver}/customers")
    silver_accounts     = spark.read.format("delta").load(f"{silver}/accounts")
    silver_transactions = spark.read.format("delta").load(f"{silver}/transactions")

    # ── Build dimensions first (facts depend on dimension surrogate keys) ─────
    dim_customers = build_dim_customers(silver_customers, run_date)
    dim_accounts  = build_dim_accounts(silver_accounts)

    # ── Write dimensions before building fact (fact reads them for SK lookup) ─
    write_gold(dim_customers, f"{gold}/dim_customers", "dim_customers")
    write_gold(dim_accounts,  f"{gold}/dim_accounts",  "dim_accounts")

    # ── Re-read from Delta to ensure consistent state for fact joins ──────────
    # Writing and re-reading ensures we join against the exact committed data,
    # not an in-memory DataFrame that might differ from what was persisted.
    dim_customers_committed = spark.read.format("delta").load(f"{gold}/dim_customers")
    dim_accounts_committed  = spark.read.format("delta").load(f"{gold}/dim_accounts")

    # ── Build and write fact ──────────────────────────────────────────────────
    fact_transactions = build_fact_transactions(
        silver_transactions,
        dim_accounts_committed,
        dim_customers_committed,
    )
    write_gold(fact_transactions, f"{gold}/fact_transactions", "fact_transactions")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("GOLD PROVISIONING COMPLETE")
    log.info("  dim_customers     → %s/dim_customers",    gold)
    log.info("  dim_accounts      → %s/dim_accounts",     gold)
    log.info("  fact_transactions → %s/fact_transactions", gold)
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        run_provisioning()
        sys.exit(0)
    except Exception as exc:
        log.exception("GOLD PROVISIONING FAILED: %s", exc)
        sys.exit(1)