"""
DQ Report generator — Nedbank DE Challenge Stage 2+
Reads Silver transactions, computes DQ issue counts,
and writes /data/output/dq_report.json.
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("dq_reporter")

DQ_CODES = [
    "DUPLICATE_DEDUPED",
    "ORPHANED_ACCOUNT",
    "TYPE_MISMATCH",
    "DATE_FORMAT",
    "CURRENCY_VARIANT",
    "NULL_REQUIRED",
]


def generate_dq_report(
    cfg: dict,
    spark: SparkSession,
    run_timestamp: datetime,
    execution_duration_seconds: float,
    source_counts: dict,
) -> None:
    """
    Read Silver transactions, count dq_flag occurrences,
    and write dq_report.json to /data/output/.
    """
    silver = cfg["output"]["silver_path"]
    output_path = cfg["output"].get("dq_report_path", "/data/output/dq_report.json")

    log.info("Generating DQ report...")

    # Count flagged records per issue code
    txn_df = spark.read.format("delta").load(f"{silver}/transactions")
    total_txn = txn_df.count()

    flag_counts = (
        txn_df
        .filter(F.col("dq_flag").isNotNull())
        .groupBy("dq_flag")
        .count()
        .collect()
    )
    flag_map = {row["dq_flag"]: row["count"] for row in flag_counts}

    # Gold counts
    gold = cfg["output"]["gold_path"]
    gold_counts = {}
    for table in ["fact_transactions", "dim_accounts", "dim_customers"]:
        try:
            gold_counts[table] = spark.read.format("delta").load(f"{gold}/{table}").count()
        except Exception:
            gold_counts[table] = 0

    # Build report
    dq_issues = []
    for code in DQ_CODES:
        count = flag_map.get(code, 0)
        if count > 0:
            dq_issues.append({
                "issue_code": code,
                "records_affected": count,
                "percentage_of_total": round(count / total_txn * 100, 4) if total_txn > 0 else 0,
                "handling_action": _get_handling_action(cfg, code),
            })

    report = {
        "stage": "2",
        "run_timestamp": run_timestamp.isoformat(),
        "execution_duration_seconds": round(execution_duration_seconds, 1),
        "source_record_counts": source_counts,
        "dq_issues": dq_issues,
        "summary": {
            "total_transactions": total_txn,
            "total_flagged_records": sum(flag_map.values()),
            "clean_records": total_txn - sum(flag_map.values()),
        },
        "gold_layer_record_counts": gold_counts,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info("DQ report written to %s", output_path)
    log.info("  Total flagged: %d / %d", sum(flag_map.values()), total_txn)
    for issue in dq_issues:
        log.info("  %s: %d (%.2f%%)", issue["issue_code"], issue["records_affected"], issue["percentage_of_total"])


def _get_handling_action(cfg: dict, code: str) -> str:
    """Read handling_action from dq_rules.yaml for a given issue code."""
    try:
        rules_path = cfg.get("dq", {}).get("rules_path", "/data/config/dq_rules.yaml")
        import yaml
        with open(rules_path) as f:
            rules = yaml.safe_load(f).get("rules", {})
        return rules.get(code, {}).get("handling_action", "UNKNOWN")
    except Exception:
        return "UNKNOWN"
