"""
Pipeline entry point — Nedbank DE Challenge
Orchestrates Bronze → Silver → Gold in sequence,
then writes dq_report.json for Stage 2+.
Exit codes: 0 = success, 1 = any stage failed.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

# Pre-create output directories before Spark/Hadoop tries to write to them.
# Required when container runs with --read-only + rw volume mounts.
for _dir in [
    "/data/output/bronze",
    "/data/output/silver",
    "/data/output/gold",
    "/tmp/spark-local",
]:
    os.makedirs(_dir, exist_ok=True)

from ingest import run_ingestion
from transform import run_transformation
from provision import run_provisioning

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline.run_all")

STAGES = [
    ("INGEST  (Bronze)",   run_ingestion),
    ("TRANSFORM (Silver)", run_transformation),
    ("PROVISION (Gold)",   run_provisioning),
]

if __name__ == "__main__":
    pipeline_start = time.time()
    run_timestamp = datetime.now(timezone.utc)

    log.info("=" * 60)
    log.info("NEDBANK DE PIPELINE STARTING")
    log.info("=" * 60)

    for stage_name, stage_fn in STAGES:
        log.info("── Stage: %s ──", stage_name)
        stage_start = time.time()
        try:
            stage_fn()
            elapsed = time.time() - stage_start
            log.info("── %s COMPLETE  (%.1fs) ──", stage_name, elapsed)
        except Exception as exc:
            elapsed = time.time() - stage_start
            log.exception("── %s FAILED after %.1fs: %s ──", stage_name, elapsed, exc)
            sys.exit(1)

    # ── DQ Report (Stage 2+) ──────────────────────────────────────────────────
    # Only write if dq_report_path is configured (Stage 1 skips this silently).
    try:
        import yaml
        config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        if cfg.get("output", {}).get("dq_report_path"):
            from spark_utils import get_spark
            from dq_reporter import generate_dq_report
            spark = get_spark(cfg)
            total = time.time() - pipeline_start
            # Source counts are read from Bronze (raw row counts)
            source_counts = {}
            for src in ["accounts", "customers", "transactions"]:
                try:
                    source_counts[src] = spark.read.format("delta").load(
                        f"{cfg['output']['bronze_path']}/{src}"
                    ).count()
                except Exception:
                    source_counts[src] = -1
            generate_dq_report(cfg, spark, run_timestamp, total, source_counts)
    except Exception as exc:
        log.warning("DQ report generation failed (non-fatal): %s", exc)

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE  total=%.1fs", total)
    log.info("=" * 60)
    sys.exit(0)