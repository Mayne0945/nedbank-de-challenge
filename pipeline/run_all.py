"""
Pipeline entry point — Nedbank DE Challenge
Orchestrates the three medallion stages in order:
  1. ingest.py    — raw source files → Bronze Delta tables
  2. transform.py — Bronze → Silver Delta tables (typed, deduped, DQ-flagged)
  3. provision.py — Silver → Gold dimensional model (fact + dims)

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Exit codes:
  0 — all three stages completed successfully
  1 — any stage failed (logged with full traceback before exit)
"""

import logging
import sys
import time

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
    ("INGEST  (Bronze)",       run_ingestion),
    ("TRANSFORM (Silver)",     run_transformation),
    ("PROVISION (Gold)",       run_provisioning),
]

if __name__ == "__main__":
    pipeline_start = time.time()
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
            log.exception(
                "── %s FAILED after %.1fs: %s ──", stage_name, elapsed, exc
            )
            sys.exit(1)

    total = time.time() - pipeline_start
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE  total=%.1fs", total)
    log.info("=" * 60)
    sys.exit(0)