"""
Shared SparkSession factory — Nedbank DE Challenge.
Delta JARs are baked into pyspark/jars/ at Docker build time,
so Spark loads them automatically from the classpath.
No Ivy/Maven download required at runtime.
"""

import logging
import os

import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

from pyspark.sql import SparkSession

log = logging.getLogger("spark_utils")


def get_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg.get("spark", {})
    spark = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[2]"))
        .appName(spark_cfg.get("app_name", "nedbank-de-pipeline"))
        # Delta JARs are in pyspark/jars/ — loaded automatically by Spark.
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Bind to loopback — required when container runs with --network none
        .config("spark.driver.host",        "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        # Scoring system runs with --memory=2g, not 4g
        .config("spark.driver.memory",      "1200m")
        # --read-only container: all Spark temp/local dirs must be under /tmp
        # which is the only writable tmpfs (512MB) provided by the harness.
        .config("spark.local.dir",          "/tmp/spark-local")
        .config("spark.driver.extraJavaOptions",
                "-Djava.io.tmpdir=/tmp")
        .config("spark.executor.extraJavaOptions",
                "-Djava.io.tmpdir=/tmp")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready  master=%s", spark.sparkContext.master)
    return spark