#!/bin/bash

# 1. Bypass read-only /etc/hosts
HOSTNAME_VAL=$(hostname)
if cp /etc/hosts /tmp/hosts.tmp 2>/dev/null; then
    echo "127.0.0.1 ${HOSTNAME_VAL}" >> /tmp/hosts.tmp
    mount --bind /tmp/hosts.tmp /etc/hosts 2>/dev/null || true
fi

# 2. Pre-create the directory tree
mkdir -p /data/output/bronze /data/output/silver /data/output/gold 
mkdir -p /tmp/spark-local /tmp/derby /tmp/spark-warehouse /data/output/spark-tmp

# 3. Redirect temporary files
export SPARK_LOCAL_DIRS=/tmp/spark-local
export JAVA_TOOL_OPTIONS="-Dderby.system.home=/tmp/derby -Dspark.sql.warehouse.dir=/tmp/spark-warehouse -Dorg.xerial.snappy.tempdir=/data/output/spark-tmp"

# 4. Execute the pipeline and capture the exit status
python pipeline/run_all.py
PIPELINE_STATUS=$?

# 5. UNLOCK THE FILES for the host machine (Fixes DuckDB read + rm cleanup errors)
chmod -R 777 /data/output 2>/dev/null || true

# 6. Exit with the actual Spark status
exit $PIPELINE_STATUS
