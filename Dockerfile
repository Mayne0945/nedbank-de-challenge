FROM nedbank-de-challenge/base:1.0

# procps provides the 'ps' command that Spark's startup script expects.
# Without it, Spark logs a harmless but noisy warning at startup.
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake Delta JARs into pyspark/jars/ at build time so runtime needs no internet
COPY warmup.py warmup.py
COPY pipeline/spark_utils.py pipeline/spark_utils.py
RUN python3 warmup.py && \
    PYSPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars && \
    find /root/.ivy2 -name "delta-spark*.jar"    -exec cp {} $PYSPARK_JARS/ \; && \
    find /root/.ivy2 -name "delta-storage*.jar"  -exec cp {} $PYSPARK_JARS/ \; && \
    find /root/.ivy2 -name "antlr4-runtime*.jar" -exec cp {} $PYSPARK_JARS/ \; && \
    echo "Baked JARs:" && ls /usr/local/lib/python3.11/site-packages/pyspark/jars/delta* && \
    rm warmup.py

COPY pipeline/ pipeline/
COPY config/ config/
COPY entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

# Use entrypoint script so the container hostname is written to /etc/hosts
# before Spark/Log4j initialises — eliminates UnknownHostException warnings
# when running with --network none.
CMD ["./entrypoint.sh"]