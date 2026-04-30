#!/bin/bash
# ── Fix hostname resolution for --network none environments ───────────────────
# When Docker runs with --network none, the container's hostname (a random hex
# ID like f36e0407ca5c) cannot be resolved via DNS. Log4j and Spark both try
# to resolve it for log metadata — this produces a wall of UnknownHostException
# warnings before Spark even starts.
#
# The fix: write the container's own hostname to /etc/hosts pointing to 127.0.0.1
# before Python starts. This makes the OS-level hostname lookup succeed without
# needing any network stack.
echo "127.0.0.1 $(hostname)" >> /etc/hosts

# Hand off to the pipeline
exec python pipeline/run_all.py
