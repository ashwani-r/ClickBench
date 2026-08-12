#!/bin/bash
#
# First-cold bring-up has to bootstrap the Hive metastore schema, boot
# Kudu master + tserver, register the tserver with the master, and
# only then wait for catalogd + impalad to publish /healthz=OK. Even
# on c6a.4xlarge this overruns the default 300s window; 900s matches
# ../impala/benchmark.sh.
#
# BENCH_RESTARTABLE=no skips the stop+start cycle between cold tries —
# same reason as ../impala/: restarting catalogd wipes its in-memory
# catalog, and with -hms_event_polling_interval_s=0 the daemon doesn't
# reload metadata from HMS. `use clickbench` would then fail with
# "Database does not exist" on every query. drop_caches alone gives a
# cold Kudu scan (page cache flushed) without losing the catalog state
# that ./load put into the running cluster.
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single"
export BENCH_RESTARTABLE=no
export BENCH_CHECK_TIMEOUT=900
exec ../lib/benchmark-common.sh
