## Apache Impala on Apache Kudu

This variant runs Apache Impala 4 as the SQL frontend over Apache Kudu
as the storage engine, all in Docker Compose on a single VM:

- `hms`          — Hive Metastore (Derby-backed)
- `statestored`  — Impala cluster-membership broker
- `catalogd`     — Impala metadata cache (Kudu-aware via `-kudu_master_hosts`)
- `impalad-1`    — combined coordinator + executor
- `kudu-master-1` — Kudu master (RPC 7051, web UI 8051)
- `kudu-tserver-1` — Kudu tablet server (RPC 7050, web UI 8050)

The Impala half is a direct fork of `../impala/`; the two Kudu
services are added on the same bridge network and the `impalad-1`
flag `-kudu_master_hosts` is flipped from the empty string (which
`../impala/` uses to explicitly disable Kudu integration) to point at
the master.

**Hardware requirement:** inherited from Impala — AVX-capable x86_64
only. Graviton/aarch64 hosts (including under QEMU emulation) are not
supported. Kudu itself has no AVX requirement, but the Impala daemons
in this stack refuse to start without it.

### Phase status

**Phase 1 — stack bring-up.** ✅ Done. `./install`, `./start`,
`./check`, `./stop` bring the six-container cluster up and down.
`./check` gates on Impala `/healthz`, Kudu master `/varz`, and at
least one LIVE tablet server registered with the master.

**Phase 2 — schema and load.** ✅ Done. `create.sql` defines an
external `hits_parquet` table over `data/hits/`, then creates a
native `hits` table `STORED AS KUDU` with the PK columns
(CounterID, EventDate, UserID, EventTime, WatchID) moved to the
front. `EventDate` is Kudu `DATE`, the three timestamp columns are
Kudu `UNIXTIME_MICROS` (Impala `TIMESTAMP`), partitioning is
`HASH(WatchID) PARTITIONS 16`. `./load` stages the Parquet file,
runs `create.sql` (which does the UPSERT and `COMPUTE STATS`, then
drops the staging table), and reclaims the ~14GB Parquet from disk.
`./data-size` reports Kudu's `on_disk_size` from
`kudu table statistics`.

Two Kudu server-side flags set in `docker-compose.yml`:
- master `--default_num_replicas=1` — the master's default of 3
  would leave every write hanging on a single-tserver cluster,
  since Kudu can't place two extra replicas.
- tserver `--max_cell_size_bytes=1048576` — Kudu's default cell
  size limit is 64 KiB, but some Title/URL/Referer values in the
  hits dataset exceed that. Raising the limit does cost peak
  memory during flush and compaction; 1 MiB covers observed cells
  comfortably without pushing toward the 16 MiB server-side cap.

`UPSERT` is used instead of `INSERT` because ClickHouse's
`PRIMARY KEY` is a sort key, not a uniqueness constraint — the
source Parquet may have PK duplicates on
(CounterID, EventDate, UserID, EventTime, WatchID), and `INSERT`
would abort the whole load on the first collision. On a fresh
table `UPSERT` is functionally identical.

**Phase 3 — query dialect.** `queries.sql` is pre-populated from
`../impala/queries.sql`. Impala-on-Kudu speaks the same dialect as
Impala-on-Parquet, so no per-query edits are expected. Partition-
pruning wins from adding RANGE partitioning on `EventDate` (for
Q37–Q43) are a candidate for a separate submission.

### Notes on the "two-engine" nature of the submission

An "Impala on Kudu" number is a joint measurement of two systems.
Impala plans, executes joins/aggregations, and evaluates predicates
that Kudu can't push down (regex, LIKE, DATE_TRUNC, arithmetic on
projected columns). Kudu handles storage, projection, primary-key
seeks, and equality/range predicates on non-PK columns. The
submission should be read as "SQL-on-Kudu via Impala," not as pure
Kudu throughput. The `Kudu` tag in `template.json` marks that.
