# Tessera

A unified observability database: metrics, logs, traces, events and alerts
stored as wide events in Parquet on object storage, served by one binary
that speaks SQL, PromQL, LogQL and TraceQL.

The design is in [`tessera-spec.md`](tessera-spec.md). Requirement IDs
from the spec (`ST-4`, `CL-1`, ...) are cited in code, tests and commit
messages.

## Status

Pre-alpha. Work follows the milestones in spec section 19.

**M0, foundations: done.** Object-store abstraction with a local
directory implementation, fault injection and storage pools; commit logs
over create-if-absent writes with checkpoints; slots and leases; the
cluster catalog with runtime tenant lifecycle; a deterministic
simulation harness; `tessera run` serving an HTTP administration API.

**M1, write path: done, with gaps listed below.** OTLP logs over HTTP;
writer logs with segments, flush records, seal and end; L1 Parquet
files; the writer registry; partition manifests with idempotent
adoption; intake; recovery of dead writers; the consistent view; an
ingester with linger, memtable, flush and log rotation; a simulation
scenario that checks the view invariant (VW-1) under faults, crashes
and duplicate owners.

Gaps left in M1: OTLP over gRPC (HTTP only); no authentication, so the
tenant is always `default`; the partition window is the default day for
every dataset (no dataset log yet); attributes live in the `_overflow`
JSON column until column promotion arrives (M8); dead writers are
detected by a quiet-time heuristic until cluster membership arrives (M6).

**M2, engine core: done, with gaps listed below.** The plan
representation every language lowers into; vectorized kernels with
weight-aware aggregates; push-based operators, pipelines and a CPU pool;
an optimizer with predicate, projection and limit pushdown and
time-range inference; manifest-level pruning and Parquet reads that
prune row groups, fetch coalesced column-chunk ranges and materialize
filtered rows late; an engine that scans files and the tail alike with
`EXPLAIN ANALYZE` reporting every pruning stage; a hand-written SQL
lexer, parser and lowering; SQL over HTTP and over the PostgreSQL wire
protocol; a differential suite against SQLite.

Gaps left in M2: no joins, `HAVING`, subqueries or window functions;
no query parameters over the PostgreSQL protocol; blocking operators
fail instead of spilling when their memory grant runs out; pipelines
run one plan node at a time rather than fused across stages; a bare
attribute reads as text until the advisor promotes it (M8); `now()` is
the node's clock and there is no result cache.

**M3, logs complete: done, with gaps listed below.** Compaction of L1
files into L2 files with deduplication, the default layouts of section
8.5 as far as stored columns allow, term and trigram indexes on the
body and a Bloom filter on trace ids in a sidecar per file, index-driven
pruning of files and row groups, garbage collection of replaced files
and adopted segments, the search syntax (also as `search('...')` in
SQL), LogQL with a hand-written parser, parser stages, label filters,
formatting and metric queries, and the Loki HTTP API. The LogQL
differential suite agrees with Loki 3.3.2 on every case in both
directions; the deviations found are documented in the `tessera-logql`
crate.

Gaps left in M3: push over protobuf (JSON only); `offset`, `stddev`,
`stdvar`, `topk`, `bottomk`, binary operations between two vectors and
`ip()` filters in LogQL; sort keys on attribute columns and ingest-time
grouping of compaction inputs (CP-2) wait for column promotion (M8);
L3, retention sampling and log patterns wait for M9; Grafana itself has
not been run against the API yet.

**M4, traces: done, with gaps listed below.** Spans with their span
events (as `event` rows, DM-8) and links over OTLP; the `TraceGroup`
operator that groups spans by trace and evaluates spanset expressions
on each trace's tree, reading only the spans of the candidate traces
(QL-13); TraceQL with a hand-written parser: spanset filters with
intrinsics and scoped attributes, logical and structural operators,
aggregate filters, pipelines, `select`, `by` and `coalesce`, and the
metrics functions over steps; the Tempo HTTP API: search, traces by
id (JSON and protobuf), tag names and values, metrics. The TraceQL
differential suite agrees with Tempo 2.7.1 on every case; the
deviations found are documented in the `tessera-traceql` crate.

Gaps left in M4: the `event.` and `link.` scopes, the nested-set
intrinsics, `quantile_over_time` and `histogram_over_time`, and the
`traceql(...)` table function in SQL; trace completion (QL-14) waits
for L3 compaction (M9); Grafana itself has not been run against the
API yet.

**M5, metrics: next.**

## Layout

```
crates/
  tessera-types     identifiers, time, event schemas, injected clock and random source
  tessera-object    ObjectStore trait, keys and layout, memory and local-directory stores, pools, fault injection
  tessera-commit    commit logs, checkpoints, slots, leases
  tessera-catalog   cluster catalog: tenants, pools, slot counts
  tessera-format    FileFormat trait and the Parquet implementation
  tessera-wal       writer logs: entries, segments, L1 flush, recovery
  tessera-manifest  partition manifests, writer registry, intake, consistent view
  tessera-ingest    ingester, ingest service, OTLP decoder
  tessera-compact   compaction jobs, deduplication, garbage collection of files and segments
  tessera-ir        plan representation: expressions, plan nodes, typing, function registry, Frontend trait
  tessera-kernels   expression evaluation, variant access, extraction, templates, windows, aggregation
  tessera-opt       binder, rewrite rules, manifest-level and index-level pruning
  tessera-exec      operators, pipelines, CPU pool, scans, the engine, end-to-end query runner
  tessera-sql       SQL lexer, parser and lowering
  tessera-search    the search syntax, lowered to a predicate
  tessera-logql     LogQL lexer, parser and lowering
  tessera-traceql   TraceQL lexer, parser and lowering
  tessera-server    HTTP administration, OTLP ingest, SQL over HTTP, PostgreSQL wire protocol, Loki and Tempo APIs
  tessera           the binary
sim/                deterministic simulation harness (library, tests and the tessera-sim binary)
compat/sql/         SQL differential tests against SQLite
compat/logql/       LogQL differential tests against a Loki instance
compat/traceql/     TraceQL differential tests against a Tempo instance
```

Dependencies between crates point downward only (spec section 18.1).

## Running

```
cargo run -p tessera -- run --data-dir ./tessera-data \
    --listen 127.0.0.1:8080 --pg-listen 127.0.0.1:5432
```

The data directory is the root pool; its layout is the object layout of
spec section 6.2. There is no authentication yet, so both listeners
bind the loopback interface by default.

```
curl -X POST localhost:8080/api/v1/tenants \
     -H 'content-type: application/json' \
     -d '{"name":"acme","settings":{"retention_seconds":2592000}}'
curl localhost:8080/api/v1/tenants
curl -X POST localhost:8080/api/v1/tenants/acme/suspend
curl -X DELETE localhost:8080/api/v1/tenants/acme
```

Logs are ingested over OTLP/HTTP, protobuf or JSON, with the dataset
named by a header (`default` when absent):

```
curl -X POST localhost:8080/v1/logs \
     -H 'content-type: application/json' -H 'x-tessera-dataset: prod' \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1700000000000000000","severityText":"ERROR","body":{"stringValue":"payment failed"}}]}]}]}'
```

Traces go to `/v1/traces` the same way; a span's events become events
of their own and its links stay with the span. An OpenTelemetry
Collector's `otlphttp` exporter pointed at `http://localhost:8080`
works as is for logs and traces.

Queries are SQL, over HTTP or over the PostgreSQL protocol. A table is
a kind of event in a dataset (`logs`, `spans`, `metrics`, `events`,
`alerts`), qualified as `prod.logs` or resolved against the request's
dataset; attributes are paths such as `resource.service.name` and
`attr.http.method`:

```
curl -X POST localhost:8080/api/v1/query -H 'content-type: application/json' \
     -d '{"dataset":"prod","query":"SELECT timestamp, body, resource.service.name AS service FROM logs WHERE severity_text = '"'"'ERROR'"'"' ORDER BY timestamp DESC LIMIT 10"}'
psql 'host=127.0.0.1 port=5432 dbname=prod' \
     -c "SELECT time_bucket(INTERVAL '5m', timestamp) AS t, count(*) FROM logs GROUP BY 1 ORDER BY 1"
```

The database a PostgreSQL client connects to is the dataset. `EXPLAIN`
shows the plan and `EXPLAIN ANALYZE` what each scan read and pruned.

The Loki API lives under `/loki/api/v1`: push JSON streams, query with
LogQL, list labels and values, and tail. Point Grafana's Loki data
source at `http://localhost:8080`. The tenant comes from
`X-Scope-OrgID` and the dataset from `x-tessera-dataset`, both
`default` when absent:

```
curl -X POST localhost:8080/loki/api/v1/push -H 'content-type: application/json' \
     -d '{"streams":[{"stream":{"app":"checkout","level":"error"},"values":[["1700000000000000000","payment timeout"]]}]}'
curl -G localhost:8080/loki/api/v1/query_range \
     --data-urlencode 'query={app="checkout"} |= "timeout" | logfmt' --data-urlencode 'since=1h'
curl -G localhost:8080/loki/api/v1/query_range \
     --data-urlencode 'query=sum by (app) (rate({app=~".+"}[5m]))' --data-urlencode 'since=1h' --data-urlencode 'step=1m'
```

The Tempo API lives under `/api`: search with TraceQL, traces by id,
tag names and values, and TraceQL metrics. Point Grafana's Tempo data
source at `http://localhost:8080`; the tenant and dataset come from
the same headers as the Loki API:

```
curl -G localhost:8080/api/search \
     --data-urlencode 'q={ resource.service.name = "checkout" } >> { status = error }' \
     --data-urlencode "start=$(($(date +%s) - 3600))" --data-urlencode "end=$(date +%s)"
curl localhost:8080/api/traces/<trace id>
curl -G localhost:8080/api/metrics/query_range \
     --data-urlencode 'q={ status = error } | rate() by (resource.service.name)' \
     --data-urlencode "start=$(($(date +%s) - 3600))" --data-urlencode "end=$(date +%s)" \
     --data-urlencode 'step=60s'
```

Compaction, index building and garbage collection run in the
background: a partition's L1 files become one indexed L2 file once
there are four of them or a lone one is an hour old.

The routes are listed in the modules of `crates/tessera-server/src`.
Logging is controlled by `TESSERA_LOG` (for example `TESSERA_LOG=debug`).

## Testing

```
cargo test --workspace               # unit tests and a 48-seed simulation
TESSERA_SIM_SEEDS=500 cargo test -p tessera-sim
cargo run --release -p tessera-sim -- --seeds 2000 --nodes 5 --steps 40 \
    --fail-before 0.15 --fail-after 0.15
```

A simulation is reproducible from its seed; a violation is reported with
the command line that replays it. The scenarios cover the commit log,
leases, the catalog and the write path (ingest, recovery, intake and
the consistent view). The SQL differential suite in `compat/sql` runs
the same questions against SQLite and is part of `cargo test`. The
LogQL suite in `compat/logql` pushes the same lines to a Loki instance
and to an in-process node and compares the answers; it runs when
`TESSERA_LOKI_URL` names a Loki (CI starts one) and reports a skip
otherwise:

```
TESSERA_LOKI_URL=http://127.0.0.1:3100 cargo test -p tessera-compat-logql
```

The TraceQL suite in `compat/traceql` does the same against a Tempo
instance started with `compat/traceql/tempo.yaml`, reached through
`TESSERA_TEMPO_URL` and `TESSERA_TEMPO_OTLP_URL` (CI starts one):

```
TESSERA_TEMPO_URL=http://127.0.0.1:3200 TESSERA_TEMPO_OTLP_URL=http://127.0.0.1:4318 \
    cargo test -p tessera-compat-traceql
```

Every commit passes `cargo fmt --check`, `cargo clippy --all-targets`
with zero findings, `cargo test --workspace` and `cargo deny check`.

## License

Source-available; text pending (see [`LICENSE`](LICENSE) and spec
section 20).
