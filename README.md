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
detected by a quiet-time heuristic (membership shortens it since M6).

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

**M5, metrics: done, with gaps listed below.** Metric data points
over OTLP (gauges, sums, histograms, exponential histograms and
summaries) and over Prometheus remote write, with series ids and
Prometheus names derived from the OpenTelemetry name and unit
(QL-19); a PromQL front end with a hand-written parser; an evaluator
with Prometheus 3 semantics: lookback and staleness, counter resets
and extrapolation in `rate` and `increase`, the `_over_time` family,
aggregations, vector matching with `group_left` and `group_right`,
set operators, subqueries, `offset` and `@`, `histogram_quantile`
over classic buckets, label and calendar functions; and the
Prometheus HTTP API: queries, series, labels, label values, metadata
and remote write. The PromQL differential suite agrees with
Prometheus 3.15.0 on every case.

Gaps left in M5: native histograms (OTLP histograms are stored but
not queryable through PromQL), `limitk`, `limit_ratio` and `info`,
the `promql(...)` table function in SQL, exemplars, rules and alerts
(M7), metric downsampling and rollups (M9); the PromQL compliance
suite has not been run; Grafana itself has not been run against the
API yet.

**M6, distributed execution and cluster: done, with gaps listed
below.** Node roles (`ingest`, `query`, `compact`, `eval`), a node
registry in the root pool with heartbeats and a membership view
(section 14.1); slot ownership through leases shared by rendezvous
hashing over the live compact nodes, renewed, handed over when the
view changes and taken over when a node's leases expire (sections 5.3
and 14.2), driving intake, recovery, compaction and collection; dead
writers recognised by membership; writes forwarded to a dataset's
preferred ingesters (IN-9); leaf scan tasks assigned to query nodes by
file id over internal HTTP routes and read locally when a peer fails
(QE-15, QE-17); per-tenant admission control on concurrent queries,
queue time and ingest bytes in flight with 429 and 503 answers
(QE-18, IN-11); three caches, local and disposable: open manifests,
file footers and closed-range query results keyed by plan and manifest
versions (10.8). The three-node test starts a cluster, scales it up,
loses a node without a word and scales it down, with zero manual
steps (OP-4).

Gaps left in M6: aggregation and the other intermediate stages run on
the coordinator over the rows the leaf tasks return (QE-14 asks for
stages on worker nodes); membership is seeded from the root pool only,
there is no gossip; load signals for an autoscaler (OP-5) and the
`_system` tenant (14.7) wait for M10; the preferred-ingester set is
two per dataset rather than sized by volume; there is no object byte
range cache on local disk because the only stores are a local
directory and memory; the scale envelope of section 17.2 has not been
run.

**M7, control store, continuous queries and alerting: done, with
gaps listed below.** A per-tenant control store over the control log
with serializable transactions and the ten tables of CS-3, exposed
in SQL as `system.<table>` for `SELECT`, `INSERT`, `UPDATE` and
`DELETE` (sections 12.3, 9.5); alert rules in SQL, PromQL and LogQL
through `CREATE ALERT`, `DROP ALERT` and `EXPLAIN ALERT`, and
through Prometheus and Loki rule files on the ruler API (CQ-3); the
alert lifecycle of section 11.8 with `FOR`; rule-group state logs
that record instances, input positions and the outbox in one commit,
so a takeover repeats no transition (CQ-12, CQ-13); the incremental
mode for SQL rules that filter, group by keys and a tumbling window
and aggregate with mergeable functions, maintained from writer-log
segments with eager firing for monotone conditions and watermark
firing for the rest (CQ-1, CQ-2, CQ-4, CQ-5, CQ-7, CQ-9, CQ-10,
CQ-11); periodic re-execution through the engine for every other
rule, with `EXPLAIN ALERT` saying which applies and why (CQ-8); every
transition written as an `alert` event with a deterministic id
(CQ-14); notifications from an outbox, at least once with an
idempotency key, to webhook, Slack-compatible, PagerDuty-compatible
and Alertmanager-compatible sinks, held back by silences and
acknowledgments and repeated at the route's interval (CQ-15, 11.9);
the `eval` role running the rule groups it owns by slot;
`system.alert_instances`, `GET /api/v1/rules` and `GET /api/v1/alerts`
over the state logs.

Gaps left in M7: rollups and recording rules (M9); one state
partition per rule group, so the group count rather than the key
space bounds a group's parallelism (CQ-6); rebuilding window state
from data files when a partition falls behind the WAL retention cap
(section 11.7); inhibition rules and Alertmanager-style grouping of
notifications; email; the incremental subset is SQL only and counts
rows unweighted; the alert latency target is set by the evaluation
interval (five seconds by default) rather than measured; hopping and
sliding windows, sketches and histograms, and absence detection run
periodically.

**M8, adaptive design: done, with gaps listed below.** A workload
log: every query appends to `_system.queries` what it asked of each
table, the columns referenced, the predicates with their observed
selectivity, grouping and ordering, files considered and pruned,
bytes and rows scanned, latency, language and source (AD-1); a
dataset log holding each kind's layout under a layout epoch, the
partition window and the advisor's report (AD-9); the advisor, which
weighs the workload with a seven-day half-life and chooses a sort key
greedily by estimated bytes scanned plus rewrite cost, dedicated
columns for hot paths, Bloom, term and trigram indexes from the
predicates, and the partition window from the file counts, adopting
a layout that beats the current one by a fifth on two consecutive
runs and honouring pins (AD-2, AD-5 to AD-7, AD-12, AD-14, 8.4);
column promotion at compaction for well-known, dense and requested
attribute paths, with `_overflow` kept whole, and scans that serve a
path from a dedicated column where a file has one and extract it
elsewhere, pruning on promoted columns by statistics and Bloom
filters (8.2, AD-3, AD-4); compaction that writes new files in the
current epoch and re-clusters files of older epochs within a budget,
recent partitions first (AD-10); default sort keys that lead with the
service (8.5); `EXPLAIN LAYOUT` (AD-11). The node test shows a
dataset whose queries filter on an attribute adopt it as the leading
sort column and rewrite its files without intervention.

Gaps left in M8: automatic rollups (8.7, with M9); the cost model
estimates selectivity from the workload and guesses cardinality,
there are no per-column sketches (AD-6); the storage term of the cost
is ignored; the workload events of alert rules are not logged; a
promoted column is read from `_overflow` by the tail and by files
older than its promotion, which is correct but not faster; space-
filling curves are not planned.

**M9, cost and retention: done, with gaps listed below.** Metering:
ingest records bytes and events per tenant, dataset, kind, service
and team, the team from a configurable attribute (CO-1); compaction
records stored bytes per the same and per log pattern or name (CO-2);
queries record bytes scanned, time taken, object-store requests,
principal, language, source and fingerprint (CO-3); all of it is
written as events of `_system.usage` and read back as `system.usage`
(CO-4). Log patterns: compaction clusters log bodies into templates
and gives each row a `pattern_id` that stays stable across files
(CO-5); `system.log_patterns` lists them with their volumes. Value
scores per kind, from reads against stored bytes, kept in the dataset
log. Retention by value: `CREATE RETENTION POLICY ... AS WHEN ...
THEN SAMPLE r AFTER d, DROP AFTER d, DOWNSAMPLE TO i AFTER d ... ELSE
...` over `kind`, severity, `value_score` and row columns;
`EXPLAIN RETENTION` estimates what a policy removes and saves (CO-9);
policies are applied at compaction, sampling by event or by trace
with `sample_rate` scaled on the survivors (CO-6, CO-7), downsampling
metrics to count, sum, min, max and last per series and interval
(CO-8), and dropping by age alone removes whole partitions; nothing
is reduced by value without an enabled policy (CO-10). Budgets per
tenant and team for ingest, and per tenant for queries, as tenant
settings; once a budget is reached, what arrives beyond it is
admitted and recorded once a day (the default), sampled at ingest, or
refused with 429, and a query is refused before it runs (13.4).
Rollups: `CREATE ROLLUP name [EVERY d] [INTO dataset] AS` a grouped
aggregation over one tumbling window, maintained by the eval role
from closed windows into a derived dataset of events, read by the
planner in place of the raw rows when a query's source, filter, keys,
aggregates and whole-window time range match, exactly, with
`"rollups": false` on the native API to turn the rewrite off
(AD-13); the advisor proposes rollups for the aggregation shapes the
workload repeats (8.7).

Gaps left in M9: `system.usage` has not been reconciled against
object-store billing (the exit criterion); unit prices and cost are
not shown (CO-4); traces with errors are not exempt from sampling and
histograms and sketches are not merged when downsampling (CO-7,
CO-8); the budget ledger counts per node, so a tenant served by
several ingesters gets up to that many budgets; teams have no query
budget, since queries carry no principal; Prometheus recording rules
are still refused rather than turned into rollups; a rollup starts at
its creation and does not backfill; a query whose range is not aligned
to a rollup's windows or reaches past its progress mark reads the raw
rows whole; narrow integers come back from a rollup as 64-bit.

**M10, hardening: done, with gaps listed below.** Authentication by
API key (CT-5, SE-1): a key belongs to one tenant, is issued once as
`tsk_<tenant>_<id>_<secret>` and stored as a hash in `system.api_keys`,
and acts with roles from `system.roles` that grant reading and writing
datasets and administering the tenant, `admin`, `reader` and `writer`
built in; `X-Scope-OrgID` selects among the tenants a principal may act
for, which for the `_system` tenant's admins is every one; a request
without a key is a cluster administrator only where anonymous access is
on, which `tessera run` does unless `--no-anonymous` is given, and gets
401 elsewhere; ingest needs write, every query path needs read on each
dataset the plan scans, control statements and the ruler APIs need
admin, the administration API needs a cluster administrator and issues
and revokes keys, and over the PostgreSQL protocol the key is the
password. An audit dataset, `_system.audit`, records every
administrative action and control statement with its principal
(SE-6). Load signals at `GET /metrics` in the Prometheus text format:
writes in flight, query queueing, compaction and intake backlog,
evaluator lag and slots held (OP-5). Self-observability: a node writes
its load signals to `_system.metrics` and its log events to
`_system.logs`, and ships default health rules for them in the
`_system` tenant (14.7). `DELETE FROM <table> WHERE ...` on a dataset
stores a tombstone that hides the matching rows at once and has
compaction remove them, one node collecting tombstones once every file
beyond L1 is clean and thirty days have passed (SE-5). Redaction rules
as tenant settings drop, hash or mask attributes and log bodies at
ingest, before anything is written (SE-4). A container image, a Helm
chart and a CI image build (OP-10). Node records announce the newest
data format a build reads, and a node refuses to start beside one that
cannot read what it writes (OP-7).

Gaps left in M10: no OIDC or mutual TLS, and traffic is not encrypted
in transit, so a cluster must run on a private network; node-to-node
routes under `/internal` carry no credentials (SE-1, SE-3); no
row-filter roles; no per-tenant encryption keys (SE-2); no Iceberg
export (OP-9); the scale envelope of 17.2 and a rolling upgrade under
load have not been run; neither the image nor the chart has run on a
cluster, and the binary is not statically linked; writer-log latency
and the commit conflict rate are not measured, so their health rules
are not shipped; redaction by a pattern over values is not
implemented; membership still comes from the registry alone, not
gossip or DNS.

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
  tessera-cluster   node roles, the node registry, membership and admission control
  tessera-ir        plan representation: expressions, plan nodes, typing, function registry, Frontend trait
  tessera-kernels   expression evaluation, variant access, extraction, templates, windows, aggregation
  tessera-opt       binder, rewrite rules, manifest-level and index-level pruning
  tessera-exec      operators, pipelines, CPU pool, scans, the engine, end-to-end query runner
  tessera-sql       SQL lexer, parser and lowering
  tessera-search    the search syntax, lowered to a predicate
  tessera-logql     LogQL lexer, parser and lowering
  tessera-traceql   TraceQL lexer, parser and lowering
  tessera-promql    PromQL lexer, parser and lowering
  tessera-eval      continuous queries and alerting: rules, lifecycle, state logs, incremental and periodic evaluation, notifications
  tessera-advisor   adaptive physical design: the cost model that turns the workload log into sort keys, indexes, dedicated columns and windows
  tessera-server    HTTP administration, OTLP ingest, remote write, SQL over HTTP, PostgreSQL wire protocol, Loki, Tempo, Prometheus and ruler APIs
  tessera-node      one node: wires the roles, the background work and the APIs of a process
  tessera           the binary: the command line over tessera-node
sim/                deterministic simulation harness (library, tests and the tessera-sim binary)
deploy/helm/tessera/ Helm chart: node groups by role over one shared root pool
Dockerfile          the container image
compat/sql/         SQL differential tests against SQLite
compat/logql/       LogQL differential tests against a Loki instance
compat/traceql/     TraceQL differential tests against a Tempo instance
compat/promql/      PromQL differential tests against a Prometheus instance
```

Dependencies between crates point downward only (spec section 18.1).

## Running

```
cargo run -p tessera -- run --data-dir ./tessera-data \
    --listen 127.0.0.1:8080 --pg-listen 127.0.0.1:5432
```

The data directory is the root pool; its layout is the object layout of
spec section 6.2. A request without an API key acts as a cluster
administrator unless `--no-anonymous` is given, so both listeners bind
the loopback interface by default. To require keys, issue one for the
`_system` tenant while anonymous access is on, then restart with
`--no-anonymous`:

```
curl -X POST localhost:8080/api/v1/tenants/_system/keys \
     -H 'content-type: application/json' -d '{"name":"ops","roles":["admin"]}'
# {"key":"tsk__system_...","id":"...","name":"ops","roles":["admin"],...}
curl -H 'authorization: Bearer tsk__system_...' localhost:8080/api/v1/tenants
```

A key belongs to one tenant and acts with its roles: `admin`, `reader`
and `writer` are built in, and `INSERT INTO system.roles` defines
others with the datasets they may read and write. Over the PostgreSQL
protocol the key is the password. `GET /metrics` and `GET /healthz`
need no key.

A cluster is several nodes on one root pool: a shared directory for
now. Each node announces itself in the pool's registry and finds the
others there, so the whole start-up configuration is the root pool,
the roles, the listen addresses and the address peers reach the node
at (OP-11):

```
cargo run -p tessera -- run --data-dir /shared/tessera --roles ingest,query     --listen 0.0.0.0:8080 --advertise http://node-1:8080
cargo run -p tessera -- run --data-dir /shared/tessera --roles compact     --listen 0.0.0.0:8081 --pg-listen 0.0.0.0:5433 --advertise http://node-2:8081
```

Adding a node is starting it; removing one is stopping it.

The same binary runs in a container built from the `Dockerfile`, and the
Helm chart in `deploy/helm/tessera` runs node groups by role over one
`ReadWriteMany` volume as the root pool, since the binary reads a local
directory. Neither has been exercised on a cluster yet; the CI builds
the image.

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
of their own and its links stay with the span. Metrics go to
`/v1/metrics`, one row per data point, or over Prometheus remote
write at `/api/v1/write`. An OpenTelemetry Collector's `otlphttp`
exporter pointed at `http://localhost:8080` works as is for logs,
traces and metrics.

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

The Prometheus API lives under `/api/v1`: instant and range queries
in PromQL, series, labels and label values. Point Grafana's
Prometheus data source at `http://localhost:8080`. A metric ingested
over OTLP is queried by the name Prometheus would give it, so
`http.server.duration` in milliseconds is `http_server_duration_milliseconds`:

```
curl -G localhost:8080/api/v1/query_range \
     --data-urlencode 'query=sum by (job) (rate(http_requests_total[5m]))' \
     --data-urlencode "start=$(($(date +%s) - 3600))" --data-urlencode "end=$(date +%s)" \
     --data-urlencode 'step=60'
curl -G localhost:8080/api/v1/query --data-urlencode 'query=up'
```

The physical design adapts to use. Every query is logged to
`_system.queries`; the advisor on compact nodes reads the log back,
weighs it by age and proposes, per dataset and kind, a sort key,
dedicated columns, indexes and a partition window, adopting a change
once it has paid off on two consecutive runs; compaction writes new
files that way and rewrites old ones within a budget. Pins in
`system.layout_pins` override it, and `EXPLAIN LAYOUT` shows what it
found:

```
psql 'host=127.0.0.1 port=5432 dbname=prod' -c "EXPLAIN LAYOUT prod"
psql 'host=127.0.0.1 port=5432 dbname=prod' -c \
  "INSERT INTO system.layout_pins (key, document) VALUES ('prod/log', '{\"sort_key\": [\"attr.host\"]}')"
```

Alert rules are continuous queries whose sink is the alert state
machine. They live in the tenant's control store, which SQL exposes
as the `system` tables, and are evaluated by the `eval` role; a rule
that filters, groups by keys and a tumbling window and aggregates with
`count`, `sum`, `min`, `max` or `avg` is maintained from the write-ahead
log as rows arrive, anything else is re-executed at its interval:

```
psql 'host=127.0.0.1 port=5432 dbname=prod' <<'SQL'
INSERT INTO system.notification_routes (name, kind, url)
  VALUES ('oncall', 'webhook', 'http://hooks.example/tessera');
CREATE ALERT checkout_errors SEVERITY critical FOR 2m NOTIFY oncall
  ANNOTATIONS (summary = 'checkout is failing') AS
  SELECT resource.service.name AS service, count(*) AS errors
  FROM logs
  WHERE severity_number >= 17 AND resource.service.name = 'checkout'
  GROUP BY resource.service.name, tumble(timestamp, INTERVAL '1 minute')
  HAVING count(*) > 100;
EXPLAIN ALERT checkout_errors;
SELECT rule, status, labels, value FROM system.alert_instances;
INSERT INTO system.silences (id, matchers, starts_at, ends_at, comment)
  VALUES ('maint', '{"service": "checkout"}', now(), now() + INTERVAL '2 hours', 'planned');
SQL
```

Prometheus and Loki rule files are accepted as written, on the ruler
API of each: `POST /api/v1/rules/{namespace}` for PromQL groups and
`POST /loki/api/v1/rules/{namespace}` for LogQL ones; their rules
notify the route named `default`, which an Alertmanager-compatible
route can point at an existing Alertmanager. `GET /api/v1/rules` and
`GET /api/v1/alerts` answer in Prometheus' shape, so Grafana's alert
list works. Every state transition is also an `alert` event of the
rule's dataset, queryable like any other event.

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

The PromQL suite in `compat/promql` pushes the same samples over
remote write to a Prometheus started with
`--web.enable-remote-write-receiver` (configuration in
`compat/promql/prometheus.yml`) and to an in-process node, reached
through `TESSERA_PROMETHEUS_URL` (CI starts one):

```
TESSERA_PROMETHEUS_URL=http://127.0.0.1:9090 cargo test -p tessera-compat-promql
```

Every commit passes `cargo fmt --check`, `cargo clippy --all-targets`
with zero findings, `cargo test --workspace` and `cargo deny check`.

## License

Source-available; text pending (see [`LICENSE`](LICENSE) and spec
section 20).
