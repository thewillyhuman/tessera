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

**M2, engine core: next.**

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
  tessera-server    HTTP: administration, OTLP ingest, dataset read
  tessera           the binary
sim/                deterministic simulation harness (library, tests and the tessera-sim binary)
```

Dependencies between crates point downward only (spec section 18.1).

## Running

```
cargo run -p tessera -- run --data-dir ./tessera-data --listen 127.0.0.1:8080
```

The data directory is the root pool; its layout is the object layout of
spec section 6.2. The API has no authentication yet, so it listens on
the loopback interface by default.

```
curl -X POST localhost:8080/api/v1/tenants \
     -H 'content-type: application/json' \
     -d '{"name":"acme","settings":{"retention_seconds":2592000}}'
curl localhost:8080/api/v1/tenants
curl -X POST localhost:8080/api/v1/tenants/acme/suspend
curl -X DELETE localhost:8080/api/v1/tenants/acme
```

Logs are ingested over OTLP/HTTP, protobuf or JSON, with the dataset
named by a header (`default` when absent), and read back newest first:

```
curl -X POST localhost:8080/v1/logs \
     -H 'content-type: application/json' -H 'x-tessera-dataset: prod' \
     -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1700000000000000000","severityText":"ERROR","body":{"stringValue":"payment failed"}}]}]}]}'
curl 'localhost:8080/api/v1/datasets/prod/logs?limit=10'
```

An OpenTelemetry Collector's `otlphttp` exporter pointed at
`http://localhost:8080` works as is for logs.

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
the consistent view).

Every commit passes `cargo fmt --check`, `cargo clippy --all-targets`
with zero findings, `cargo test --workspace` and `cargo deny check`.

## License

Source-available; text pending (see [`LICENSE`](LICENSE) and spec
section 20).
