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

**M1, write path: next.** OTLP logs, writer logs, the writer registry,
memtable, L1 flush, intake, recovery and the consistent view.

## Layout

```
crates/
  tessera-types     identifiers, time, injected clock and random source; depends on nothing
  tessera-object    ObjectStore trait, keys, memory and local-directory stores, pools, fault injection
  tessera-commit    commit logs, checkpoints, slots, leases
  tessera-catalog   cluster catalog: tenants, pools, slot counts
  tessera-server    HTTP administration API
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

The routes are listed in `crates/tessera-server/src/admin.rs`. Logging
is controlled by `TESSERA_LOG` (for example `TESSERA_LOG=debug`).

## Testing

```
cargo test --workspace               # unit tests and a 48-seed simulation
TESSERA_SIM_SEEDS=500 cargo test -p tessera-sim
cargo run --release -p tessera-sim -- --seeds 2000 --nodes 5 --steps 40 \
    --fail-before 0.15 --fail-after 0.15
```

A simulation is reproducible from its seed; a violation is reported with
the command line that replays it.

Every commit passes `cargo fmt --check`, `cargo clippy --all-targets`
with zero findings, `cargo test --workspace` and `cargo deny check`.

## License

Source-available; text pending (see [`LICENSE`](LICENSE) and spec
section 20).
