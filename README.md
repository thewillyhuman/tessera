# Tessera

A unified observability database: metrics, logs, traces, events and alerts
stored as wide events in Parquet on object storage, served by one binary
that speaks SQL, PromQL, LogQL and TraceQL.

The design is in [`tessera-spec.md`](tessera-spec.md). Requirement IDs
from the spec (`ST-4`, `CL-1`, ...) are cited in code and tests.

## Status

Pre-alpha. Work follows the milestones in spec section 19; M0
(foundations) is in progress.

## Layout

```
crates/
  tessera-types    event model, identifiers, errors; depends on nothing
```

Dependencies between crates point downward only (spec section 18.1).

## Building

```
cargo build
cargo test
cargo clippy --all-targets
cargo fmt --check
```

## License

Source-available; text pending (see [`LICENSE`](LICENSE) and spec
section 20).
