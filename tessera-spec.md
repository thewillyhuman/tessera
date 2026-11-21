# Tessera: a unified observability database

**Status:** Draft 0.2 · 2026-10-10
**Implementation language:** Rust
**Distribution:** self-hosted software; no managed service is planned
**License:** Apache License 2.0 (section 20)

The key words MUST, SHOULD and MAY are used as in RFC 2119. Requirements carry IDs (for example `ST-4`) so that code, tests and issues can refer to them. IDs were renumbered in this draft.

### Changes since draft 0.1

| Decision | Effect on the specification |
|----------|-----------------------------|
| Self-hosted only; tenants configurable at runtime | Cluster catalog and tenant lifecycle (section 12); managed-service concerns removed |
| No architectural scale ceiling, in compute or storage | New goal G9 and principle P7; write path, manifests and work ownership redesigned so that no shared component has fixed capacity (sections 5.3, 5.4, 6); continuous queries run as a partitioned dataflow (section 11.3) |
| Own query engine | New section 10; third-party planner and executor removed from the dependency list |
| Parquet confirmed | No longer an open question |
| LogQL and TraceQL from the start | Five query languages (section 9); Loki and Tempo HTTP APIs (section 16); each signal ships with its native language in the same milestone (section 19) |
| Single region | Unchanged (section 14.5) |
| Name confirmed; license decided | Section 20: open source under the Apache License 2.0 |

---

## 1. Summary

Tessera is a single database for all observability data: metrics, logs, traces, events and alerts. It stores everything as wide events in Parquet on object storage, runs as one binary from a laptop to a cluster of any size, speaks SQL, PromQL, LogQL and TraceQL over the same data, and manages its own physical design by watching the queries it serves.

Four ideas carry most of the design:

1. **Object storage is the only durable state.** Every node is stateless or holds only caches and recoverable buffers. Coordination uses create-if-absent writes on the object store, so there is no consensus cluster to operate.
2. **The write path shares nothing.** Each ingester appends only to its own log. Acknowledging a write never touches state that another node also writes, so ingest capacity grows with the number of nodes and nothing else.
3. **Files describe themselves.** Each data file records its own schema, sort order and indexes. Physical design can change over time without migrations: new files use the new layout, old files stay valid, and background work converges them.
4. **One log feeds everything.** The write-ahead log is the durability mechanism, the source of sub-second freshness, and the stream that continuous queries, rollups and alerts consume.

## 2. Goals and non-goals

### 2.1 Goals

| ID | Goal | Measured by |
|----|------|-------------|
| G1 | **Self-managing operations.** Adding or losing a node needs no human action; no shard planning, no coordination service. | Zero manual steps in scale-up, scale-down and node-loss tests |
| G2 | **Object-storage native, open format.** All durable data in S3-compatible storage; data files readable by third-party engines. | External engine reads a table through exported Iceberg metadata |
| G3 | **No upfront schema decisions.** Arbitrary events accepted; columns, sort order, indexes and partitioning derived from data and workload. | No DDL required before first write or first fast query |
| G4 | **One data model.** Metrics, logs, traces, events and alert history are views over one event model and can be joined. | Cross-signal query in a single statement |
| G5 | **Native query languages over the same data.** SQL, PromQL, LogQL, TraceQL and a search syntax compile to one plan representation and one engine. | Grafana's Prometheus, Loki, Tempo and PostgreSQL data sources work unmodified |
| G6 | **Alerts as continuous queries**, maintained incrementally, with a transactional store for alert state and configuration. | Event-to-notification latency (section 17) |
| G7 | **Cost transparency.** Ingest and query cost attributed per tenant, team, dataset, log pattern and query; retention by value as well as age. | `system.usage` answers "what does this cost and who uses it" |
| G8 | **One binary.** The same executable and code paths serve a laptop and a large cluster. | Single-node mode passes the same test suite as cluster mode |
| G9 | **No architectural ceiling.** Ingest rate, stored volume, file count, query size, rule count, tenant count and node count each grow by adding nodes or storage, never by redesign. | Scalability model (section 5.4) holds; scale tests show near-linear throughput (section 17.2) |

### 2.2 Non-goals

- A managed service. Tessera is software that others install and run.
- General-purpose OLTP. The control store (section 12.3) is for small configuration data only.
- Relevance-ranked full-text search. Search returns matches ordered by time, not by score.
- The Elasticsearch query DSL.
- Arbitrary row-level updates. Data is append-only apart from compaction, policy-driven sampling and delete-by-predicate.
- Active-active operation across regions in the first release (section 14.5).

## 3. Design principles

**P1. Durable state lives in the object store and nowhere else.** Local disk is cache or recoverable buffer. Deleting every node and starting new ones MUST lose no acknowledged data.

**P2. Hints may be wrong; conditional writes are the truth.** Gossip, caches, routing and ownership are optimizations. Correctness rests only on create-if-absent writes against the object store, which also provide fencing.

**P3. Files are immutable and self-describing.** Layout decisions are per file, never global.

**P4. Decide late, revise cheaply.** Anything a user would otherwise have to choose up front (types, columns, keys, indexes, partition sizes, rollups) is inferred, versioned and revisable in the background.

**P5. The system observes itself with itself.** Workload logs, usage metering and internal telemetry are ordinary events in system datasets.

**P6. Human hints are allowed and never required.**

**P7. No component has fixed capacity.** Every piece of shared state is partitioned, and every partitioning can be made finer at runtime. Any work done by a single owner is either cheap metadata work whose heavy part is delegated, or is itself partitioned.

### 3.1 How the known tensions are resolved

| Tension | Resolution | Section |
|---------|------------|---------|
| Object storage vs. sub-second freshness | Batched WAL segments to object storage for durability; unflushed rows served from ingester memory for freshness | 6.3, 6.8 |
| Schema-free ingestion vs. scan speed | Frequently present or frequently queried attributes get dedicated columns per file; the long tail goes to a shredded variant column | 4.4, 8.2 |
| Single binary vs. elastic scaling | Roles inside one binary; all roles stateless; single-node mode swaps the object store for a local directory | 5.2 |
| No consensus cluster vs. strong consistency | Commit logs using conditional object writes; leases with fencing for single-owner work | 6.5, 14.2 |
| Strong consistency vs. unlimited write throughput | Writers never contend: each appends to its own log, and files enter shared manifests later through batched, partitioned commits | 6.3, 6.7 |
| Unified model vs. signal-specific efficiency | One logical table per dataset; physical files are segregated by kind so layout can specialize | 4.2, 6.4 |
| Instant alerts vs. late and out-of-order data | Monotone conditions fire eagerly; other conditions wait for a watermark | 11.5 |

## 4. Data model

### 4.1 Hierarchy

```
tenant ── dataset ── event
```

- A **tenant** is the isolation, accounting and authorization boundary. Tenants are created, changed and removed at runtime (section 12.2).
- A **dataset** is a namespace of events with one evolving schema (for example `prod`, `staging`). A tenant MAY use a single dataset. Datasets are created implicitly on first write (G3).
- An **event** is a timestamped set of typed attributes.

### 4.2 The event

Every record of every signal is an event. A `kind` column says which signal produced it and determines which well-known columns apply.

| Column | Type | Applies to | Notes |
|--------|------|------------|-------|
| `timestamp` | timestamp(ns) | all | Event time. Required. |
| `observed_timestamp` | timestamp(ns) | all | Set at ingest. |
| `kind` | enum | all | `log`, `span`, `metric`, `event`, `alert` |
| `event_id` | fixed(16) | all | Identity for deduplication (section 7.5) |
| `trace_id` | fixed(16) | log, span, event, metric exemplars | Correlation |
| `span_id`, `parent_span_id` | fixed(8) | span, log, event | |
| `name` | string | span, event, metric | Span name, event name, metric name |
| `span_kind` | enum | span | |
| `duration_ns` | int64 | span | |
| `status_code`, `status_message` | enum, string | span | |
| `links` | variant | span | List of linked trace and span ids with attributes |
| `severity_number`, `severity_text` | int32, string | log | |
| `body` | string or variant | log | |
| `metric_type` | enum | metric | gauge, sum, histogram, exp_histogram, summary |
| `value` | float64 | metric | Gauge or sum value |
| `histogram` | struct | metric | Bucket bounds and counts, or exponential layout |
| `temporality`, `is_monotonic` | enum, bool | metric | |
| `series_id` | fixed(16) | metric | Hash of metric name and sorted attribute set |
| `sample_rate` | float64 | all | Fraction of similar rows kept; 1.0 unless sampled (section 13.3). Rows weigh `1 / sample_rate` in aggregates |
| `pattern_id` | fixed(8) | log | Assigned at compaction (section 13.2) |
| `resource.*` | dynamic | all | Resource attributes |
| `scope.*` | dynamic | all | Instrumentation scope |
| `attr.*` | dynamic | all | Record attributes |

Well-known column names follow OpenTelemetry semantic conventions where one exists.

- **DM-1** The logical model MUST expose one table per dataset, `events`, plus views `logs`, `spans`, `metrics`, `alerts` defined as filters on `kind`.
- **DM-2** Any two events in a tenant MUST be joinable on any shared column, across kinds and datasets.
- **DM-3** Alert state transitions MUST be recorded as events of kind `alert`, so that alert history is queried like any other signal.

### 4.3 Types

Scalar types: `null`, `bool`, `int64`, `float64`, `string`, `bytes`, `timestamp(ns)`. Composite: `list<scalar>`, and nested maps, which are flattened into dotted paths (`attr.http.request.method`). Lists of maps are kept as variant values.

### 4.4 Dynamic attributes and type conflicts

- **DM-4** Ingest MUST accept any attribute path with any supported type without prior declaration.
- **DM-5** If a path is observed with more than one type, each type is stored separately (physically `attr.code#i64`, `attr.code#str`). The logical column is then of type `variant`. Comparisons against a literal use the sub-column matching the literal's type; `int64` and `float64` compare numerically with each other. Explicit `CAST` is always available.
- **DM-6** Unqualified names in queries resolve in the order: well-known column, `attr.`, `resource.`, `scope.`. Qualified names are always accepted. A small set of aliases is built in (`service` for `resource.service.name`).

### 4.5 Metrics

A metric data point is one event. There is no separate series index held in memory: series identity is the `series_id` column, and label matching is columnar filtering over dictionary- and run-length-encoded attribute columns, which the metric sort order (section 8.5) makes cheap.

- **DM-7** The number of distinct series MUST NOT bound ingest or require configuration. Cardinality affects compression ratio and scan cost only.

### 4.6 Span events and links

- **DM-8** An OpenTelemetry span event is stored as an event of kind `event` carrying the `trace_id` and `span_id` of its span. Span events are therefore queryable on their own and joinable to their span.
- **DM-9** Span links are stored in the `links` column of the span.

### 4.7 Structured log bodies

- **DM-10** When a log body is a JSON object or a logfmt line, ingest MAY parse it and store its fields under `body.*` in addition to the original text (default: on for JSON, off for logfmt, per dataset). Query-time parsing stages (LogQL `| json`, `| logfmt`) then resolve against stored columns instead of re-parsing (section 9.4).

## 5. Architecture

### 5.1 Overview

```
        OTLP · Prometheus remote write · Loki push · JSON
                                 │
                        ┌────────▼────────┐
                        │     ingest      │  memtable (fresh tail)
                        └───┬─────────▲───┘
        own writer log      │         │ tail reads
        (segments, flushes) │         │
┌───────────────────────────▼─────────┼───────────────────────────────┐
│                  OBJECT STORE (one or more pools)                   │
│  writer logs   partition manifests   data files   sidecar indexes   │
│  cluster catalog   control logs   rule state logs   leases          │
└────▲──────────────▲─────────────────┼──────────────▲────────────────┘
     │              │                 │              │
┌────┴────┐   ┌─────┴─────┐     ┌─────┴─────┐  ┌─────┴─────┐
│ compact │   │   eval    │     │   query   │  │ catalog & │
│ intake  │   │ continuous│     │ SQL PromQL│  │ control   │
│ compact │   │ queries,  │     │ LogQL     │  │ store     │
│ recover │   │ rollups,  │     │ TraceQL   │  │ (library  │
│ advisor │   │ alerts    │     │ search    │  │ in every  │
│ GC      │   │           │     │           │  │ role)     │
└─────────┘   └───────────┘     └───────────┘  └───────────┘
```

### 5.2 Roles

One binary, `tessera`, runs any combination of four roles:

| Role | Responsibility | State held |
|------|----------------|------------|
| `ingest` | Decode, validate, buffer, write its own log, acknowledge, flush to L1 files, serve the fresh tail | In-memory rows not yet in data files (recoverable from its log) |
| `query` | Parse, plan, prune, execute, cache | Caches only |
| `compact` | Intake, compaction, clustering, indexing, retention, writer recovery, layout advisor, garbage collection | None (leased work) |
| `eval` | Continuous queries: rollups and alert rules; notification dispatch | Operator state (checkpointed to the object store) |

The catalog and control store (section 12) are libraries used by every role, not separate processes.

- **AR-1** `tessera run` with no arguments MUST start all roles in one process against a local directory and be ready to accept writes and queries.
- **AR-2** Single-node and cluster modes MUST share the same storage, commit and query code paths. The local filesystem is an object-store implementation, not a special case.
- **AR-3** No role may require a peer of the same role to be reachable in order to make progress.

### 5.3 Work ownership

Some work is best done by one node at a time: committing to a partition manifest, running intake for a writer, evaluating a rule group, recovering a dead writer. Giving each such item its own lease would cost object-store requests in proportion to the number of items, so ownership is granted in **slots**.

- **AR-4** Each kind of single-owner work has a slot count *N* recorded in the cluster catalog. An item belongs to slot `hash(item key) mod N`. A node that holds the lease for a slot (section 14.2) owns every item in it.
- **AR-5** Slot counts are powers of two and are doubled automatically when owners report sustained overload. Doubling moves no data: each old slot becomes two, and the new leases are acquired independently.
- **AR-6** Ownership is a hint in the sense of P2. Two nodes that both believe they own an item cannot corrupt it, because every effect passes through a conditional write; they can only cause retries.
- **AR-7** An owner does metadata work only (planning, batching, committing). CPU- and I/O-heavy work is handed to any worker node of the role.

### 5.4 Scalability model

This table is the audit behind G9. For each dimension it names what grows, where coordination happens, and why that point does not become a ceiling.

| Dimension | Grows by | Coordination point | Why it is not a ceiling |
|-----------|----------|--------------------|-------------------------|
| Ingest throughput | Adding `ingest` nodes | None on the acknowledgment path | A writer appends only to its own log (ST-5); one registry write per writer and dataset lifetime (IN-6) |
| Stored volume | Adding storage pools | Cluster catalog (rare writes) | Object references carry their pool; pools are added at runtime (ST-2) |
| Object-store request rate | Pools and hashed key components | None | High-rate object classes spread over key space (ST-4) |
| Files per dataset | Partitions by kind and time window | Partition manifest log | Manifests are columnar tables read by range request, never loaded whole (MF-5); the partition window narrows for future windows (MF-2) |
| Manifest commit rate | Partitions; batching | One batching committer per partition | A commit carries any number of actions (CL-5); busy datasets get narrower windows |
| Compaction | Adding `compact` nodes | Partition owner plans and commits | Rewrites run on any worker (AR-7) |
| Concurrent queries | Adding `query` nodes | None | Any node can coordinate any query |
| Size of one query | Distributed stages with exchanges; spill to disk | Coordinator plans and streams results only | Intermediate stages run on worker nodes (QE-14); no stage requires one node to hold the whole input |
| Continuous queries | Adding `eval` nodes | Rule-group slot owner | Source tasks scale with writers and keyed state is hash-partitioned and rescalable (CQ-4, CQ-5) |
| Alert instances | Rule-group state partitions | One state log per partition | Transitions never go through a tenant-wide log (CQ-13) |
| Tenants and datasets | Lazy loading | Cluster catalog | Catalog checkpoints are columnar; an idle tenant uses storage only (CT-6) |
| Single-owner work | Slots | Slot leases | Slot count doubles under load (AR-5) |
| Nodes | Per-role membership pools | None | Membership is a hint; discovery falls back to a node registry in the object store (section 14.1) |

Limits that remain are per unit, not global: one event (1 MiB by default), one commit object, one state partition's memory, one partition's commit stream. Each unit can be made more numerous or smaller at runtime. The design is validated up to the envelope in section 17.2; beyond it nothing in the architecture changes, but it has not been measured.

## 6. Storage

### 6.1 Storage pools

- **ST-1** A deployment is started with one **root pool** (an object-store bucket or a prefix in one). The root pool holds the cluster catalog.
- **ST-2** Further pools MAY be added, weighted, drained and removed at runtime through the catalog. Every reference to an object in metadata is a (pool, key) pair. New objects are placed across pools by weight. This removes any per-bucket limit on volume or request rate.
- **ST-3** A pool's object store MUST provide create-if-absent writes, read-after-write consistency and strongly consistent listing. Stores without these are unsupported.
- **ST-4** Keys of object classes written at high rate (data files, sidecars, writer logs) MUST contain a hashed component so that the store can spread load across its key space.

### 6.2 Object layout

```
root pool
  _cluster/catalog/_log/{version:020}          cluster catalog (tenants, pools, slot counts)
  _cluster/leases/{role}/{slot}/{epoch}        slot leases
  _cluster/nodes/{node_id}                     node registry (discovery fallback)

any pool
  _wal/{writer_id}/{seq:012}                   writer log entries
  {tenant}/_control/_log/{version:020}         tenant control store
  {tenant}/_eval/{group}/{part}/_log/{version:020}    rule state log
  {tenant}/{dataset}/_log/{version:020}        dataset log (layout, tombstones, settings)
  {tenant}/{dataset}/_writers/{writer_id}      writer registry
  {tenant}/{dataset}/p/{kind}/{window}/_log/{version:020}       partition manifest log
  {tenant}/{dataset}/p/{kind}/{window}/d/{hh}/{file_id}.parquet data file
  {tenant}/{dataset}/p/{kind}/{window}/i/{hh}/{file_id}.idx     sidecar index
```

`{tenant}` is the tenant's immutable id, not its display name. `{writer_id}` is random, which satisfies ST-4 for writer logs; `{hh}` is a hash prefix of the file id.

### 6.3 Writer logs

Each ingester process owns one **writer log**: a strictly ordered sequence of entries, each an object created with create-if-absent at the next sequence number. No other node appends to it while the writer is alive.

| Entry | Written by | Meaning |
|-------|-----------|---------|
| Segment | Writer | A batch of acknowledged rows, for any number of tenants and datasets |
| Flush record | Writer, or recovery after a seal | "For dataset D, the rows in segments (r, s] are now also in these L1 files", with each file tagged by its partition |
| Seal | Recovery | The writer is declared dead; only recovery may append after this entry |
| End | Writer on clean shutdown, or recovery | The log is complete |

- **ST-5** A writer obtains a fresh random `writer_id` at start and appends entries with strictly increasing `seq`. Appending is the only write on the acknowledgment path.
- **ST-6** A segment is written when 250 ms have elapsed since the first buffered row or 8 MiB is buffered, whichever comes first (both tunable). A write request is acknowledged only after the segment containing it is durably stored.
- **ST-7** A segment contains one block per (tenant, dataset, kind), each an Arrow IPC stream with lightweight compression, and a footer indexing block offsets and observed-time bounds, so a reader fetches only the blocks it needs with range requests.
- **ST-8** An idle writer writes nothing. A writer ends its log and starts a new one after 1 h or 1,000,000 entries, which bounds recovery and listing work.
- **ST-9 (fencing)** To recover a writer believed dead, a recovery job creates a seal entry at the next unused `seq`. If the create succeeds, the writer can no longer append: its next append will collide, and on reading the seal it MUST stop. If the create fails, the writer is alive and has advanced; the job retries at the next `seq` or gives up.
- **ST-10** After a seal, recovery converts the remaining unflushed segments into L1 files, appends the flush records, and appends the end entry. Concurrent recovery jobs are serialized by the same create-if-absent rule.

### 6.4 Data files

- **ST-11** Data files are Apache Parquet. A file contains events of exactly one partition: one tenant, dataset, kind and event-time window (section 6.6).
- **ST-12** Each file's manifest entry records: time range, source (writer and segment range for L1, input files for higher levels), row count, size, layout epoch, sort key, per-column statistics (min, max, null count, distinct-count sketch) for dedicated columns, a Bloom filter over the set of attribute paths present, and references to sidecar indexes.
- **ST-13** Attributes without a dedicated column are stored in one `_overflow` column using the Parquet Variant encoding with shredding. *Verify at implementation time that the Rust Parquet implementation supports Variant adequately; the fallback is a map of path to tagged scalar.*
- **ST-14** Secondary indexes (Bloom filters, term dictionaries and postings, trigram indexes) live in a sidecar file per data file, in a Tessera-specific format. Data files MUST remain readable without their sidecar.
- **ST-15** The file format is accessed only through a `FileFormat` trait, so that the format decision stays local to one crate.

### 6.5 Commit logs

A **commit log** is a totally ordered sequence of commit objects. The cluster catalog, each tenant's control store, each dataset's settings, each partition's manifest and each rule-state partition are all commit logs and share one implementation.

```rust
/// A totally ordered, durable log of commits backed by create-if-absent writes.
#[async_trait]
pub trait CommitLog: Send + Sync {
    /// Latest committed version and a handle to the state at that version.
    async fn snapshot(&self) -> Result<Arc<Snapshot>>;

    /// Try to commit `actions` as version `base.version() + 1`.
    /// `Err(Conflict)` means another committer won; rebase and retry.
    async fn commit(&self, base: &Snapshot, actions: Vec<Action>)
        -> Result<Version, CommitError>;

    /// Commits after `from`, for followers.
    fn tail(&self, from: Version) -> BoxStream<'_, Result<Commit>>;
}

pub enum Action {
    // Partition manifest
    AddFile(FileMeta),
    RemoveFile { file_id: FileId, reason: RemoveReason },
    /// Every row of this partition in `writer`'s entries up to `through`
    /// is represented by files in this manifest.
    AdvanceWriter { writer: WriterId, through: Seq },
    DropPartition,
    // Dataset log
    SetLayout(LayoutSpec),
    SetPartitionWindow { from: Timestamp, window: Duration },
    AddTombstone(DeletePredicate),
    // Catalog, control store, rule state
    Kv(KvMutation),
}
```

- **CL-1** Version *N+1* is committed by creating the object for that version with create-if-absent. Exactly one committer can succeed for each version.
- **CL-2** On conflict a committer MUST re-read, test for logical conflict (for example, a compaction whose inputs were removed by another commit), and either rebase and retry or abandon.
- **CL-3** Checkpoints are written periodically and are tiered: a base checkpoint plus incremental ones, merged in the background, so that checkpoint cost is proportional to change, not to total size. A pointer to the latest checkpoint is a hint only (P2).
- **CL-4** In normal operation each log has one committer, its slot owner (section 5.3), which receives actions from other nodes and groups everything pending into the next commit.
- **CL-5** A commit MAY carry any number of actions. Commit throughput in actions per second is therefore bounded by commit object size, not by the number of commits.
- **CL-6** Any node MAY commit directly when the owner is unreachable (AR-3). CL-1 keeps this safe.

### 6.6 Partitions and manifests

A **partition** is one (tenant, dataset, kind, event-time window). It is the unit of manifest, of compaction and of retention.

- **MF-1** The window length is a property of the dataset, recorded in the dataset log (`SetPartitionWindow`). The default is one day.
- **MF-2** The advisor (section 8.8) adjusts the window for windows that have not started yet, between 5 minutes and 30 days, aiming for 1,000 to 100,000 files per partition. Existing partitions keep the window they were created with; the mapping from a timestamp to its partition is a deterministic function of the dataset log.
- **MF-3** A partition exists once the first commit of its manifest log exists. Nodes discover partitions by listing, cache the result, and learn of new ones through hints.
- **MF-4** A partition manifest holds the set of data files of the partition and, per writer, the `AdvanceWriter` position.
- **MF-5** Manifest checkpoints are Parquet files sorted by time and sort-key bounds. A reader prunes files by scanning the checkpoint with range requests and applies the commits after it. No node is ever required to hold a whole manifest in memory.
- **MF-6** Dropping a partition (retention by age) is one `DropPartition` commit followed by garbage collection.

### 6.7 Intake

Intake moves L1 files from writer logs into partition manifests. It is metadata work only.

- **IT-1** For each (dataset, writer) pair, the intake owner reads the writer's flush records in order. For each record and each partition it touches, it asks that partition's owner to commit, atomically, `AddFile` for the record's files in that partition and `AdvanceWriter { writer, through: s }`, where `s` is the last segment the record covers.
- **IT-2** Adoption is idempotent: a partition ignores a request whose `through` is not beyond its current position for that writer.
- **IT-3** When a writer's log has ended and every flush record that names a dataset has been adopted by every partition it touches, intake removes the writer from that dataset's registry.
- **IT-4** Intake lag affects only how much data is read through the tail path (section 6.8). It never affects correctness or acknowledgment.

### 6.8 The consistent view

A query, a compaction or a continuous-query rebuild sees a partition *P* of dataset *D* at manifest version *V* as:

```
rows(P, V) =  rows in the files listed in manifest(P) at V
            ⊎ for every writer w in registry(D):
                 rows of P in w's segments with seq > position(P, V, w)
```

where `position(P, V, w)` is the `AdvanceWriter` position recorded at *V*, or zero if there is none.

- **VW-1** The union has no gaps and no duplicates, because a partition adopts files and advances its position in one commit (IT-1), and a writer registers with a dataset before acknowledging any row for it (IN-6).
- **VW-2** A reader MUST read the dataset's writer registry before it reads partition manifests. A writer missing from the registry was either fully adopted before the manifests were read, or registered after the read began.
- **VW-3** The rows beyond a position may be obtained from any of three equivalent sources: the owning ingester's memory, the L1 files named in the writer's flush records, or the segments themselves in the object store. The first two are optimizations. If the ingester is unreachable the reader MUST fall back to the object store, so ingester reachability affects latency and never correctness.
- **VW-4** Tail rows held in memory are scanned without indexes. The L1 flush interval bounds how much data that is.

### 6.9 Levels and compaction

| Level | Contents | Produced by | Typical size |
|-------|----------|-------------|--------------|
| L0 | Writer log segments, multi-tenant | ingest | ≤ 8 MiB |
| L1 | Parquet, default sort, statistics and path filter only | ingest flush, when a dataset's buffer reaches 64 MiB or its oldest row is 2 min old | 1–64 MiB |
| L2 | Parquet, current layout, full indexes | compaction | ~512 MiB |
| L3 | As L2, after retention sampling, final clustering and trace completion (section 9.5) | compaction | ~512 MiB |

- **CP-1** Compaction never crosses partitions. Inputs and outputs belong to one manifest, so every compaction is one commit to one log.
- **CP-2** Within a partition, files are grouped by ingest-time window, and windows widen with level. Sort-key pruning works inside each group.
- **CP-3** Compaction is the single place where the following are applied: re-sorting to the current layout, column promotion and demotion, index building, deduplication, tombstones, pattern assignment, sampling and downsampling.
- **CP-4** The partition owner selects jobs and hands them to workers. A worker writes its output files and returns their metadata; the owner commits. A worker whose job was superseded leaves garbage, never an inconsistent manifest.
- **CP-5** Late-arriving data (a row whose event time falls in an old window) creates or reopens that window's partition like any other write.

### 6.10 Garbage collection

- **GC-1** A data file or sidecar is deleted once no manifest version newer than the snapshot-retention window (default 24 h) references it.
- **GC-2** A writer-log segment is deleted once (a) every partition touched by its rows has adopted past it, (b) a reader grace period has elapsed since then (default 1 h; queries running longer MUST revalidate their view), and (c) every continuous query has checkpointed past it, with a hard cap of 24 h (section 11.7 covers consumers that fall behind the cap).
- **GC-3** Deletion is always preceded by the commit that makes the object unreachable. A reader that finds an object missing MUST reload its view and retry.

## 7. Ingestion

### 7.1 Protocols

- **IN-1** OTLP over gRPC and HTTP for logs, traces and metrics (primary path).
- **IN-2** Prometheus remote write.
- **IN-3** Loki push API.
- **IN-4** HTTP JSON lines for arbitrary events: any JSON object with a timestamp is a valid event of kind `event`.
- Further endpoints are listed in section 16.

### 7.2 Pipeline

```
decode → authenticate, resolve tenant, authorize, apply limits
       → normalize to Arrow batches → redact (policy) → buffer
       → append segment to own writer log → acknowledge → memtable
       → L1 flush → flush record
```

- **IN-5** Normalization flattens nested maps, assigns `series_id` for metrics, splits span events into their own rows (DM-8), parses structured bodies where enabled (DM-10), sets `observed_timestamp`, and tags each batch with tenant, dataset and kind.
- **IN-6 (registration)** Before a writer acknowledges its first row for a dataset, it MUST have created its entry in that dataset's writer registry. This is one object write per writer and dataset, performed in parallel with the first segment write.
- **IN-7** Ingest MUST NOT require a synchronous schema registration. New attribute paths appear in files and are discovered from file metadata; the catalog's column list is derived, not authoritative.

### 7.3 Routing

- **IN-8** Any ingester MUST accept any write. There is no partition ownership on the write path and nothing to rebalance (G1).
- **IN-9** To keep the number of writers per dataset small, an ingester SHOULD forward a batch to one of a **preferred set** of ingesters for that dataset, chosen by rendezvous hashing over live ingesters. The set's size follows the dataset's recent volume. Forwarding is a hint: if the preferred ingester is unreachable or overloaded, the receiving ingester writes the batch itself.

### 7.4 Durability and backpressure

- **IN-10** Default acknowledgment means "durable in the object store". A per-request `ack=buffered` option MAY acknowledge before the segment is stored, for callers that prefer latency to durability.
- **IN-11** Under overload, ingest MUST reject with a retryable status and a retry delay, per tenant, before memory limits are reached. One tenant's overload MUST NOT cause rejections for others.

### 7.5 Delivery semantics and deduplication

Ingest is at-least-once. Deduplication happens at compaction, on identity:

| Kind | Identity |
|------|----------|
| metric | (`series_id`, `timestamp`); last write wins |
| span | (`trace_id`, `span_id`) |
| log, event, alert | client-supplied id (`log.record.uid` or `event_id`) when present; otherwise not deduplicated |

- **IN-12** Queries over not-yet-compacted data MAY see duplicates caused by client retries. A query option `dedup=exact` forces deduplication at read time at higher cost.

### 7.6 Limits (defaults, per tenant, changeable at runtime)

Maximum event size 1 MiB; maximum 2,000 attributes per event; maximum attribute path length 512 bytes; events with `timestamp` more than 7 days in the past or 1 h in the future are rejected.

## 8. Adaptive physical design

This is the part of the system that delivers G3. It has three components: a workload log, an advisor, and lazy application at compaction.

### 8.1 Workload log

- **AD-1** Every query appends an event to the system dataset `_system.queries` recording, per scanned dataset and kind: referenced columns; predicates with operator and observed selectivity; grouping and ordering columns; files considered, files pruned, bytes scanned and bytes returned; latency; and the principal, language and source (dashboard, alert rule, ad hoc).
- **AD-2** Workload events are weighted with exponential decay (half-life 7 days by default) so that the layout follows current use.

### 8.2 Column promotion

A path receives a dedicated column in a file being written when any of these hold: it is a well-known column; its density among the file's rows is at least 1%; or it is in the advisor's hot set (referenced by recent queries).

- **AD-3** At most 1,024 dedicated columns per file (tunable). Beyond the cap, lower-scoring paths stay in `_overflow`.
- **AD-4** A query for a path reads the dedicated column where one exists and extracts from `_overflow` elsewhere. Files whose path Bloom filter excludes the path are skipped entirely.

### 8.3 Sort key selection

For each (tenant, dataset, kind) the advisor chooses a sort key of up to four columns followed by `timestamp`.

```
cost(L) = Σ_q  w_q · est_bytes_scanned(q, L)      // decayed workload
        + λ · rewrite_bytes(current → L)          // cost of changing
        + μ · storage_bytes(L)                    // compression effect
```

- **AD-5** Selection is greedy: start from the empty key and repeatedly add the column that most reduces cost, preferring lower-cardinality columns earlier, until no addition improves cost by more than a threshold.
- **AD-6** Estimates use per-column sketches (distinct count, quantiles) and the selectivities measured in the workload log.
- **AD-7** A new layout is adopted only if it beats the current one by a margin (default 20%) for two consecutive advisor runs, to prevent oscillation.
- Multi-dimensional clustering (space-filling curves) is a later extension behind the same interface.

### 8.4 Index selection

| Index | Built when | Purpose |
|-------|-----------|---------|
| Min/max statistics | Always, all dedicated columns | Range pruning |
| Bloom filter | Workload shows equality predicates on a high-cardinality column | Point lookups (`trace_id`, request ids) |
| Term index | Workload shows token search on a string column | Text search (section 9.7) |
| Trigram index | Workload shows substring or regex predicates | `LIKE '%x%'`, regex, LogQL line filters |

- **AD-8** Index choices are recorded per file. A query MUST produce the same result whether or not an index exists.

### 8.5 Defaults before any workload exists

| Kind | Default sort key | Default indexes |
|------|------------------|-----------------|
| log | `service`, `severity_number`, `timestamp` | term and trigram index on `body`; Bloom on `trace_id` |
| span | `service`, `name`, `timestamp` | Bloom on `trace_id` |
| metric | `name`, `series_id`, `timestamp` | none |
| event | `name`, `timestamp` | Bloom on `trace_id` |
| alert | `name`, `timestamp` | none |

### 8.6 Applying layouts

- **AD-9** A layout is versioned by a **layout epoch** stored in the dataset log (`SetLayout`). New L2 and L3 files use the newest epoch. Files of older epochs remain valid and queryable.
- **AD-10** Re-clustering old files is a budgeted background job that rewrites a file only when predicted savings over its remaining retention exceed the rewrite cost. Recent partitions are prioritized.
- **AD-11** `EXPLAIN LAYOUT <dataset>` MUST show the current layout, the alternatives considered, their estimated costs and the queries that drove the choice.
- **AD-12** Users MAY pin a sort key, a dedicated column, an index or a partition window. Pins are stored in the control store and are never required (P6).

### 8.7 Automatic rollups

The advisor detects repeated aggregation shapes (same filters, grouping and window, typically from dashboards and alert rules) and proposes a rollup: a continuous query (section 11) whose output is written to a derived dataset. The planner rewrites matching queries to read the rollup.

- **AD-13** Automatic rollups MUST be transparent: results equal the un-rewritten query within documented sketch error bounds, and a query option disables rewriting.

### 8.8 Partition window selection

- **AD-14** The advisor sets each dataset's partition window from observed file counts and commit rates (MF-2), narrowing it for high-volume datasets and widening it for small ones so that long-retention, low-volume tenants do not accumulate many near-empty partitions.

## 9. Query languages

### 9.1 Overview

| Language | Used for | Entry points |
|----------|----------|--------------|
| SQL (PostgreSQL-flavored) | Analysis, joins across signals, administration, control tables | PostgreSQL wire protocol, HTTP, Arrow Flight SQL |
| PromQL | Metrics dashboards and rules | Prometheus HTTP API |
| LogQL | Log dashboards, exploration and rules | Loki HTTP API |
| TraceQL | Trace search and trace metrics | Tempo HTTP API |
| Search syntax | Fast interactive filtering across kinds | HTTP, and `search('...')` inside SQL |

- **QL-1** All five languages MUST compile to the same plan representation and run on the same engine (section 10). No language has a private storage path or a private executor.
- **QL-2** Each language has its own hand-written lexer and parser in this code base, producing precise error positions and messages in the terms of that language.
- **QL-3** Languages MUST be composable inside SQL through table functions: `promql('rate(http_requests_total[5m])', start, end, step)`, `logql('{app="checkout"} |= "timeout"', start, end)`, `traceql('{ span.http.response.status_code >= 500 }', start, end)`, and the predicate `search('timeout -healthcheck')`.
- **QL-4** For PromQL, LogQL and TraceQL the reference implementation defines the semantics. Each is tested differentially against its reference on identical data, and every deviation MUST be documented.

### 9.2 SQL

The dialect follows PostgreSQL for syntax, types and functions where they apply, with these additions:

- Time functions for observability: `tumble`, `hop`, `time_bucket`, `rate`, `increase`, `histogram_quantile`, sketch aggregates (`approx_distinct`, `approx_quantile`).
- `ASOF JOIN` for aligning series and events in time.
- Trace functions: `trace(id)`, `span_tree(id)`, `root_span`, `critical_path`.
- Statements for the control plane: `CREATE ALERT`, `CREATE ROLLUP`, `CREATE RETENTION POLICY`, `CREATE TENANT`, `EXPLAIN LAYOUT`, `EXPLAIN RETENTION`.
- Path syntax for dynamic attributes: `attr."http.request.method"` or `attr.http.request.method` where unambiguous.

### 9.3 PromQL

- **QL-5** The implementation MUST follow Prometheus evaluation semantics: instant and range queries, lookback delta, staleness, counter-reset handling in `rate` and `increase`, subqueries, the `@` and `offset` modifiers, and native histograms.
- **QL-6** Target: pass the Prometheus PromQL compliance suite.
- Selector evaluation is a columnar scan over `name` and attribute columns in metric sort order, not an inverted-index lookup (DM-7).

### 9.4 LogQL

- **QL-7** Supported: stream selectors with all four matcher types; line filters (`|=`, `!=`, `|~`, `!~`) including chained and `or` forms; parser stages (`json`, `logfmt`, `pattern`, `regexp`, `unpack`); label filter expressions; `line_format`, `label_format`, `drop`, `keep`, `decolorize`; and metric queries (`rate`, `count_over_time`, `bytes_over_time`, `bytes_rate`, `sum_over_time` and the other unwrapped range aggregations, `quantile_over_time`, `absent_over_time`, aggregation operators, binary operations, `topk`, `bottomk`).
- **QL-8** A stream selector matches any attribute of a log event, resource or record, under the name mapping of section 9.8. Tessera has no distinction between stream labels and structured metadata, and no cardinality limit on what a selector may reference.
- **QL-9** Line filters use the term and trigram indexes where they exist (section 9.7).
- **QL-10** A parser stage is resolved by the planner against columns already stored (`body.*`, DM-10). Only fields that are not stored are extracted at run time.
- **QL-11** Live tailing follows the writer logs of the dataset with the query's filter applied, so tail latency equals WAL latency.

### 9.5 TraceQL

- **QL-12** Supported: spanset filters with intrinsics (`duration`, `name`, `status`, `statusMessage`, `kind`, `rootName`, `rootServiceName`, `traceDuration`) and scoped attributes (`span.`, `resource.`, `event.`, `link.`, and unscoped); spanset logical operators; structural operators (descendant, child, sibling, ancestor, parent, their negated and union forms); aggregate filters (`count`, `avg`, `min`, `max`, `sum`); pipelines; `select` and `by`; and TraceQL metrics (`rate`, `count_over_time`, `quantile_over_time`, `histogram_over_time` and related functions).
- **QL-13** Structural operators are evaluated by the `TraceGroup` operator (section 10.6): spans matching each side are grouped by `trace_id`, and relations are evaluated on the per-trace tree. When ancestry between two matching spans passes through spans that matched neither side, the operator fetches the trace's skeleton (`span_id`, `parent_span_id`) by `trace_id`.
- **QL-14 (trace completion)** When a span partition is older than the trace-completion horizon (default 1 h), compaction to L3 adds, for every trace that lies wholly within the partition, nested-set columns (`nest_left`, `nest_right`, `nest_parent`) and trace-level columns (`root_name`, `root_service`, `trace_duration`). On such files, structural operators and trace intrinsics are plain column predicates. Files without these columns use QL-13; results are identical (AD-8).
- **QL-15** The `event.` scope reads span events stored as `event` rows (DM-8); the `link.` scope reads the `links` column (DM-9).

### 9.6 Search syntax

A small filter language for interactive use: `service:checkout severity>=error "connection reset" duration>500ms -attr.user.tier:free`. Bare terms match `body`; `field:value` is equality; quoted strings are phrases; `-` negates. It applies to any kind.

### 9.7 Text matching

- **QL-16** Token search uses the term index: a per-file dictionary of tokens with compressed postings at row-group granularity, followed by an exact check on the surviving rows.
- **QL-17** Substring and regular-expression predicates, from any language, use the trigram index where one exists and fall back to a scan otherwise. A regular expression is analyzed for required literals, which drive the index lookup.
- **QL-18** Tokenization is fixed per layout epoch (lowercased, split on non-alphanumeric characters, with additional tokens for common compound forms such as dotted and hyphenated identifiers) so that an index and a query always agree.
- Results are ordered by time. There is no relevance scoring (section 2.2).

### 9.8 Names across languages

Attribute paths contain dots; Prometheus and Loki label names cannot.

- **QL-19** In PromQL and LogQL, a label name refers to the attribute whose path equals the name after replacing characters outside `[a-zA-Z0-9_]` with `_`. Metric names additionally follow the OpenTelemetry-to-Prometheus rules for unit and `_total` suffixes. The mapping is applied in both directions, on ingest through Prometheus and Loki protocols and on query.
- **QL-20** If two attributes of a dataset map to the same label name, the more specific scope wins (`attr.` over `resource.` over `scope.`), and the collision is reported in `system.schema_warnings`.
- **QL-21** TraceQL and SQL use attribute paths directly.

## 10. Query engine

Tessera has its own query engine. This is a deliberate choice: the workload is narrow (time-ordered, append-only, wide and sparse events; a few dominant query shapes), and the engine is built around those shapes instead of general analytics.

### 10.1 Scope

- **QE-1** Built in this code base: the parsers (section 9), the binder and catalog, the plan representation, the optimizer, the physical planner, the vectorized kernels and operators, the scheduler, distributed execution, and the incremental runtime (section 11).
- **QE-2** Reused: the Apache Arrow memory layout as the vector format, with its Rust array containers and IPC encoding, and the Rust Parquet page encoders and decoders. The scan path above the page level (I/O scheduling, range coalescing, pruning, late materialization) is Tessera's own. No third-party planner, optimizer or executor is a dependency.

### 10.2 Plan representation

One relational algebra with time-series extensions. Front ends lower into it; nothing after lowering knows which language a query came from.

| Operator | Purpose |
|----------|---------|
| `Scan` | Read a set of partitions with pushed-down predicates, projection, order and limit |
| `Filter`, `Project`, `Unnest` | Row-level work, including variant access |
| `Aggregate`, `WindowAggregate` | Grouped and time-bucketed aggregation |
| `RangeVector` | Per-series sliding windows with PromQL and LogQL semantics |
| `Join` | Hash, merge, as-of, and lookup-by-key variants |
| `TraceGroup` | Group spans by trace and evaluate structural predicates |
| `Extract` | Derive fields from text (pattern, regexp, JSON, logfmt) |
| `TopK`, `Sort`, `Limit` | Ordering |
| `Union` | Combine files, tail sources and kinds |
| `Exchange` | Move data between tasks: hash, range, broadcast, gather |

- **QE-3** Every plan node carries its time range, required ordering and weight semantics (QE-13), so that rewrites preserve them.
- **QE-4** `EXPLAIN` and `EXPLAIN ANALYZE` work for a query in any language and show the lowered plan.

### 10.3 Optimizer

- **QE-5** Rewrites are rule-based where they are always beneficial and cost-based where they are not. Costs come from manifest statistics, sketches and the workload log's measured selectivities.
- **QE-6** Required rewrites: predicate and projection pushdown into `Scan`; time-range inference in every language; index selection; aggregate pushdown below exchanges; limit and order pushdown with early termination; join ordering with runtime filters (for example, a set of `trace_id` values found on one side becomes a Bloom filter on the other); rollup substitution (AD-13); resolution of extraction stages against stored columns (QL-10); and elimination of common subplans across the queries of one dashboard request.

### 10.4 Pruning

Pruning proceeds from cheapest to most expensive, and each stage sees only what survived the previous one:

1. **Dataset, kind and partitions**, from the query's time range.
2. **File time ranges and sort-key bounds**, from the manifest.
3. **Path presence**, against each file's attribute-path Bloom filter.
4. **Column statistics**, min/max per file from the manifest.
5. **Sidecar indexes**: Bloom, term and trigram, per file.
6. **Row groups and pages**, using Parquet statistics and page indexes.
7. **Late materialization**: evaluate predicates on filter columns first, then fetch the remaining columns only for matching rows.

- **QE-7** Stages 1 to 4 use manifest data only and read no data file. For large manifests they run as a distributed scan of the manifest checkpoints (MF-5).
- **QE-8** `EXPLAIN ANALYZE` MUST report, per stage, how many files, row groups and bytes were eliminated.

### 10.5 Execution model

- **QE-9** Execution is vectorized and push-based. A plan is split into pipelines; pipelines run as morsel-sized tasks on a work-stealing scheduler bound to a dedicated CPU pool (RS-4).
- **QE-10** Kernels operate on encoded data where possible: dictionary, run-length and constant vectors are filtered, compared, grouped and joined without decoding. Filters produce selection vectors, not copies.
- **QE-11** Every blocking operator (aggregate, sort, join, trace grouping) accounts its memory and spills to local disk when its grant is exhausted.
- **QE-12** Execution is adaptive: operators report actual cardinalities, and the scheduler may change parallelism, swap join sides or repartition between stages.

### 10.6 Specialized operators

These are the reasons to own the engine.

| Operator or kernel | What it exploits |
|--------------------|------------------|
| Time-ordered top-K scan | "Latest N matching rows" reads files newest first and stops when N rows older than every unread file are found |
| Sorted time-bucket aggregation | Files sorted by key then time are aggregated in a streaming pass with no hash table |
| Series-at-a-time range vectors | In metric sort order each series is contiguous; windows slide over it with memory proportional to the window, in parallel across series |
| Counter and histogram kernels | Reset-aware `rate` and `increase`; merging of classic and exponential histograms; quantile estimation |
| `TraceGroup` | Tree construction per trace; nested-set predicates on completed traces (QL-14) |
| Text kernels | SIMD substring search, multi-pattern matching, regular expressions with literal prefilters |
| Variant access | Reads a dedicated column where a file has one and the shredded overflow elsewhere, behind one expression |
| Tail scan | The same operators run over ingester memtables and writer-log segments |

- **QE-13 (weights)** Aggregate functions are weight-aware. Over data with `sample_rate < 1`, counts, sums, rates and histograms scale each row by `1 / sample_rate` by default; `_raw` variants ignore weights. PromQL, LogQL and TraceQL metric functions inherit this.

### 10.7 Distributed execution

- **QE-14** A physical plan is a graph of stages separated by exchanges. Leaf stages scan; intermediate stages (repartitioned aggregation, joins, trace grouping, sorts) run on any `query` nodes; the coordinator plans, schedules and streams the final stage to the client. No stage requires a single node to hold its whole input.
- **QE-15** Leaf tasks are assigned by rendezvous hashing on file id so that a file tends to be scanned by the node that has it cached. Assignment is a hint: any node produces the same answer.
- **QE-16** Leaf tasks over the tail run on the ingester that owns the rows, with the predicate pushed down, or on a `query` node reading L1 files or segments (VW-3).
- **QE-17** Leaf tasks are idempotent and are retried on another node if a node fails. Results are streamed with backpressure end to end.

### 10.8 Caching

Three caches, all local to a node and all disposable: manifest pages and file footers in memory; object byte ranges on local disk; and query results keyed by (plan fingerprint, manifest versions, time bucket) for closed time ranges.

### 10.9 Limits and fairness

- **QE-18** Every query runs under per-tenant limits on bytes scanned, memory, concurrency and wall time. A query that exceeds its memory grant MUST spill or fail by itself; it MUST NOT take down the node.
- **QE-19** Scheduling is fair across tenants and prioritizes alert evaluation and interactive queries over background and export work.

### 10.10 One implementation per function

- **QE-20** Each scalar, aggregate and window function has one implementation, used by batch execution and by the incremental runtime alike. This is what makes QL-4's differential tests and the incremental-versus-batch tests (section 18.5) meaningful.

## 11. Continuous queries and alerting

### 11.1 Model

A **continuous query** is a query registered with the system whose result is maintained incrementally as new data arrives. Rollups (section 8.7) and alert rules are both continuous queries; they differ only in their sink: a derived dataset or the alert state machine.

- **CQ-1** Continuous queries consume writer-log segments (L0) directly. They never wait for data files, so their latency is bounded by WAL latency.
- **CQ-2** A continuous query reads only the segment blocks for its tenant, dataset and kinds, using the segment footer (ST-7).

### 11.2 Definition

- **CQ-3** Rules are defined in SQL, PromQL or LogQL. Prometheus rule files and Loki ruler files are accepted unchanged. TraceQL metrics queries MAY be used as rule expressions.

```sql
CREATE ALERT checkout_errors
  SEVERITY critical
  FOR 2m
  NOTIFY 'oncall-payments'
AS
  SELECT service, count(*) AS errors
  FROM logs
  WHERE severity >= 'error' AND service = 'checkout'
  GROUP BY service, tumble(timestamp, INTERVAL '1 minute')
  HAVING count(*) > 100;
```

Each distinct group in the result is one **alert instance**, identified by a fingerprint of its grouping values.

### 11.3 Dataflow

A rule group runs as a two-stage dataflow so that neither the input rate nor the number of groups is bounded by one node.

```
writer logs ──▶ source tasks ──hash(group key)──▶ state partitions ──▶ sinks
               (stateless)                         (keyed state, one state log each)
```

- **CQ-4** **Source tasks** read segments for a set of writers, apply each rule's filter, projection and partial aggregation, and emit one batch per (segment, state partition). They are stateless and deterministic, and their number follows the number of writers.
- **CQ-5** **State partitions** hold the windows and aggregates for the group keys that hash to them. Each records, per writer, the last segment it has applied, and ignores input at or before that position, so source retries are harmless.
- **CQ-6** The number of state partitions of a rule group is changed at runtime by restoring from checkpoints and redistributing keys. Ownership of source tasks and state partitions is by slot (section 5.3).

### 11.4 Incremental operators

- **CQ-7** The supported incremental subset is: filter, projection, extraction, tumbling, hopping and sliding windows, grouped aggregation with mergeable aggregates (count, sum, min, max, average, sketches, histograms), PromQL and LogQL range-vector functions, and absence detection.
- **CQ-8** A rule outside this subset (for example, one with an arbitrary join) is evaluated by periodic re-execution at a configured interval. `EXPLAIN` on a rule MUST state which mode applies and why.

### 11.5 Time, watermarks and firing

Windows are defined on event time. Progress is measured in observed time: a writer's watermark is the observed time of its last segment, or the current time minus an idle timeout (default 2 s) if it has written nothing since. A dataset's watermark is the minimum over its registered live writers, minus an allowed lateness (default 10 s).

- **CQ-9 (eager firing)** If a condition is monotone in arriving data (once true for a window, more data cannot make it false, as with `count(*) > N` or `max(x) > N`), it fires as soon as it becomes true, without waiting for the watermark.
- **CQ-10 (watermark firing)** Any other condition (averages, ratios, `<` on a growing aggregate, absence) is evaluated when the watermark passes the end of the window.
- **CQ-11** Data arriving later than the allowed lateness is stored and queryable but does not retroactively change alert state. The count of such rows per rule is exposed as a metric.

### 11.6 State and exactly-once

- **CQ-12** Each state partition has a **state log** (a commit log, section 6.5). One commit records, atomically: the operator state checkpoint, the per-writer input positions, and the alert state transitions produced since the previous commit. On takeover, the new owner loads the last commit and replays input from those positions. A stale owner's commit fails by CL-1.
- **CQ-13** Alert instance state lives in state logs, never in a tenant-wide log. `system.alert_instances` is a view over them.
- **CQ-14** Each transition is also written as an `alert` event with a deterministic `event_id`, so replays are removed by deduplication (section 7.5).
- **CQ-15** Notifications follow the outbox pattern: a transition is committed first, then sent, then marked sent. Delivery is at least once and carries an idempotency key (rule, fingerprint, transition number).

### 11.7 Falling behind

If a state partition falls behind the WAL retention cap (GC-2), it rebuilds window state by querying data files for the affected windows through the consistent view (section 6.8) and then resumes from the live logs. Rules in this condition are marked `degraded`, which is itself alertable.

### 11.8 Alert lifecycle

```
inactive ──condition true──▶ pending ──held for FOR──▶ firing ──condition false──▶ resolved
                                │                         │
                                └──condition false────────┴──▶ inactive
```

Silences, inhibition rules, acknowledgments and routing are rows in the tenant's control store (section 12.3), changed transactionally and visible to every evaluator.

### 11.9 Notifications

Built-in sinks: webhook, email, Slack-compatible webhook, PagerDuty-compatible events, and Alertmanager-compatible push for organizations that keep an existing Alertmanager. Grouping, repeat intervals and escalation follow Alertmanager's model.

## 12. Catalog, tenants and control store

### 12.1 Cluster catalog

- **CT-1** The cluster catalog is a commit log in the root pool. It holds tenants, storage pools, slot counts, cluster-wide settings and cluster administrators. It is the only cluster-wide mutable state, and it changes at human rate.

### 12.2 Tenants

Tenants are objects in the software, managed while it runs.

```sql
CREATE TENANT acme WITH (ingest_rate = '200 MB/s', retention = '30 days');
ALTER TENANT acme SET query_concurrency = 64;
ALTER TENANT acme RENAME TO acme_corp;
DROP TENANT acme_corp;
```

- **CT-2** Tenants are created, altered, suspended and dropped through SQL or the HTTP administration API, by a cluster administrator. No restart or configuration file change is involved.
- **CT-3** A change takes effect on every node within 5 s, through the catalog log and hints.
- **CT-4** Two tenants always exist: `default`, so that a single-team installation needs no tenant setup, and `_system`, which receives Tessera's own telemetry (section 14.7).
- **CT-5** A request's tenant is resolved from its credentials: an API key belongs to one tenant; an OIDC token is mapped by a configured claim; and on the Prometheus, Loki and Tempo endpoints the `X-Scope-OrgID` header selects among the tenants the authenticated principal may act for. Unauthenticated access to `default` is possible only when explicitly enabled; it is enabled by `tessera run` in single-node mode.
- **CT-6** Nodes load a tenant's state on first use and release it when idle. A tenant that sends and queries nothing consumes storage only. The number of tenants is not bounded by node memory.
- **CT-7** Per-tenant settings, all changeable at runtime: ingest rate and burst; event limits (section 7.6); query limits (QE-18); default retention; storage pool placement; encryption key; structured-body parsing; and feature switches.
- **CT-8** Dropping a tenant makes it unreachable at once and deletes its data after a grace period (default 7 days), during which it can be restored.
- **CT-9** A principal with grants in several tenants MAY query across them by qualifying names (`acme.prod.logs`). Without such grants, a query sees one tenant only.

### 12.3 Control store

The control store is the "small transactional corner": a per-tenant key-value store, whose only durable form is the tenant's control log (section 6.5). It holds configuration that people change, not state that machines churn.

- **CS-1** Transactions are serializable. A transaction reads from a snapshot, and its commit succeeds only if no key in its read set changed since that snapshot; otherwise it is retried.
- **CS-2** Nodes that serve a tenant tail its control log. A committer always sees its own writes; other nodes observe a commit within 1 s or immediately when hinted.
- **CS-3** The store is exposed in SQL as ordinary tables in the `system` schema, with `INSERT`, `UPDATE` and `DELETE`: `system.alert_rules`, `system.rollups`, `system.silences`, `system.acknowledgments`, `system.notification_routes`, `system.retention_policies`, `system.layout_pins`, `system.api_keys`, `system.roles`, `system.saved_queries`.
- **CS-4** Machine-rate state is kept elsewhere by design: alert instances in state logs (CQ-13), usage in system datasets (section 13.1), schemas in file metadata (IN-7). The control store therefore stays small however large the tenant is.
- **CS-5** Control tables can be joined with telemetry: `SELECT ... FROM alerts JOIN system.silences ...`.

## 13. Cost and retention

### 13.1 Metering

- **CO-1** Ingest records bytes and events received per (tenant, dataset, kind, service, team). `team` is derived from a configurable attribute mapping.
- **CO-2** Compaction records stored bytes per the same dimensions plus `pattern_id` for logs and `name` for metrics and spans.
- **CO-3** Queries record bytes scanned, CPU time and object-store requests per principal, language, source and query fingerprint (shared with the workload log, section 8.1).
- **CO-4** All of it is queryable as `system.usage`. An operator MAY configure unit prices so that usage is also shown as cost, for internal chargeback.

### 13.2 Log patterns

At compaction, log bodies are clustered into templates (a Drain-style algorithm: `connection to <*> timed out after <*> ms`), and each row receives a `pattern_id`. Patterns and their volumes are listed in `system.log_patterns`.

- **CO-5** Pattern assignment MUST be stable: a template keeps its id across files and compactions.
- Rows in L1 files have no `pattern_id` yet; cost by pattern is exact only for compacted data.

### 13.3 Retention by value

Each group of data (a log pattern, a metric name, a span name per service) has a **value score** derived from how often queries, dashboards and alert rules read it relative to what it costs to store. Policies act on age, on value, or both:

```sql
CREATE RETENTION POLICY trim_noise ON prod AS
  WHEN kind = 'log' AND severity < 'warn' AND value_score < 0.01
    THEN SAMPLE 0.05 AFTER INTERVAL '3 days', DROP AFTER INTERVAL '30 days'
  WHEN kind = 'metric'
    THEN DOWNSAMPLE TO INTERVAL '5 minutes' AFTER INTERVAL '30 days'
  ELSE DROP AFTER INTERVAL '90 days';
```

- **CO-6** Sampling sets `sample_rate` on surviving rows to the fraction kept, so weighted aggregates remain unbiased (QE-13).
- **CO-7** Span sampling MUST be consistent per trace (a deterministic function of `trace_id`), so that traces are kept or dropped whole. Traces containing errors or referenced by alert events MAY be exempted by policy.
- **CO-8** Metric downsampling keeps count, sum, min, max and last per interval, and merges histograms and sketches, so that PromQL functions remain answerable.
- **CO-9** Policies are applied at compaction (CP-3); dropping by age alone removes whole partitions (MF-6). `EXPLAIN RETENTION` MUST show what a policy would remove and what it would save before it is enabled.
- **CO-10** The system MAY recommend policies; it MUST NOT reduce data by value without an enabled policy.

### 13.4 Budgets

Tenants and teams MAY have ingest and query budgets. On reaching an ingest budget, the configured action applies: alert only (default), sample at ingest, or reject.

## 14. Cluster and operations

### 14.1 Membership

Nodes discover each other through gossip, in one pool per role, seeded by DNS, a static list, or the node registry in the root pool (`_cluster/nodes/`). Membership is used for query fan-out, tail reads, forwarding and hints. It is a hint (P2): a wrong membership view can slow the system down but cannot make it incorrect.

### 14.2 Leases

Slots (section 5.3) are owned through leases stored in the root pool.

- **OP-1** A lease is a sequence of epoch objects `_cluster/leases/{role}/{slot}/{epoch}`, each created with create-if-absent and naming a holder and a duration. Renewal is the holder creating the next epoch; takeover is another node creating it after the current one has expired.
- **OP-2** Safety never depends on clocks. Every effect of leased work passes through a conditional write (a commit, a log append, an idempotent notification), so a holder that has lost its lease cannot take effect. Clock skew affects only how quickly takeover happens.
- **OP-3** Default lease duration 30 s, renewal every 10 s. The request cost of leases is proportional to the number of slots, not to the number of partitions, writers or rules.

### 14.3 Scaling

- **OP-4** Adding a node of any role requires starting the process and nothing else. Removing one requires stopping it.
- **OP-5** Every node exports load signals (ingest queue depth, query queue time, compaction and intake backlog, evaluator lag, slot load) suitable for an autoscaler.

### 14.4 Failure behavior

| Failure | Effect | Recovery |
|---------|--------|----------|
| Ingester crash | In-flight unacknowledged writes fail and are retried by clients | Log sealed (ST-9) and flushed by recovery (ST-10); its tail stays readable from the object store meanwhile (VW-3) |
| Query node crash | Its tasks fail | Leaf tasks are retried elsewhere (QE-17); a crashed coordinator fails its queries |
| Slot owner crash (`compact`) | Intake and compaction for its slots pause; the tail grows | Lease expires and another node takes the slots; no acknowledged data is affected |
| Compaction worker crash | Job stalls | Owner reassigns the job; partial outputs are garbage-collected |
| Evaluator crash | Its state partitions pause | Lease expires; new owner resumes from the state log (CQ-12) |
| Object store unavailable | Writes are rejected with a retryable status; queries serve from cache where possible | Automatic on restoration; no acknowledged data is lost |
| Network partition between nodes | Slower queries; tail read from the object store | None needed |

- **OP-6** A node MUST be able to start, serve and stop with every other node unreachable (AR-3).

### 14.5 Multi-region

One region per deployment. Durability is the object store's. For disaster recovery, where the store replicates across regions, a deployment in the second region can open the replicated pools read-only, and read-write after failover. Active-active across regions is not planned for the first release; nothing in the storage design (independent writer logs, partitioned manifests) prevents adding it later.

### 14.6 Upgrades and format evolution

- **OP-7** Rolling upgrades are supported between adjacent minor versions. Commit objects, writer-log entries, sidecar indexes and checkpoints carry format versions; a node MUST refuse to write a format that the oldest live node cannot read.
- **OP-8** Data files written by any earlier release MUST remain readable.

### 14.7 Self-observability

Tessera writes its own logs, metrics and traces to the `_system` tenant, and ships default alert rules for its own health (WAL latency, intake and compaction backlog, evaluator lag, commit conflict rate, slot load).

### 14.8 Open-format export

- **OP-9** A background job MAY publish Apache Iceberg metadata for each dataset, so that external engines can query the Parquet files directly (G2). Exported tables expose dedicated columns as typed columns and `_overflow` as a variant column. The export covers adopted files only, not the tail.

### 14.9 Packaging and configuration

- **OP-10** Tessera is distributed as a statically linked binary and a container image, with a Helm chart. A Kubernetes operator MAY follow; it is never required.
- **OP-11** Start-up configuration is limited to what a node needs to find the cluster: the root pool, its roles, listen addresses and membership seeds. Everything else (tenants, pools, limits, policies, rules) lives in the catalog and control stores and is changed at runtime.

## 15. Security

- **SE-1** Authentication by API key, OIDC or mutual TLS. Authorization is role-based, down to dataset and, optionally, row-filter level (for example, a role restricted to `service IN (...)`).
- **SE-2** Tenants are isolated by object prefix, by per-tenant limits in every role, and optionally by per-tenant encryption keys. Writer-log segments are shared between tenants; their blocks are per tenant and are encrypted with the tenant's key where one is set.
- **SE-3** All network traffic is encrypted in transit. Data at rest relies on object-store encryption, with optional client-side encryption per tenant. Where a tenant has a key, every object under its prefix is encrypted with it, metadata included, and so are its blocks and the files of its flush records in the writer logs; object names, the cluster catalog and a dataset's Iceberg export are not. What the system records about a tenant belongs to the system tenant and is encrypted with that tenant's key.
- **SE-4** Redaction rules (drop, hash or mask by attribute path or pattern) apply at ingest, before anything is written (section 7.2).
- **SE-5** Delete-by-predicate takes effect logically at once through a tombstone in the dataset log, and physically within a bounded time (default 30 days) through compaction and garbage collection.
- **SE-6** Administrative actions and data access are recorded as events in an audit dataset.

## 16. APIs and compatibility

| Interface | Scope in first release |
|-----------|------------------------|
| OTLP gRPC and HTTP | Full: logs, metrics, traces |
| Prometheus remote write | Full |
| Prometheus HTTP API | Instant and range queries, series, labels, label values, metadata, exemplars, rules, alerts |
| Prometheus rule files | Alerting and recording rules |
| Loki HTTP API | Push; instant and range queries; labels, label values, series; index statistics and volume; detected fields; live tail |
| Loki ruler files | Alerting and recording rules in LogQL |
| Tempo HTTP API | Trace by id; search with TraceQL; tag names and values; TraceQL metrics |
| PostgreSQL wire protocol | Queries, control-table DML and administration statements; enough for `psql`, common drivers and Grafana's PostgreSQL data source |
| Arrow Flight SQL | Bulk result retrieval |
| HTTP JSON | Ingest (JSON lines), query in any language, search, administration |
| Elasticsearch bulk API | Ingest only |
| Alertmanager | Push-compatible as a notification sink |
| Tenant header | `X-Scope-OrgID` on the Prometheus, Loki and Tempo endpoints (CT-5) |

- **AP-1** Grafana's Prometheus, Loki, Tempo and PostgreSQL data sources MUST work against Tessera without a plugin, including trace-to-logs and logs-to-trace links, which resolve against the same store.
- **AP-2** The supported upstream API versions are pinned per release and tested against the corresponding Grafana releases.

Not in scope: Elasticsearch query DSL; Jaeger and Zipkin ingest (MAY be added; OTLP covers both through the OpenTelemetry Collector).

## 17. Performance and scale targets

### 17.1 Performance

Initial targets, to be validated and revised by benchmark, on current cloud hardware with same-region object storage.

| Area | Target |
|------|--------|
| Write acknowledgment (durable) | p50 ≤ 200 ms, p99 ≤ 500 ms |
| Freshness (acknowledged to queryable) | p99 ≤ 1 s |
| Ingest throughput | ≥ 20 MB/s of uncompressed OTLP per ingest core |
| Log compression | ≥ 10× against raw JSON on representative application logs |
| Metric storage | ≤ 3 bytes per sample after L2 compaction |
| Trace by id, 30 days of data, cold cache | p99 ≤ 500 ms |
| Dashboard panel, 1 h range, warm | p95 ≤ 200 ms |
| Token or line-filter search over 1 TB of compressed logs, cold | p95 ≤ 5 s |
| TraceQL attribute search, 24 h of spans, cold | p95 ≤ 3 s |
| Alert, eager rule: event received to notification sent | p99 ≤ 3 s |
| Alert, watermark rule | allowed lateness + 3 s at p99 |
| New node taking load | ≤ 30 s from process start |
| Node loss | No acknowledged data lost; dead ingester's tail queryable within 10 s |
| Tenant or limit change taking effect | ≤ 5 s on every node |

### 17.2 Scale validation envelope

G9 is a property of the design: no component has fixed capacity. What can be claimed as tested is narrower. The first release is validated to the envelope below; these are proposals for what to test, not limits.

| Dimension | Validated to | Criterion |
|-----------|--------------|-----------|
| Ingest nodes | 500 | Throughput at 500 nodes ≥ 0.8 × 50 times the throughput at 10 nodes |
| Query nodes | 500 | Throughput of a fixed query mix scales ≥ 0.8 linear |
| Total nodes | 1,000 | Membership converges; no coordination hot spot |
| Stored data | 100 TB real; metadata synthesized to 100 PB equivalent | Pruning for a 1 h query stays ≤ 1 s at p95 |
| Data files | 10⁹ in manifests | No node loads a whole manifest (MF-5) |
| Tenants | 100,000, of which 1% active | Idle tenants cost no memory or requests (CT-6) |
| Partitions | 10⁶ | Lease request rate independent of partition count (OP-3) |
| Active series | 10⁹ across the cluster | No ingest degradation (DM-7) |
| Alert rules and instances | 10⁵ rules, 10⁷ instances | Latency targets of 17.1 hold |

## 18. Rust implementation

### 18.1 Workspace layout

```
tessera/
  crates/
    tessera-types      event model, schema, ids, errors
    tessera-object     object-store abstraction, pools, conditional writes, fault injection
    tessera-commit     CommitLog, tiered checkpoints, leases, slots
    tessera-catalog    cluster catalog, tenants, control store
    tessera-wal        writer logs: segments, flush records, seal, recovery
    tessera-format     FileFormat trait, Parquet implementation, sidecar indexes
    tessera-manifest   partitions, manifest tables, intake, consistent view
    tessera-ingest     protocol decoders, normalization, memtable, flush, forwarding
    tessera-compact    compaction, clustering, dedup, sampling, patterns, trace completion, GC
    tessera-advisor    workload analysis, layout, index and partition-window selection
    tessera-ir         plan representation, types, function registry
    tessera-kernels    vectorized kernels: filter, hash, aggregate, sort, text, variant, sketches
    tessera-opt        binder, rewrite rules, cost model, pruning
    tessera-exec       pipelines, scheduler, operators, spill, memory accounting
    tessera-dist       stages, exchanges, task placement, result streaming, caches
    tessera-sql        SQL lexer, parser, lowering
    tessera-promql     PromQL lexer, parser, lowering
    tessera-logql      LogQL lexer, parser, lowering
    tessera-traceql    TraceQL lexer, parser, lowering
    tessera-search     search syntax, tokenizer
    tessera-eval       dataflow, incremental operators, watermarks, alert state machine, notifiers
    tessera-meter      usage metering, value scores
    tessera-cluster    membership, node roles, admission control
    tessera-server     gRPC, HTTP (native, Prometheus, Loki, Tempo), PostgreSQL wire, Flight SQL
    tessera            the binary
  sim/                 deterministic simulation harness
  compat/              differential test harnesses against Prometheus, Loki and Tempo
  bench/               benchmarks and load generators
```

Dependencies point downward only; `tessera-types` depends on nothing in the workspace. The language crates depend on `tessera-ir` and nothing below it.

### 18.2 Core traits

Besides `CommitLog` (section 6.5):

```rust
/// The only way any crate touches durable storage. One instance per pool.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes>;
    async fn put(&self, key: &Key, body: Bytes) -> Result<()>;
    /// Fails with `AlreadyExists` if the key is present. Basis of all coordination.
    async fn put_if_absent(&self, key: &Key, body: Bytes) -> Result<(), PutError>;
    fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta>>;
    async fn delete(&self, key: &Key) -> Result<()>;
}

pub trait FileFormat: Send + Sync {
    fn writer(&self, layout: &LayoutSpec, sink: ObjectSink) -> Box<dyn FileWriter>;
    fn open(&self, meta: &FileMeta, store: Arc<dyn ObjectStore>) -> Box<dyn FileReader>;
}

/// A push-based physical operator. A pipeline is a chain of these.
pub trait Operator: Send {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow>;
    fn finish(&mut self, out: &mut dyn Sink) -> Result<()>;
}

/// A front end: text in one language to the shared plan.
pub trait Frontend {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Plan, Diagnostic>;
}

/// An operator that can be maintained as input arrives.
pub trait IncrementalOperator: Send {
    fn on_batch(&mut self, batch: &Batch, out: &mut Emitter) -> Result<()>;
    fn on_watermark(&mut self, watermark: Timestamp, out: &mut Emitter) -> Result<()>;
    fn snapshot(&self) -> Result<Bytes>;
    fn restore(&mut self, state: Bytes) -> Result<()>;
}

pub trait LayoutAdvisor: Send + Sync {
    fn propose(&self, stats: &DatasetStats, workload: &WorkloadSummary,
               current: &LayoutSpec) -> Option<LayoutProposal>;
}
```

### 18.3 Candidate dependencies

To be confirmed by evaluation at the start of each milestone; versions and feature coverage must be checked then.

| Need | Candidate |
|------|-----------|
| Vector format, array containers, IPC | Apache Arrow (`arrow`) |
| Parquet page encoding and decoding | `parquet` |
| Object storage clients | `object_store` |
| Async runtime | `tokio` |
| gRPC, HTTP | `tonic`, `axum` |
| PostgreSQL wire protocol | `pgwire` |
| OTLP types | `opentelemetry-proto` |
| Regular expressions | `regex` |
| Deterministic simulation | `turmoil` or `madsim` |

Parsers, planner, optimizer and executor are not on this list by decision (QE-1).

### 18.4 Engineering rules

- **RS-1** `unsafe` is confined to named modules with documented invariants and is run under Miri in CI.
- **RS-2** All I/O goes through `ObjectStore` and an injected clock and random source, so that every component can run under deterministic simulation.
- **RS-3** Memory is accounted per query and per tenant through a shared pool; allocations on hot paths that bypass it are bugs.
- **RS-4** CPU-heavy work (decode, sort, encode, scan, kernels) runs on a dedicated pool, never on the async I/O runtime.
- **RS-5** No panics on any input from the network or from storage; fuzz every decoder and every parser.
- **RS-6** Dependency licenses are checked in CI against an allow-list of permissive licenses (section 20).

### 18.5 Testing strategy

| Technique | What it establishes |
|-----------|---------------------|
| Deterministic simulation with fault injection (crashes, partitions, slow and failing object-store calls, clock skew, duplicate owners) | No acknowledged write is lost; the consistent view has no gaps or duplicates (VW-1) across flush, intake, compaction, recovery and garbage collection; leases and fencing hold |
| Layout-invariance property tests | The same query returns the same result under any compaction state, layout epoch, index set and trace-completion state (AD-8) |
| Differential tests | SQL against a reference engine; PromQL against Prometheus, LogQL against Loki, TraceQL against Tempo, on identical data (QL-4) |
| Incremental-versus-batch tests | A continuous query's output equals the batch query over the same data (CQ-7, QE-20) |
| Compliance suites | Prometheus PromQL compliance; OTLP conformance |
| Fuzzing | All decoders, file readers and parsers |
| Scale tests | The envelope of section 17.2, with synthetic metadata where real data would be impractical |
| Benchmarks in CI | Section 17.1 targets tracked per commit with regression gates |

## 19. Milestones

Each milestone ends with a usable system and has an exit criterion. Two rules shape the order: the storage design is scale-ready from the first milestone, so that nothing is rebuilt when the cluster grows; and each signal arrives together with its native query language.

| # | Scope | Exit criterion |
|---|-------|----------------|
| M0 | Foundations: `ObjectStore` with fault injection and pools, simulation harness, `CommitLog`, leases and slots, cluster catalog, tenants | Simulation finds no safety violation over a long randomized run; tenants created and changed at runtime |
| M1 | Write path: OTLP logs, writer logs, registry, memtable, L1 flush, intake, recovery, consistent view | View invariant (VW-1) holds under simulation with crashes and duplicate owners |
| M2 | Engine core: plan representation, kernels, pipelines, scheduler, Parquet scan with pruning, SQL front end, PostgreSQL wire | SQL differential tests pass; M1 data, including the tail, is queryable on one node |
| M3 | Logs complete: compaction, default layouts, term and trigram indexes, search syntax, LogQL, Loki API | Grafana's Loki data source works unmodified; LogQL differential tests pass |
| M4 | Traces: spans, span events and links, `TraceGroup`, trace completion, TraceQL, Tempo API | Grafana's Tempo data source works unmodified; TraceQL differential tests pass; trace-by-id target met |
| M5 | Metrics: OTLP metrics, remote write, range-vector operators, PromQL, Prometheus API | PromQL compliance target met; existing dashboards render |
| M6 | Distributed execution and cluster: roles, membership, stages and exchanges, caches, admission control, forwarding | Scale-up, scale-down and node-loss tests pass with zero manual steps (G1); first scale-envelope run |
| M7 | Control store, continuous-query dataflow, alerting in SQL, PromQL and LogQL, notifications | Alert latency targets met; takeover produces no duplicate transitions |
| M8 | Adaptive design: workload log, advisor, column promotion, sort keys, indexes, partition windows | A dataset with an unfavorable default layout converges without intervention; `EXPLAIN LAYOUT` works |
| M9 | Cost: metering, log patterns, value scores, retention by value, automatic rollups | `system.usage` reconciles with object-store billing within a stated tolerance |
| M10 | Hardening: security, Iceberg export, packaging, upgrade testing, full scale-envelope run | Rolling upgrade under load with no errors; section 17.2 criteria met |

## 20. Licensing

### 20.1 Decision

Tessera is open source under the Apache License 2.0.

- **LI-1** Anyone may run, study, modify and redistribute Tessera free of charge and for any purpose, commercial ones included.
- **LI-2** Withdrawn. Earlier drafts barred charging for Tessera or for a product or service built on it. No open source license allows that restriction, and it was dropped with the decision to be open source.
- **LI-3** The terms are those of the Apache License 2.0: attribution, patent grant, no warranty.

### 20.2 Why the Apache License 2.0

- **It is the license of the ecosystem.** Prometheus, OpenTelemetry, Arrow, Parquet and Iceberg, whose protocols and formats Tessera speaks and whose libraries it builds on, are all under it. Code and people move between these projects without a license question.
- **It carries a patent grant.** Every contributor licenses the patents their contribution needs, and loses the license by suing over it. MIT and BSD say nothing on patents.
- **It asks little of an adopter.** Tessera is self-hosted software with no managed service planned (section 2.2), so what the project needs most is to be easy to adopt and to contribute to. A company can run, change and embed permissively licensed software without the review a copyleft or source-available license calls for.
- **What it gives up.** Anyone may sell Tessera or host it for a fee and owes nothing back but attribution. A project with a hosted business to protect would weigh that differently; this one has none.

### 20.3 Alternatives considered

| License | Why not |
|---------|---------|
| MIT, BSD | As permissive, but without the patent grant. |
| MIT or Apache 2.0, at the user's choice | The convention for Rust libraries, so that they link into anything. Tessera is a server, not a library; one license is simpler. |
| AGPL 3.0 | Open source, and it keeps network forks open. Many companies do not allow it at all, which costs the adoption the project is after. |
| Apache 2.0 with Commons Clause, Elastic License 2.0, Functional Source License | Not open source: each restricts selling or hosting. They were the candidates while LI-2 stood. |

### 20.4 Consequences

- **LI-4** The license text is the unmodified Apache License 2.0, in `LICENSE`; `NOTICE` carries the copyright line. This document is not legal advice.
- **LI-5** Dependencies MUST be under permissive licenses (Apache 2.0, MIT, BSD and similar), so that the whole can be distributed under the project's own. Copyleft dependencies are excluded, save file-level copyleft in a dependency the project does not modify (RS-6).
- **LI-6** Contributions are accepted under the project's license, as its section 5 provides, and each commit is signed off under the [Developer Certificate of Origin](https://developercertificate.org/). No contributor agreement is asked for; a later change of license would therefore need every contributor's consent.
- **LI-7** Source files carry the SPDX identifier `Apache-2.0`.

## 21. Open questions and risks

### 21.1 Decisions needed

1. **Name availability.** Other software projects already use the name "Tessera". A trademark search and a check of package names (crates.io, container registries, domain) should be done before the first public release.
2. **Engine boundary.** QE-2 reuses the Arrow memory layout and the Parquet page codecs and builds everything above them. Confirm this boundary, or state that these should be written from scratch too.
3. **Compatibility baseline.** Which Prometheus, Loki, Tempo and Grafana versions the first release pins (AP-2).

### 21.2 Technical risks

| Risk | Why it matters | Mitigation |
|------|----------------|------------|
| Scope of an own engine | A planner, optimizer, vectorized executor and distributed scheduler are a multi-year effort on their own, and correctness bugs in an engine are silent | Narrow workload (section 10); one plan representation; differential tests from M2; specialized operators added only after the general path is correct |
| Three external language semantics | PromQL, LogQL and TraceQL are defined by their implementations and keep evolving | Pinned versions (AP-2); differential harnesses in `compat/`; documented deviations (QL-4) |
| Read-path complexity | The consistent view combines manifests, flush records and writer logs; a mistake loses or duplicates rows | One invariant (VW-1), stated once and checked continuously under simulation from M1 |
| Object-store request cost at low volume | An active ingester writes about four segments per second: roughly 350,000 writes a day regardless of data volume | Tunable linger; idle writers write nothing (ST-8); `ack=buffered`; local-disk object store in single-node mode |
| Metric scans without an inverted index | Selectors over very many series may be slower than in purpose-built time-series databases | Metric sort order, encoded-data kernels (QE-10), series-at-a-time operators, automatic rollups; benchmark in M5 before committing further |
| Advisor instability | A layout that keeps changing wastes compute on rewrites | Hysteresis (AD-7), rewrite budgets (AD-10), decayed workload (AD-2), pins (AD-12) |
| Variant support in Rust Parquet | Section 6.4 depends on it | Verify in M1; fallback encoding defined (ST-13) |
| Many small tenants | Per-partition logs multiply object counts | Wider partition windows for small datasets (AD-14); shared writer logs; lazy tenant loading (CT-6) |
| Incremental and batch results diverging | Alerts would disagree with dashboards | One implementation per function (QE-20); incremental-versus-batch tests in CI |
| Unlimited scale is designed, not yet measured | Bottlenecks appear in practice that an audit misses | Scale-envelope runs at M6 and M10 (section 17.2); self-observability of slot load and commit conflicts |

## 22. Glossary

| Term | Meaning |
|------|---------|
| Adoption | The commit by which a partition takes a writer's L1 files into its manifest and advances its position for that writer |
| Commit log | Ordered sequence of commit objects created with create-if-absent |
| Consistent view | The definition of a partition's rows as manifest files plus writer-log rows beyond the adopted position |
| Dedicated column | An attribute stored as its own Parquet column in a given file |
| Fencing | Preventing a superseded process from taking effect, by making its writes fail a condition |
| Flush record | A writer-log entry stating that a range of segments is also available as L1 files |
| Intake | The background work that drives adoption |
| Layout epoch | A version of a dataset's physical design: sort key, dedicated columns, indexes |
| Manifest | The set of data files of a partition at a given version |
| Overflow column | The variant column holding attributes without a dedicated column |
| Partition | One tenant, dataset, kind and event-time window |
| Pool | An object-store bucket or prefix registered with the cluster |
| Registry | The per-dataset list of writers that may hold rows not yet adopted |
| Sidecar | The index file accompanying a data file |
| Slot | A hash bucket of single-owner work, owned through one lease |
| State log | The commit log of one continuous-query state partition |
| Tail | Rows that are durable in writer logs but not yet in a manifest |
| Trace completion | Adding tree and trace-level columns to spans once a trace can no longer grow |
| Watermark | A time before which no more data is expected |
| Writer log | The append-only log owned by one ingester process |
