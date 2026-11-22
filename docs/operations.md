# Operating Tessera

What someone running Tessera needs to know: what a deployment is made
of, how to install, secure, observe, scale and upgrade it, and what
each failure looks like. The design is in
[`tessera-spec.md`](../tessera-spec.md); the gaps the implementation
still has are listed per milestone in the [README](../README.md).

## A deployment

One binary, `tessera`, runs every role: `ingest`, `query`, `compact`
and `eval`. A node is a process started with a list of roles, and a
cluster is the set of nodes that share one **root pool**: a bucket in
an S3-compatible store, or a prefix in one. Everything durable lives
there. A node keeps nothing that cannot be lost: its local disk holds
spill files of running queries and, when asked, a cache of object
reads.

Nodes need no address list. Each writes a record under
`_cluster/nodes/` in the root pool at every heartbeat and reads the
others', so starting a node adds it and stopping one removes it.

## The object store

The store must provide (spec ST-3):

- **create-if-absent writes**: a `PUT` with `If-None-Match: *` that
  is refused when the key exists. Amazon S3, MinIO, Ceph RGW and
  Cloudflare R2 have it. Every commit, lease and log entry rests on
  it. A store that ignores the condition would answer every such
  write with success and let two nodes overwrite each other, so a
  node checks for it when it starts, with an object it creates twice
  under `_cluster/probes/`, and refuses to run on a store that lets
  the second creation through.
- **read-after-write consistency and consistent listing in key
  order.** S3 general purpose buckets list in key order; directory
  buckets (S3 Express One Zone) do not and are not supported.

The client reads its configuration from the environment, as the AWS
tools do:

| Variable | Meaning |
|----------|---------|
| `AWS_REGION` | The bucket's region; any value for a store that has none |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Static credentials |
| `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN` | A web identity, such as an EKS service account |
| `AWS_ENDPOINT_URL` | The endpoint of a store other than Amazon's |
| `AWS_ALLOW_HTTP` | `true` for an endpoint that speaks plain HTTP |

Without static credentials or a web identity the client asks the
container's or the instance's metadata service.

The client retries a request the store answers with a server error,
or does not answer, up to four times within thirty seconds. After
that a write is refused with a retryable status (503) and a query
fails; nothing acknowledged is lost.

For durability and disaster recovery, turn on what the store offers:
versioning and replication of the bucket. A second deployment can
open a replicated bucket once the first has stopped writing to it.

## Installing

### On Kubernetes

The chart in `deploy/helm/tessera` runs groups of nodes by role, by
default two `ingest,query` nodes behind the service and one
`compact,eval` node. It needs Kubernetes 1.30 or newer.

```
kubectl create secret generic tessera-bucket \
    --from-literal=AWS_ACCESS_KEY_ID=... --from-literal=AWS_SECRET_ACCESS_KEY=...
helm install tessera deploy/helm/tessera \
    --set rootPool.url=s3://my-bucket/tessera \
    --set rootPool.env.AWS_REGION=eu-west-1 \
    --set rootPool.credentialsSecret=tessera-bucket
```

Where the cluster hands credentials to a service account, set
`serviceAccountName` and leave `rootPool.credentialsSecret` empty.
For a store other than Amazon's add
`rootPool.env.AWS_ENDPOINT_URL`.

Each pod keeps a cache of what it read of the bucket in an
`emptyDir`, up to `cache.bytes` (8 GiB by default; zero turns it
off): give the nodes that much ephemeral storage. Where the pods'
network is shared with anything else, name in `clusterSecret` a
secret whose entry `secret` the nodes present to each other:

```
kubectl create secret generic tessera-cluster \
    --from-literal=secret="$(openssl rand -hex 32)"
```

The first install has no API key to refuse requests without one, so
install with `allowAnonymous=true`, issue a key (below), and upgrade
with `allowAnonymous=false`. Keep the service private meanwhile.

`deploy/kind/smoke.sh` runs the chart on a kind cluster against MinIO
and is the quickest way to see a working deployment.

### As a process

```
AWS_REGION=eu-west-1 tessera run \
    --root-pool s3://my-bucket/tessera \
    --roles ingest,query \
    --listen 0.0.0.0:8080 --pg-listen 0.0.0.0:5432 \
    --advertise http://node-1.internal:8080 \
    --cache-dir /var/cache/tessera \
    --no-anonymous
```

`--advertise` is the address the other nodes reach this one at; it is
needed whenever `--listen` is not such an address. `tessera run
--help` lists every option. A single node for a laptop needs none of
them: `tessera run` uses the directory `./tessera-data` as its root
pool and listens on the loopback interface.

A node stops in order at SIGTERM or SIGINT: it stops accepting
requests, closes its writer log, and withdraws from the registry.
One that is killed instead loses nothing acknowledged; its log is
closed by another node's recovery.

### Sizing

- `ingest` and `query` nodes are stateless and scale with traffic.
  A write is acknowledged once its segment is in the object store, so
  acknowledgement latency is the linger time (`--linger-ms`, 50 by
  default) plus one `PUT`.
- `compact` nodes own intake, compaction and garbage collection by
  slot; `eval` nodes own rule groups by slot. Slots move by lease
  when nodes come and go.
- Give query nodes a `--cache-dir` on fast local disk
  (`--cache-bytes`, 8 GiB by default). It holds what was read of data
  files, their indexes, writer-log entries and manifest checkpoints,
  and is emptied at every start.
- Queries spill to `--spill-dir` (the system's temporary directory by
  default) when they outgrow their memory grant.

## Securing

- **Authentication.** With `--no-anonymous` every request needs an
  API key, an OIDC token (`--oidc-issuer`, `--oidc-audience`) or a
  client certificate (`--tls-client-ca`). Without the flag a request
  with no credential is a cluster administrator: only for a node
  nobody else can reach.
- **The first key.** While anonymous access is on:

  ```
  curl -X POST http://tessera:8080/api/v1/tenants/_system/keys \
       -H 'content-type: application/json' -d '{"name":"ops","roles":["admin"]}'
  ```

  The answer carries the key once. A key of the `_system` tenant
  with the `admin` role administers the cluster; a tenant's own keys
  act within it, with the roles `admin`, `writer`, `reader` or those
  the tenant defines in `system.roles`.
- **Encryption in transit.** `--tls-cert` and `--tls-key` serve TLS
  on both listeners; `--tls-ca` is the CA the nodes check each other
  against. `--cluster-secret` (or `TESSERA_CLUSTER_SECRET`), the same
  on every node, closes the routes between nodes to everything else;
  without it they are open to whoever reaches the port.
- **Encryption at rest.** Use the store's own. On top of it,
  `--keyring` names a file of `name = <64 hex digits>` lines, and a
  tenant whose `encryption_key` setting names one has everything
  under its prefix sealed under it: data files, indexes, manifests,
  the dataset and control logs, the state of its rules, and its
  blocks and flush records in the writer logs. A tenant created with
  a key is sealed from its first row: a node that has not heard of
  the tenant yet refuses its rows, and the node that knows it writes
  them. A key set on a tenant that already exists reaches each node
  within a second, and what was written before stays as it was until
  compaction rewrites it.
  With the chart, `keyringSecret` names the secret whose entry
  `keyring` is that file, mounted read-only into every node.
  Not sealed, since they place the data and do not hold it: object
  names (tenant ids, dataset and rule group names), the catalog, the
  headers of writer-log entries and the Iceberg export. What the
  system records about every tenant, the bounds queries asked for,
  usage by service and the audit trail, is the `_system` tenant's:
  give that tenant an `encryption_key` too to seal it. Keep every key
  a tenant ever used in the keyring for as long as objects sealed
  under it remain.
- **Audit.** Administrative actions and control statements are rows
  of the `_system` tenant's `audit` dataset.

## Tenants and settings

Tenants, their limits and policies are not start-up configuration:
they live in the catalog and change while the cluster runs.

```
curl -X POST  http://tessera:8080/api/v1/tenants \
     -H 'content-type: application/json' -d '{"name":"acme"}'
curl -X PATCH http://tessera:8080/api/v1/tenants/acme \
     -H 'content-type: application/json' \
     -d '{"retention_seconds":2592000,"query_concurrency":8}'
```

| Setting | Effect |
|---------|--------|
| `ingest_rate_bytes_per_second` | Sustained ingest rate; beyond it writes get 429 |
| `retention_seconds` | How long data is kept unless a retention policy says otherwise |
| `query_concurrency` | Queries running at once; others queue, then get 503 |
| `query_max_scan_bytes`, `query_max_memory_bytes`, `query_max_seconds` | Limits of one query |
| `ingest_budget`, `query_budget`, `team_ingest_budgets`, `team_query_budgets` | Daily budgets and what happens past them |
| `encryption_key` | The keyring key the tenant's data is sealed under |
| `redactions` | What is dropped, hashed or masked at ingest |
| `rule_partitions` | State partitions per rule group |
| `features` | Switches `patterns`, `promotion`, `advisor`, `rollups` off |

A request body may hold 32 MiB once decompressed; gzip is accepted on
every route and over gRPC.

## Observing

- `GET /healthz` answers `ok` while the process serves.
- `GET /metrics` exposes, in the Prometheus text format, what an
  autoscaler and an operator need:

  | Metric | Watch for |
  |--------|-----------|
  | `tessera_ingest_queue_depth` | Writes in flight; scale `ingest` on it |
  | `tessera_write_ack_seconds` | Above the linger time plus a `PUT`: the store is slow |
  | `tessera_query_queue_seconds` | Queries waiting for a turn; scale `query` on it |
  | `tessera_intake_backlog` | Writers whose logs had to be recovered |
  | `tessera_compaction_backlog` | Jobs deferred by the budget; scale `compact` on it |
  | `tessera_evaluator_lag_seconds` | Rule groups behind; scale `eval` on it |
  | `tessera_rules_degraded` | Rules rebuilt from files after falling behind the logs |
  | `tessera_alert_latency_seconds` | From the last row to the notification sent |
  | `tessera_slots_owned{kind}` | Slots this node holds |
  | `tessera_object_cache_hits_total`, `_misses_total`, `_bytes` | The use of `--cache-dir` |
  | `tessera_object_store_requests_total{operation}` | What the node asks of its store, by operation; its rate while idle is what the store bills for nothing |

- Every node writes the same signals to the `_system` tenant's
  `metrics` dataset and its own log events to `logs`, so they can be
  queried and graphed with the node's own APIs:

  ```
  curl -G http://tessera:8080/api/v1/query -H 'X-Scope-OrgID: _system' \
       -H 'x-tessera-dataset: metrics' --data-urlencode 'query=tessera_write_ack_seconds'
  ```

- The `_system` tenant ships with alert rules on those metrics:
  write acknowledgement above 5 s, commit conflicts growing, intake
  or compaction backlog, evaluator lag above 60 s, degraded rules,
  alert latency above 3 s, query queueing above 2 s and slot load.
  They notify nobody until the `_system` tenant has a route; add one
  in `system.notification_routes`.
- `TESSERA_LOG` sets the log filter, `info` by default
  (`TESSERA_LOG=tessera_exec=debug,info`).

## Scaling and upgrading

- **Scaling** is changing a group's `replicas`. Slots rebalance by
  themselves within a few lease renewals (thirty-second leases,
  renewed every ten).
- **Rolling upgrade.** `helm upgrade` replaces nodes one at a time.
  A pod keeps serving for `drainSeconds` after it is asked to leave,
  so no request is refused, and an acknowledged write is never lost:
  the smoke test replaces every node under writes and counts the
  rows. Upgrade between adjacent versions only. A node refuses to
  start beside a live one that cannot read the data format it
  writes; upgrade that one first.
- **Releases.** A tag `v<version>` on the repository publishes the
  image `ghcr.io/thewillyhuman/tessera:<version>`, which the chart
  uses by default, and the chart itself to
  `oci://ghcr.io/thewillyhuman/charts`. The image is the one CI built
  from the tagged commit and passed the smoke test on, so the tag
  goes on a commit of main whose CI passed, or is pushed with it and
  waits. Every such commit has its image before any release, as
  `ghcr.io/thewillyhuman/tessera:sha-<commit>`: run one with
  `--set image.tag=sha-<commit>`.
- **Rolling back** to the previous version is safe while no node of
  the new version has written a newer format; the refusal above is
  what tells.

## When something fails

| What | What you see | What to do |
|------|--------------|------------|
| An `ingest` node dies | Its in-flight writes fail and clients retry them | Nothing: recovery closes its log and its rows stay readable meanwhile |
| A `query` node dies | The queries it coordinated fail; others take over its share of scans | Nothing |
| A `compact` or `eval` node dies | Intake, compaction or its rule groups pause for up to a lease (30 s) | Nothing: another node takes the slots |
| The object store is down | Writes get 503, queries fail or are served from the cache | Nothing once it is back; nothing acknowledged is lost |
| The store throttles (503 Slow Down) | `tessera_write_ack_seconds` grows | Spread the load over more pools or prefixes; raise the store's limits |
| A node cannot reach the others | Slower queries: it reads what its peers would have | Fix the network; correctness does not depend on it |
| A node's disk fills | Spilling queries fail; the cache stops keeping reads | Free or enlarge the disk; nothing durable is there |
| A task inside a node dies | The node logs it, stops in order and exits with an error | Nothing: the supervisor starts it again; report the log |
| A tenant names a key the nodes lack | Its writes are refused rather than stored plain | Add the key to every node's keyring |

## Requests against the store

A bucket bills per request, and `tessera_object_store_requests_total`
counts what each node asks, by operation. A node left idle makes
about nine a second, measured against MinIO on one node with every
role, a cache directory, five datasets and seven partitions: half
are reads of log heads that find nothing new, a quarter listings,
and the rest heartbeats, leases and the node's own metrics. The
background work goes over every dataset, writer and partition once
in six rounds, half a minute at the default interval, and what that
round asks grows with what the cluster holds; the rounds between
follow the logs of the writers it found, a read each, and go over a
dataset only when a flush reached it. A new dataset, a writer that
has just registered and a layout the advisor adopted are therefore
acted on within half a minute, not within five seconds; rows are
readable all the while. An `eval` node reads the rules of a tenant
that has any every round, and looks one round in six whether a
tenant that had none has one now: a tenant's first rule or rollup
starts within half a minute, a change to its rules within a round. Without `--cache-dir` the node's own health
rules add the writer-log entries they read. Budget for it where
requests are billed.

A query's requests grow with the partitions of its dataset, one a day
by default, not with the files in them: it asks for the partitions'
names, reads the head of each one's manifest, sixteen at a time, and
then only the files and ranges its time range and filters leave.
What it read of files, indexes, writer-log entries and manifest
checkpoints the cache keeps.

## Known limits

Listed with their context in the README's status section. The ones an
operator meets first:

- one region per deployment;
- only the root pool is opened: further pools in the catalog are not
  used yet;
- the cache keeps whole ranges as they were read, so it helps
  repeated queries more than new ones;
- one round in six of the background work goes over every dataset
  and partition whether or not anything changed, so the requests of
  an idle cluster still grow with what it holds (see above).
