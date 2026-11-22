#!/usr/bin/env bash
# The image and the chart on a cluster (OP-10): builds the image, starts
# a kind cluster, installs MinIO and the chart with the bucket as the
# root pool, then checks, from a pod inside the cluster, that
#
#   1. rows pushed through the service are read back through it,
#   2. the nodes read the bucket through their cache (section 10.8),
#   3. no acknowledged write is lost when every node is replaced, one
#      at a time, while writes keep arriving (OP-4, section 14.6),
#   4. the rows are still there once every pod is a new one, since
#      nothing durable lives on a node (G2), and
#   5. the chart's Grafana has its dashboards and its three data
#      sources answer: the nodes' metrics, their logs and the usage.
#
# The nodes present a cluster secret to each other (SE-3).
#
# Needs docker, kind, kubectl and helm. The cluster gets a kubeconfig
# of its own, so the caller's is never touched, and is deleted at the
# end unless KEEP=1.
#
#   deploy/kind/smoke.sh            build, run, clean up
#   IMAGE=tessera:dev SKIP_BUILD=1 deploy/kind/smoke.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=${CLUSTER:-tessera-smoke}
IMAGE=${IMAGE:-tessera:smoke}
CLIENT_IMAGE=curlimages/curl:8.11.1
API=http://tessera-tessera:8080
GRAFANA=http://tessera-tessera-grafana:3000
export KUBECONFIG=${SMOKE_KUBECONFIG:-$(mktemp)}

say() { printf '\n== %s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
cleanup() {
    if [ "${KEEP:-0}" != 1 ]; then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# Runs a shell command in the client pod.
client() { kubectl exec client -- sh -c "$1"; }

# How many log rows the dataset holds, as the service answers.
count() {
    client "curl -sf -m 30 -X POST $API/api/v1/query -H 'content-type: application/json' \
        -d '{\"dataset\":\"prod\",\"query\":\"SELECT count(*) FROM logs\"}'" |
        sed -E 's/.*"rows":\[\[([0-9]+)(\.0)?\]\].*/\1/'
}

# Has Grafana run one query, the JSON fields in $3, against its data
# source of type $1 and uid $2, and prints what it answers.
grafana_query() {
    kubectl exec -i client -- curl -s -m 30 -X POST "$GRAFANA/api/ds/query" \
        -H 'content-type: application/json' -d @- <<JSON
{"from": "now-15m", "to": "now", "queries": [{"refId": "A", "intervalMs": 60000, "maxDataPoints": 100,
  "datasource": {"type": "$1", "uid": "$2"}, $3}]}
JSON
}

# Pushes log lines numbered $1 to $2 in one request.
push() {
    local values="" second
    second=$(date +%s)
    for i in $(seq "$1" "$2"); do
        values="${values}[\"$second$(printf '%09d' "$i")\",\"line $i\"],"
    done
    client "curl -s -o /dev/null -w '%{http_code}' -m 30 -X POST $API/loki/api/v1/push \
        -H 'content-type: application/json' -H 'x-tessera-dataset: prod' \
        -d '{\"streams\":[{\"stream\":{\"app\":\"smoke\"},\"values\":[${values%,}]}]}'"
}

if [ "${SKIP_BUILD:-0}" != 1 ]; then
    say "building $IMAGE"
    docker build -q -t "$IMAGE" .
fi

say "starting the cluster $CLUSTER"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" ||
    kind create cluster --name "$CLUSTER" --wait 120s
kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG"
kind load docker-image --name "$CLUSTER" "$IMAGE"

say "installing MinIO and the chart"
kubectl apply -f deploy/kind/minio.yaml
kubectl get secret tessera-cluster >/dev/null 2>&1 ||
    kubectl create secret generic tessera-cluster --from-literal=secret="smoke-$RANDOM$RANDOM"
kubectl rollout status deployment/minio --timeout=180s
helm upgrade --install tessera deploy/helm/tessera --wait --timeout 420s \
    --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" \
    --set allowAnonymous=true \
    --set grafana.enabled=true \
    --set clusterSecret=tessera-cluster \
    --set rootPool.url=s3://tessera/smoke \
    --set rootPool.credentialsSecret=tessera-bucket \
    --set rootPool.env.AWS_REGION=us-east-1 \
    --set rootPool.env.AWS_ENDPOINT_URL=http://minio:9000 \
    --set-string rootPool.env.AWS_ALLOW_HTTP=true
kubectl get pod client >/dev/null 2>&1 ||
    kubectl run client --image="$CLIENT_IMAGE" --restart=Never --command -- sleep 86400
kubectl wait --for=condition=Ready pod/client --timeout=120s

say "rows pushed through the service are read back through it"
before=$(count)
code=$(push 1 100)
[ "$code" = 204 ] || fail "the push was answered $code"
held=$(count)
[ "$held" = $((before + 100)) ] || fail "pushed 100 rows onto $before, the service counts $held"
echo "ok: $held rows"

say "the nodes read the bucket through their cache"
cached=0
for _ in 1 2 3 4 5 6; do
    count >/dev/null
    reads=$(client "curl -sf -m 10 $API/metrics" |
        awk '/^tessera_object_cache_(hits|misses)_total/ { n += $2 } END { print n + 0 }')
    [ "$reads" -gt "$cached" ] && cached=$reads
done
[ "$cached" -gt 0 ] || fail "no node read anything through its cache"
echo "ok: $cached reads through a cache"

say "no acknowledged write is lost while every node is replaced"
client 'rm -f /tmp/stop /tmp/sent /tmp/acked'
# The loop runs in the pod's shell: only the service's address is put
# in from here.
# shellcheck disable=SC2016
kubectl exec client -- sh -c '
    i=1000; sent=0; acked=0
    while [ ! -f /tmp/stop ]; do
        i=$((i + 1)); sent=$((sent + 1))
        code=$(curl -s -o /dev/null -w "%{http_code}" -m 10 -X POST '"$API"'/loki/api/v1/push \
            -H "content-type: application/json" -H "x-tessera-dataset: prod" \
            -d "{\"streams\":[{\"stream\":{\"app\":\"smoke\"},\"values\":[[\"$(date +%s)$(printf "%09d" $i)\",\"line $i\"]]}]}")
        [ "$code" = 204 ] && acked=$((acked + 1))
        sleep 0.05
    done
    echo $sent > /tmp/sent; echo $acked > /tmp/acked' &
writer=$!
sleep 5
kubectl rollout restart deployment/tessera-tessera-front deployment/tessera-tessera-back
kubectl rollout status deployment/tessera-tessera-front --timeout=300s
kubectl rollout status deployment/tessera-tessera-back --timeout=300s
sleep 5
client 'touch /tmp/stop'
wait "$writer"
sent=$(client 'cat /tmp/sent')
acked=$(client 'cat /tmp/acked')
after=$(count)
echo "sent $sent, acknowledged $acked, the service counts $((after - held)) more rows"
[ "$acked" -gt 0 ] || fail "no write was acknowledged during the restart"
[ "$after" -ge $((held + acked)) ] || fail "acknowledged rows are missing"
[ "$after" -le $((held + sent)) ] || fail "more rows than were ever sent"

say "the rows outlive the pods that took them"
kubectl delete pod -l app.kubernetes.io/name=tessera --wait=true >/dev/null
kubectl rollout status deployment/tessera-tessera-front --timeout=300s
kubectl rollout status deployment/tessera-tessera-back --timeout=300s
again=$(count)
[ "$again" = "$after" ] || fail "$after rows before the pods were deleted, $again after"
echo "ok: $again rows"

say "the chart's Grafana shows the cluster"
kubectl rollout status deployment/tessera-tessera-grafana --timeout=300s
boards=$(client "curl -sf -m 30 '$GRAFANA/api/search?tag=tessera'")
for uid in tessera-overview tessera-tenants tessera-costs; do
    case "$boards" in *"\"uid\":\"$uid\""*) ;; *) fail "the dashboard $uid is not provisioned: $boards" ;; esac
done
# Each data source answers with a value: the nodes' metrics, their
# logs and the usage they record. Grafana's plugins take a moment to
# start, so each is asked again for a while.
answers() {
    local answer=""
    for _ in $(seq 1 30); do
        answer=$(grafana_query "$@")
        case "$answer" in
            *'"error"'*) ;;
            *'"values":[['[0-9\"]*) return 0 ;;
        esac
        sleep 3
    done
    printf '%s\n' "$answer" >&2
    return 1
}
answers prometheus tessera-metrics \
    '"expr": "count(count by (node) (tessera_ingest_queue_depth))", "instant": true, "range": false' ||
    fail "the metrics data source does not answer"
answers loki tessera-logs \
    '"expr": "sum(count_over_time({level=~\".+\"}[15m]))", "queryType": "instant"' ||
    fail "the logs data source does not answer"
answers grafana-postgresql-datasource tessera-usage \
    '"rawSql": "SELECT count(*) AS value FROM system.usage WHERE phase = '"'"'ingest'"'"'", "format": "table", "rawQuery": true' ||
    fail "the usage data source does not answer"
echo "ok: three dashboards, three data sources"

say "passed"
