#!/usr/bin/env bash
set -euo pipefail

namespace=reports
result_dir=${1:?usage: run_elastic_benchmark.sh RESULT_DIRECTORY}
mkdir -p "$result_dir"
observer_pid=""

restore_cluster() {
  if [[ -n "$observer_pid" ]]; then
    kill "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  kubectl -n "$namespace" patch hpa report-service --type merge \
    -p '{"spec":{"minReplicas":2,"maxReplicas":8}}' >/dev/null || true
  kubectl -n "$namespace" patch deployment report-service --type merge \
    -p '{"spec":{"template":{"spec":{"affinity":null}}}}' >/dev/null || true
}
trap restore_cluster EXIT INT TERM

kubectl -n "$namespace" create configmap benchmark-v2 \
  --from-file=benchmark_v2.py=benchmark_v2.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$namespace" patch deployment report-service --type merge -p '{
  "spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"node-role.kubernetes.io/control-plane","operator":"DoesNotExist"}]}]}}}}}}
}' >/dev/null
kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null

# Establish a reproducible cold autoscaling baseline.
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p '{"spec":{"minReplicas":2,"maxReplicas":2}}' >/dev/null
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  available=$(kubectl -n "$namespace" get deployment report-service -o jsonpath='{.status.availableReplicas}')
  [[ "${available:-0}" == "2" ]] && break
  sleep 2
done
sleep 30
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p '{"spec":{"minReplicas":2,"maxReplicas":8}}' >/dev/null

db_url=$(kubectl -n "$namespace" get secret report-service -o jsonpath='{.data.DATABASE_URL}' | base64 -d)
db_url=${db_url/192.168.15.210:5432/127.0.0.1:5433}
DATABASE_URL="$db_url" .benchmark-venv/bin/python benchmark_observer.py \
  --duration 2400 --interval 5 --label elastic-30m-rerun >"$result_dir/observer-elastic.jsonl" \
  2>"$result_dir/observer-elastic.stderr" &
observer_pid=$!

args='--scenario elastic-30m-rerun --stages 300:50,300:100,600:200,300:300,300:100 --submit-workers 128 --poll-workers 128 --drain-timeout 600 --sample-interval 10'
overrides=$(jq -nc --arg args "$args" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"benchmark",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q urllib3==2.5.0 && python /bench/benchmark_v2.py " + $args)],resources:{requests:{cpu:"500m",memory:"256Mi"},limits:{cpu:"3",memory:"1Gi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:"benchmark-v2"}}]}}')
kubectl -n "$namespace" delete pod elastic-30m-rerun --ignore-not-found --wait=true >/dev/null
kubectl -n "$namespace" run elastic-30m-rerun --image=python:3.12-alpine --restart=Never \
  --overrides="$overrides" >/dev/null

deadline=$((SECONDS + 2400))
phase=""
while (( SECONDS < deadline )); do
  phase=$(kubectl -n "$namespace" get pod elastic-30m-rerun -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]] && break
  sleep 5
done
kubectl -n "$namespace" logs elastic-30m-rerun >"$result_dir/elastic-30m-rerun.jsonl" \
  2>"$result_dir/elastic-30m-rerun.stderr" || true
kubectl -n "$namespace" get pod elastic-30m-rerun -o json >"$result_dir/elastic-30m-rerun-pod.json" || true
if [[ "$phase" != "Succeeded" ]]; then
  echo "elastic benchmark finished with phase ${phase:-timeout}" >&2
  exit 1
fi
echo "$(date -Is) elastic benchmark complete"
