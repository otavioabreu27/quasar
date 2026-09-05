#!/usr/bin/env bash
set -euo pipefail

namespace=reports
result_dir=${1:?usage: run_benchmark_suite.sh RESULT_DIRECTORY}
mkdir -p "$result_dir"

original_min=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.minReplicas}')
original_max=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.maxReplicas}')
observer_pid=""

restore_cluster() {
  if [[ -n "$observer_pid" ]]; then
    kill "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  kubectl -n "$namespace" patch hpa report-service --type merge \
    -p "{\"spec\":{\"minReplicas\":$original_min,\"maxReplicas\":$original_max}}" >/dev/null || true
  kubectl -n "$namespace" patch deployment report-service --type merge \
    -p '{"spec":{"template":{"spec":{"affinity":null}}}}' >/dev/null || true
}
trap restore_cluster EXIT INT TERM

kubectl -n "$namespace" create configmap benchmark-v2 \
  --from-file=benchmark_v2.py=benchmark_v2.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Keep the load generator on the control-plane and application pods on workers.
kubectl -n "$namespace" patch deployment report-service --type merge -p '{
  "spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"node-role.kubernetes.io/control-plane","operator":"DoesNotExist"}]}]}}}}}}
}' >/dev/null
kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null

db_url=$(kubectl -n "$namespace" get secret report-service -o jsonpath='{.data.DATABASE_URL}' | base64 -d)
db_url=${db_url/192.168.15.210:5432/127.0.0.1:5433}
DATABASE_URL="$db_url" .benchmark-venv/bin/python benchmark_observer.py \
  --duration 3600 --interval 5 --label reliable-benchmark >"$result_dir/observer.jsonl" 2>"$result_dir/observer.stderr" &
observer_pid=$!

kubectl get nodes -o json >"$result_dir/nodes.json"
kubectl -n "$namespace" get deployment,hpa,pdb,service -o yaml >"$result_dir/cluster-config-before.yaml"

run_job() {
  local name=$1
  shift
  local args="$*"
  local overrides
  overrides=$(jq -nc --arg args "$args" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"benchmark",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q urllib3==2.5.0 && python /bench/benchmark_v2.py " + $args)],resources:{requests:{cpu:"500m",memory:"256Mi"},limits:{cpu:"3",memory:"1Gi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:"benchmark-v2"}}]}}')
  kubectl -n "$namespace" delete pod "$name" --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" run "$name" --image=python:3.12-alpine --restart=Never --overrides="$overrides" >/dev/null
  local deadline=$((SECONDS + 2700))
  local phase=""
  while (( SECONDS < deadline )); do
    phase=$(kubectl -n "$namespace" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    if [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]]; then
      break
    fi
    sleep 2
  done
  kubectl -n "$namespace" logs "$name" >"$result_dir/$name.jsonl" 2>"$result_dir/$name.stderr" || true
  kubectl -n "$namespace" get pod "$name" -o json >"$result_dir/$name-pod.json" || true
  kubectl -n "$namespace" delete pod "$name" --wait=false >/dev/null || true
  if [[ "$phase" != "Succeeded" ]]; then
    echo "$name finished with phase ${phase:-timeout}" >&2
  fi
}

wait_for_replicas() {
  local expected=$1
  local deadline=$((SECONDS + 180))
  local available="0"
  while (( SECONDS < deadline )); do
    available=$(kubectl -n "$namespace" get deployment report-service \
      -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)
    if [[ "${available:-0}" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $expected available replicas (got ${available:-0})" >&2
  return 1
}

for replicas in 1 2 4 8; do
  kubectl -n "$namespace" patch hpa report-service --type merge \
    -p "{\"spec\":{\"minReplicas\":$replicas,\"maxReplicas\":$replicas}}" >/dev/null
  kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null
  wait_for_replicas "$replicas"
  run_job "warmup-r$replicas" --scenario "warmup-r$replicas" --duration 15 --rate 50 \
    --submit-workers 32 --poll-workers 32 --drain-timeout 120
  for rate in 50 150 300 500; do
    for repetition in 1 2 3; do
      name="fixed-r${replicas}-t${rate}-n${repetition}"
      echo "$(date -Is) starting $name"
      run_job "$name" --scenario "$name" --duration 20 --rate "$rate" \
        --submit-workers 96 --poll-workers 96 --drain-timeout 240
      sleep 3
    done
  done
done

# Reset to two pods, then restore elasticity for a 30-minute ramp/hold/recovery run.
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p '{"spec":{"minReplicas":2,"maxReplicas":2}}' >/dev/null
kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null
wait_for_replicas 2
sleep 30
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p "{\"spec\":{\"minReplicas\":$original_min,\"maxReplicas\":$original_max}}" >/dev/null

echo "$(date -Is) starting elastic-30m"
run_job elastic-30m --scenario elastic-30m \
  --stages 300:50,300:100,600:200,300:300,300:100 \
  --submit-workers 128 --poll-workers 128 --drain-timeout 600 --sample-interval 10

kubectl -n "$namespace" get deployment,hpa,pods -o yaml >"$result_dir/cluster-config-after.yaml"
echo "$(date -Is) suite complete"
