#!/usr/bin/env bash
set -euo pipefail

namespace=reports
result_dir=${1:?usage: run_release_benchmark.sh RESULT_DIRECTORY}
scenario=${SCENARIO:-release-0.2.0-phased-5m}
stages=${STAGES:-60:50,60:150,60:300,60:500,60:150}
mkdir -p "$result_dir"

original_min=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.minReplicas}')
original_max=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.maxReplicas}')
observer_pid=""
database_observer_pod=release-020-db-observer

restore_cluster() {
  if [[ -n "$observer_pid" ]]; then
    kill "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  kubectl -n "$namespace" delete pod "$database_observer_pod" --ignore-not-found --wait=false >/dev/null || true
  kubectl -n "$namespace" patch hpa report-service --type merge \
    -p "{\"spec\":{\"minReplicas\":$original_min,\"maxReplicas\":$original_max}}" >/dev/null || true
  kubectl -n "$namespace" patch deployment report-service --type merge \
    -p '{"spec":{"template":{"spec":{"affinity":null}}}}' >/dev/null || true
}
trap restore_cluster EXIT INT TERM

kubectl -n "$namespace" create configmap benchmark-v2 \
  --from-file=benchmark_v2.py=benchmark_v2.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$namespace" create configmap benchmark-observer \
  --from-file=benchmark_observer.py=benchmark_observer.py --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Isolate the generator from the application so client CPU cannot inflate HPA metrics.
kubectl -n "$namespace" patch deployment report-service --type merge -p '{
  "spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"node-role.kubernetes.io/control-plane","operator":"DoesNotExist"}]}]}}}}}}
}' >/dev/null
kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null

# Reproducible cold autoscaling baseline: two pods, then reopen the HPA ceiling.
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p '{"spec":{"minReplicas":2,"maxReplicas":2}}' >/dev/null
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  available=$(kubectl -n "$namespace" get deployment report-service -o jsonpath='{.status.availableReplicas}')
  [[ "${available:-0}" == "2" ]] && break
  sleep 2
done
sleep 20

kubectl get nodes -o json >"$result_dir/nodes.json"
kubectl -n "$namespace" get deployment,hpa,pdb,service,pods -o yaml >"$result_dir/cluster-before.yaml"

kubectl -n "$namespace" patch hpa report-service --type merge \
  -p '{"spec":{"minReplicas":2,"maxReplicas":8}}' >/dev/null

.benchmark-venv/bin/python benchmark_observer.py --source kubernetes \
  --duration 660 --interval 5 --label "$scenario" >"$result_dir/kubernetes-observer.jsonl" \
  2>"$result_dir/observer.stderr" &
observer_pid=$!

database_observer_overrides=$(jq -nc --arg scenario "$scenario" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"observer",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q psycopg[binary]==3.2.10 && python /bench/benchmark_observer.py --source postgresql --duration 660 --interval 5 --label " + $scenario)],env:[{name:"DATABASE_URL",valueFrom:{secretKeyRef:{name:"report-service",key:"DATABASE_URL"}}}],resources:{requests:{cpu:"50m",memory:"64Mi"},limits:{cpu:"500m",memory:"256Mi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:"benchmark-observer"}}]}}')
kubectl -n "$namespace" delete pod "$database_observer_pod" --ignore-not-found --wait=true >/dev/null
kubectl -n "$namespace" run "$database_observer_pod" --image=python:3.12-alpine --restart=Never \
  --overrides="$database_observer_overrides" >/dev/null
kubectl -n "$namespace" wait --for=condition=Ready "pod/$database_observer_pod" --timeout=120s >/dev/null

args="--url http://report-service:8080 --scenario $scenario --stages $stages --submit-workers 128 --poll-workers 128 --drain-timeout 300 --sample-interval 5"
overrides=$(jq -nc --arg args "$args" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"benchmark",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q urllib3==2.5.0 && python /bench/benchmark_v2.py " + $args)],resources:{requests:{cpu:"500m",memory:"256Mi"},limits:{cpu:"3",memory:"1Gi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:"benchmark-v2"}}]}}')

pod_name=release-020-phased-5m
kubectl -n "$namespace" delete pod "$pod_name" --ignore-not-found --wait=true >/dev/null
kubectl -n "$namespace" run "$pod_name" --image=python:3.12-alpine --restart=Never \
  --overrides="$overrides" >/dev/null

deadline=$((SECONDS + 900))
phase=""
while (( SECONDS < deadline )); do
  phase=$(kubectl -n "$namespace" get pod "$pod_name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]] && break
  sleep 5
done

kubectl -n "$namespace" logs "$pod_name" >"$result_dir/load.jsonl" 2>"$result_dir/load.stderr" || true
kubectl -n "$namespace" get pod "$pod_name" -o json >"$result_dir/load-pod.json" || true
kubectl -n "$namespace" logs "$database_observer_pod" >"$result_dir/postgresql-observer.jsonl" \
  2>"$result_dir/postgresql-observer.stderr" || true
kubectl -n "$namespace" get pod "$database_observer_pod" -o json >"$result_dir/postgresql-observer-pod.json" || true
kubectl -n "$namespace" get deployment,hpa,pdb,service,pods -o yaml >"$result_dir/cluster-after.yaml"
kubectl top pods -n "$namespace" >"$result_dir/pod-resources-after.txt" 2>&1 || true

kill "$observer_pid" 2>/dev/null || true
wait "$observer_pid" 2>/dev/null || true
observer_pid=""

sha256sum "$result_dir"/* >"$result_dir/checksums.sha256"
if [[ "$phase" != "Succeeded" ]]; then
  echo "release benchmark finished with phase ${phase:-timeout}" >&2
  exit 1
fi
echo "$(date -Is) release benchmark complete: $result_dir"
