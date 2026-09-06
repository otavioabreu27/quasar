#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

namespace=reports
result_dir=${1:?usage: run_release_benchmark.sh RESULT_DIRECTORY}
scenario=${SCENARIO:-candidate-phased-5m}
stages=${STAGES:-60:50,60:150,60:300,60:500,60:150}
batch_size=${BATCH_SIZE:-1}
baseline_replicas=${REPLICAS:-2}
max_replicas=${MAX_REPLICAS:-8}
require_runtime_metrics=${REQUIRE_RUNTIME_METRICS:-1}
generators=${GENERATORS:-1}
[[ "$generators" == 1 || "$generators" == 2 ]] || exit 1
expected_image=${EXPECTED_IMAGE:?Set EXPECTED_IMAGE to the already-deployed image@sha256:digest}
[[ "$scenario" =~ ^[a-zA-Z0-9._-]+$ && "$stages" =~ ^[0-9:.,]+$ && "$batch_size" =~ ^[0-9]+$ ]] || exit 1
[[ "$baseline_replicas" =~ ^[1-9][0-9]*$ && "$max_replicas" =~ ^[1-9][0-9]*$ ]] || exit 1
(( baseline_replicas <= max_replicas && batch_size >= 1 && batch_size <= 1000 )) || exit 1
[[ "$require_runtime_metrics" == 0 || "$require_runtime_metrics" == 1 ]] || exit 1
# This runner's observer deadlines are deliberately sized for five-minute load.
.benchmark-venv/bin/python -c 'import sys; stages=[tuple(map(float,s.split(":"))) for s in sys.argv[1].split(",")]; assert all(len(s)==2 and min(s)>0 for s in stages); assert sum(s[0] for s in stages)==300' "$stages"
[[ ! -e "$result_dir" ]] || { echo "Result directory already exists" >&2; exit 1; }
mkdir -p "$result_dir"
actual_image=$(kubectl -n "$namespace" get deployment report-service -o jsonpath='{.spec.template.spec.containers[0].image}')
[[ "$expected_image" == *@sha256:* && "$actual_image" == "$expected_image" ]] || { echo "Deploy the expected immutable image first" >&2; exit 1; }
kubectl -n "$namespace" get deployment report-service -o json >"$result_dir/deployment-original.json"
kubectl -n "$namespace" get hpa report-service -o json >"$result_dir/hpa-original.json"
jq -n --arg image "$expected_image" --arg scenario "$scenario" --arg stages "$stages" --argjson batch "$batch_size" --argjson runtime "$require_runtime_metrics" --argjson generators "$generators" '{image:$image,scenario:$scenario,stages:$stages,batch_size:$batch,require_runtime_metrics:$runtime,generators:$generators}' >"$result_dir/experiment.json"
sha256sum benchmark_v2.py benchmark_parallel.py benchmark_observer.py run_release_benchmark.sh >"$result_dir/scripts.sha256"
affinity_restore=$(jq -c 'if .spec.template.spec | has("affinity") then [{op:"add",path:"/spec/template/spec/affinity",value:.spec.template.spec.affinity}] else [{op:"remove",path:"/spec/template/spec/affinity"}] end' "$result_dir/deployment-original.json")
run_id="quasar-bench-$(date +%s)-$$"
load_config="$run_id-load-script"
observer_config="$run_id-observer-script"
pod_name="$run_id-load"

original_min=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.minReplicas}')
original_max=$(kubectl -n "$namespace" get hpa report-service -o jsonpath='{.spec.maxReplicas}')
observer_pid=""
database_observer_pod="$run_id-db"
settings_changed=0
evidence_saved=0

save_evidence() {
  kubectl -n "$namespace" logs "$pod_name" 2>"$result_dir/load.stderr" | jq -Rc 'fromjson? | select(type == "object")' >"$result_dir/load.jsonl" || true
  kubectl -n "$namespace" logs "$database_observer_pod" 2>"$result_dir/postgresql-observer.stderr" | jq -Rc 'fromjson? | select(type == "object")' >"$result_dir/postgresql-observer.jsonl" || true
  kubectl -n "$namespace" get pod "$pod_name" -o json >"$result_dir/load-pod.json" || true
  kubectl -n "$namespace" get pod "$database_observer_pod" -o json >"$result_dir/postgresql-observer-pod.json" || true
  kubectl -n "$namespace" get deployment,hpa,pdb,service,pods -o yaml >"$result_dir/cluster-after.yaml" || true
  evidence_saved=1
}

restore_cluster() {
  local exit_status=$?
  trap - EXIT INT TERM
  [[ "$evidence_saved" == 1 ]] || save_evidence
  if [[ -n "$observer_pid" ]]; then
    kill "$observer_pid" 2>/dev/null || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  if [[ -f "$result_dir/kubernetes-observer.jsonl" ]]; then
    mv "$result_dir/kubernetes-observer.jsonl" "$result_dir/kubernetes-observer.raw.jsonl"
    jq -Rc 'fromjson? | select(type == "object")' "$result_dir/kubernetes-observer.raw.jsonl" >"$result_dir/kubernetes-observer.jsonl"
  fi
  kubectl -n "$namespace" delete pod "$database_observer_pod" --ignore-not-found --wait=false >/dev/null || true
  kubectl -n "$namespace" delete pod "$pod_name" --ignore-not-found --wait=false >/dev/null || true
  kubectl -n "$namespace" delete configmap "$load_config" "$observer_config" --ignore-not-found >/dev/null || true
  if [[ "$settings_changed" == 1 ]]; then
  kubectl -n "$namespace" patch hpa report-service --type merge \
    -p "{\"spec\":{\"minReplicas\":$original_min,\"maxReplicas\":$original_max}}" >/dev/null || exit_status=1
  kubectl -n "$namespace" patch deployment report-service --type json -p "$affinity_restore" >"$result_dir/restore.log" 2>&1 || exit_status=1
  kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >>"$result_dir/restore.log" 2>&1 || exit_status=1
  fi
  (cd "$result_dir" && find . -maxdepth 1 -type f ! -name checksums.sha256 -print0 | sort -z | xargs -0 sha256sum) >"$result_dir/checksums.sha256"
  exit "$exit_status"
}
trap restore_cluster EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

kubectl -n "$namespace" create configmap "$load_config" --from-file=benchmark_v2.py=benchmark_v2.py --from-file=benchmark_parallel.py=benchmark_parallel.py >/dev/null
kubectl -n "$namespace" create configmap "$observer_config" --from-file=benchmark_observer.py=benchmark_observer.py >/dev/null

# Isolate the generator from the application so client CPU cannot inflate HPA metrics.
settings_changed=1
affinity=$(jq -c '(.spec.template.spec.affinity // {}) | .nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms //= [{}] | .nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms |= map(.matchExpressions += [{key:"kubernetes.io/hostname",operator:"NotIn",values:["k3s-master"]}]) | [{op:"add",path:"/spec/template/spec/affinity",value:.}]' "$result_dir/deployment-original.json")
kubectl -n "$namespace" patch deployment report-service --type json -p "$affinity" >/dev/null
kubectl -n "$namespace" rollout status deployment/report-service --timeout=180s >/dev/null

# Reproducible cold autoscaling baseline: two pods, then reopen the HPA ceiling.
kubectl -n "$namespace" patch hpa report-service --type merge \
  -p "{\"spec\":{\"minReplicas\":$baseline_replicas,\"maxReplicas\":$baseline_replicas}}" >/dev/null
kubectl -n "$namespace" scale deployment report-service --replicas="$baseline_replicas" >/dev/null
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  available=$(kubectl -n "$namespace" get deployment report-service -o jsonpath='{.status.availableReplicas}')
  total=$(kubectl -n "$namespace" get pods -l app.kubernetes.io/name=report-service -o json | jq '.items | length')
  [[ "${available:-0}" == "$baseline_replicas" && "$total" == "$baseline_replicas" ]] && break
  sleep 2
done
[[ "${available:-0}" == "$baseline_replicas" && "${total:-0}" == "$baseline_replicas" ]] || { echo "Fixed-pod baseline not reached" >&2; exit 1; }
sleep 20

kubectl get nodes -o json >"$result_dir/nodes.json"
kubectl -n "$namespace" get deployment,hpa,pdb,service,pods -o yaml >"$result_dir/cluster-before.yaml"

.benchmark-venv/bin/python benchmark_observer.py --source kubernetes \
  --duration 900 --interval 5 --label "$scenario" >"$result_dir/kubernetes-observer.jsonl" \
  2>"$result_dir/observer.stderr" &
observer_pid=$!

database_observer_overrides=$(jq -nc --arg scenario "$scenario" --arg config "$observer_config" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"observer",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q psycopg[binary]==3.2.10 >&2 && python /bench/benchmark_observer.py --source postgresql --duration 900 --interval 5 --label " + $scenario)],env:[{name:"DATABASE_URL",valueFrom:{secretKeyRef:{name:"report-service",key:"DATABASE_URL"}}}],resources:{requests:{cpu:"50m",memory:"64Mi"},limits:{cpu:"500m",memory:"256Mi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:$config}}]}}')
kubectl -n "$namespace" delete pod "$database_observer_pod" --ignore-not-found --wait=true >/dev/null
kubectl -n "$namespace" run "$database_observer_pod" --image=python:3.12-alpine --restart=Never \
  --overrides="$database_observer_overrides" >/dev/null
kubectl -n "$namespace" wait --for=condition=Ready "pod/$database_observer_pod" --timeout=120s >/dev/null

# Ready only means the container started; pip/import/connect can still fail.
deadline=$((SECONDS + 60))
telemetry_ready=0
while (( SECONDS < deadline )); do
  db_ready=$(kubectl -n "$namespace" logs "$database_observer_pod" | jq -Rsc '[split("\n")[] | fromjson? | select(has("postgresql"))] | length')
  kube_ready=$(jq -Rsc --argjson count "$baseline_replicas" --argjson required "$require_runtime_metrics" '[split("\n")[] | fromjson? | select(.kubernetes.pods | length == $count) | select($required == 0 or ((.kubernetes.runtime | length == $count) and all(.kubernetes.runtime[]; has("metrics"))))] | length' "$result_dir/kubernetes-observer.jsonl")
  if (( db_ready > 0 && kube_ready > 0 )); then telemetry_ready=1; break; fi
  sleep 2
done
[[ "$telemetry_ready" == 1 ]] || { echo "No complete baseline telemetry; load cancelled" >&2; exit 1; }
kubectl -n "$namespace" patch hpa report-service --type merge -p "{\"spec\":{\"minReplicas\":$baseline_replicas,\"maxReplicas\":$max_replicas}}" >/dev/null

args="--url http://report-service:8080 --scenario $scenario --stages $stages --batch-size $batch_size --submit-workers 128 --poll-workers 128 --drain-timeout 300 --sample-interval 5"
entry="benchmark_v2.py"
if [[ "$generators" == 2 ]]; then entry="benchmark_parallel.py"; args="--generators 2 $args"; fi
overrides=$(jq -nc --arg args "$args" --arg entry "$entry" --arg config "$load_config" '{spec:{nodeSelector:{"kubernetes.io/hostname":"k3s-master"},containers:[{name:"benchmark",image:"python:3.12-alpine",command:["sh","-lc"],args:[("pip install -q urllib3==2.5.0 >&2 && python /bench/" + $entry + " " + $args)],resources:{requests:{cpu:"500m",memory:"256Mi"},limits:{cpu:"3",memory:"1Gi"}},volumeMounts:[{name:"script",mountPath:"/bench",readOnly:true}]}],volumes:[{name:"script",configMap:{name:$config}}]}}')

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

save_evidence
finished=$(jq -s '[.[] | select(.type == "summary")][-1].finished_epoch_ms // 0' "$result_dir/load.jsonl")
deadline=$((SECONDS + 45))
bracketed=0
while (( SECONDS < deadline && finished > 0 )); do
  save_evidence
  db_final=$(jq -s --argjson end "$finished" '[.[] | select(.timestamp_epoch_ms >= $end and has("postgresql"))] | length' "$result_dir/postgresql-observer.jsonl")
  kube_final=$(jq -Rsc --argjson end "$finished" '[split("\n")[] | fromjson? | select(.timestamp_epoch_ms >= $end and has("kubernetes"))] | length' "$result_dir/kubernetes-observer.jsonl")
  if (( db_final > 0 && kube_final > 0 )); then bracketed=1; break; fi
  sleep 2
done
[[ "$bracketed" == 1 ]] || { echo "Missing telemetry after drain" >&2; exit 1; }
kubectl top pods -n "$namespace" >"$result_dir/pod-resources-after.txt" 2>&1 || true

kill "$observer_pid" 2>/dev/null || true
wait "$observer_pid" 2>/dev/null || true
observer_pid=""

if [[ "$phase" != "Succeeded" ]]; then
  echo "release benchmark finished with phase ${phase:-timeout}" >&2
  exit 1
fi
echo "$(date -Is) release benchmark complete: $result_dir"
