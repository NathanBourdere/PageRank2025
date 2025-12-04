#!/usr/bin/env bash
set -euo pipefail

VARIANT="$1"
shift
PROJECT_ID=""
BUCKET=""
CLUSTER_BASE="pagerank-dp"
REGION="europe-west1"
ZONE="europe-west1-b"
IMAGE_VERSION="2.2-debian12"
MACHINE_TYPE="n1-standard-4"
NODES_LIST=(2 4 6)
LOCAL_RESULTS_DIR="results"
JOBS_DIR_LOCAL="src"
TOPK=50
PARTITIONS=200

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT_ID="$2"; shift 2;;
    --bucket) BUCKET="$2"; shift 2;;
    --clusters) IFS=',' read -r -a NODES_LIST <<< "$2"; shift 2;;
    --machine-type) MACHINE_TYPE="$2"; shift 2;;
    --image) IMAGE_VERSION="$2"; shift 2;;
    --partitions) PARTITIONS="$2"; shift 2;;
    --topk) TOPK="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ -z "$PROJECT_ID" || -z "$BUCKET" ]]; then
  echo "Usage: $0 <rdd|df> --project PROJECT_ID --bucket BUCKET_NAME"
  exit 1
fi

INPUT_PATH="gs://${BUCKET}/sample10pct.ttl"
GCS_JOB_DIR="gs://${BUCKET}/jobs"

JOB_LOCAL_PATH="$JOBS_DIR_LOCAL/pagerank_${VARIANT}.py"
JOB_GCS_PATH="$GCS_JOB_DIR/pagerank_${VARIANT}.py"

mkdir -p "$LOCAL_RESULTS_DIR"
echo "Uploading job $JOB_LOCAL_PATH -> $JOB_GCS_PATH"
gcloud storage cp "$JOB_LOCAL_PATH" "$JOB_GCS_PATH" --project="$PROJECT_ID"

CLUSTERS_TO_DELETE=()
cleanup() {
  for c in "${CLUSTERS_TO_DELETE[@]}"; do
    echo "Deleting cluster $c..."
    gcloud dataproc clusters delete "$c" --region="$REGION" --project="$PROJECT_ID" --quiet || true
  done
}
trap cleanup EXIT

create_cluster() {
  local total_nodes=$1
  local cluster_name="$CLUSTER_BASE-${VARIANT}-${total_nodes}nodes"
  local num_workers=$(( total_nodes > 2 ? total_nodes - 1 : 2 ))  # master + workers

  gcloud dataproc clusters create "$cluster_name" \
      --project="$PROJECT_ID" \
      --region="$REGION" \
      --master-machine-type="$MACHINE_TYPE" \
      --worker-machine-type="$MACHINE_TYPE" \
      --num-workers="$num_workers" \
      --image-version="$IMAGE_VERSION" \
      --quiet --no-address

  echo "$cluster_name"  # retourner juste le nom réel
}

submit_job_async() {
  local cluster_name="$1"
  local timestamp=$(date +%s)
  local out_gcs="gs://${BUCKET}/outputs/${cluster_name}-${VARIANT}-${timestamp}"

  local spark_args=(-- --input "$INPUT_PATH" --output "$out_gcs" --partitions "$PARTITIONS" --topk "$TOPK")

  echo "Submitting pyspark job to cluster $cluster_name, output -> $out_gcs"

  local job_id
  job_id=$(gcloud dataproc jobs submit pyspark "$JOB_GCS_PATH" \
      --cluster="$cluster_name" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --async \
      --format="value(reference.jobId)" \
      -- "${spark_args[@]}")

  if [[ -z "$job_id" ]]; then
    echo "Failed to get job id"; exit 1
  fi

  echo "$job_id|$out_gcs"
}

wait_and_collect() {
  local job_id="$1"
  local status_json
  echo "Waiting for job $job_id..."
  status_json=$(gcloud dataproc jobs wait "$job_id" --region="$REGION" --project="$PROJECT_ID" --format=json)
  local status submission_time start_time end_time
  status=$(echo "$status_json" | jq -r '.status.state')
  submission_time=$(echo "$status_json" | jq -r '.status.submissionTime')
  start_time=$(echo "$status_json" | jq -r '.status.startTime')
  end_time=$(echo "$status_json" | jq -r '.status.doneTime')
  echo "$status|$submission_time|$start_time|$end_time"
}

for n in "${NODES_LIST[@]}"; do
  echo -e "\n==== Running variant=$VARIANT on $n nodes ===="
  cluster_name=$(create_cluster "$n")

  res=$(submit_job_async "$cluster_name")
  job_id=$(echo "$res" | cut -d'|' -f1)
  out_gcs=$(echo "$res" | cut -d'|' -f2)

  job_meta=$(wait_and_collect "$job_id")
  status=$(echo "$job_meta" | cut -d'|' -f1)
  submission_time=$(echo "$job_meta" | cut -d'|' -f2)
  start_time=$(echo "$job_meta" | cut -d'|' -f3)
  end_time=$(echo "$job_meta" | cut -d'|' -f4)

  run_dir="$LOCAL_RESULTS_DIR/${cluster_name}"
  mkdir -p "$run_dir"

  if gsutil -q ls "${out_gcs}/*" 2>/dev/null; then
    gsutil -m cp -r "${out_gcs}/pagerank_topk*" "$run_dir/" || true
    gsutil -m cp -r "${out_gcs}/part-*" "$run_dir/" || true
  else
    echo "No output files found at ${out_gcs}"
  fi

  cat > "$run_dir/metadata.txt" <<EOF
variant=${VARIANT}
cluster=${cluster_name}
nodes=${n}
machine_type=${MACHINE_TYPE}
project=${PROJECT_ID}
bucket=${BUCKET}
job_id=${job_id}
status=${status}
submission_time=${submission_time}
start_time=${start_time}
end_time=${end_time}
output_gcs=${out_gcs}
partitions=${PARTITIONS}
topk=${TOPK}
EOF

  echo "Run for $n nodes complete. Results in $run_dir"
done

echo "All runs finished. Local results directory: $LOCAL_RESULTS_DIR"
