#!/usr/bin/env bash
# dataproc-pagerank-pipeline.sh
# Pipeline paramétrable pour lancer PageRank (DataFrame ou RDD) sur Dataproc
# - crée le cluster (1 master + N-1 workers)
# - upload du job sur GCS
# - soumet le job en asynchrone, récupère les timestamps et l'ID job
# - télécharge les résultats (top-k) et un log métrique
# - supprime le cluster
# Usage:
# ./dataproc-pagerank-pipeline.sh <rdd|df> --project PROJECT_ID --bucket BUCKET_NAME [--clusters 2,4,6]

set -euo pipefail

# Default params
VARIANT="$1" # rdd or df
shift
PROJECT_ID=""
BUCKET=""
CLUSTER_BASE="pagerank-dp"
REGION="europe-west1"
IMAGE_VERSION="2.1-debian11"
MACHINE_TYPE="n1-standard-4"
NODES_LIST=(2 4 6)
LOCAL_RESULTS_DIR="results"
JOBS_DIR_LOCAL="src"
TOPK=50
PARTITIONS=200

# parse optional args
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
  echo "Usage: $0 <rdd|df> --project PROJECT_ID --bucket BUCKET_NAME [--clusters 2,4,6]"
  exit 1
fi

if [[ "$VARIANT" != "rdd" && "$VARIANT" != "df" ]]; then
  echo "Variant must be 'rdd' or 'df'"
  exit 1
fi

INPUT_PATH="gs://${BUCKET}/wikilinks_lang=en.ttl.bz2"
GCS_JOB_DIR="gs://${BUCKET}/jobs"

# Determine job filenames
if [[ "$VARIANT" == "rdd" ]]; then
  JOB_LOCAL_PATH="$JOBS_DIR_LOCAL/pagerank_rdd.py"
  JOB_GCS_PATH="$GCS_JOB_DIR/pagerank_rdd.py"
else
  JOB_LOCAL_PATH="$JOBS_DIR_LOCAL/pagerank_df.py"
  JOB_GCS_PATH="$GCS_JOB_DIR/pagerank_df.py"
fi

mkdir -p "$LOCAL_RESULTS_DIR"

# Upload job to GCS (overwrite)
echo "Uploading job $JOB_LOCAL_PATH -> $JOB_GCS_PATH"
gsutil cp "$JOB_LOCAL_PATH" "$JOB_GCS_PATH"

# Helper: create cluster with total_nodes (including master)
create_cluster() {
  local total_nodes=$1
  local cluster_name="$CLUSTER_BASE-${VARIANT}-${total_nodes}nodes-$(date +%s)"
  local num_workers=$total_nodes

  if [[ $num_workers -lt 1 ]]; then
    echo "ERROR: need at least 1 worker" >&2
    exit 1
  fi

  echo "Creating cluster $cluster_name (master + $num_workers workers) with $MACHINE_TYPE..."
  
  if [[ $num_workers -eq 1 ]]; then
    # single-node cluster
    gcloud dataproc clusters create "$cluster_name" \
      --project="$PROJECT_ID" --region="$REGION" --single-node \
      --image-version="$IMAGE_VERSION" --no-address --quiet \
      --properties="dataproc:dataproc.allow.zero.workers=true"
  else
    gcloud dataproc clusters create "$cluster_name" \
      --project="$PROJECT_ID" --region="$REGION" \
      --master-machine-type="$MACHINE_TYPE" --worker-machine-type="$MACHINE_TYPE" \
      --num-workers="$num_workers" --image-version="$IMAGE_VERSION" --quiet --no-address
  fi

  # RETURN JUST THE NAME
  echo "$cluster_name"
}

# Helper: delete cluster
delete_cluster() {
  local cluster_name="$1"
  echo "Deleting cluster $cluster_name..."
  gcloud dataproc clusters delete "$cluster_name" --region="$REGION" --project="$PROJECT_ID" --quiet || true
}

# Helper: submit job async and return job-id
submit_job_async() {
  local cluster_name="$1"
  local out_gcs="gs://${BUCKET}/outputs/${cluster_name}-${VARIANT}-$(date +%s)"

  local spark_args=(-- --input "$INPUT_PATH" --output "$out_gcs" --partitions "$PARTITIONS" --topk "$TOPK")

  echo "Submitting pyspark job to cluster $cluster_name, output -> $out_gcs"

  local submit_out
  submit_out=$(gcloud dataproc jobs submit pyspark "$JOB_GCS_PATH" \
    --cluster="$cluster_name" --region="$REGION" --project="$PROJECT_ID" --async -- "${spark_args[@]}" 2>&1)

  local job_id
  job_id=$(echo "$submit_out" | grep -Eo 'jobId: [^ ]+' | awk '{print $2}')
  if [[ -z "$job_id" ]]; then
    echo "Failed to get job id from gcloud output:"; echo "$submit_out"; exit 1
  fi

  echo "$job_id|$out_gcs"
}

# Helper: wait for job and collect start/end and status
wait_and_collect() {
  local job_id="$1"
  local cluster_name="$2"

  echo "Waiting for job $job_id to finish..."
  gcloud dataproc jobs wait "$job_id" --region="$REGION" --project="$PROJECT_ID"

  local desc
  desc=$(gcloud dataproc jobs describe "$job_id" --region="$REGION" --project="$PROJECT_ID" --format=json)

  local status submission_time start_time end_time
  status=$(echo "$desc" | grep -Po '(?<="state": ")[^"]+' | head -n1 || true)
  submission_time=$(echo "$desc" | grep -Po '(?<="submitted": ")[^"]+' || true)
  start_time=$(echo "$desc" | grep -Po '(?<="startTime": ")[^"]+' || true)
  end_time=$(echo "$desc" | grep -Po '(?<="doneTime": ")[^"]+' || true)

  echo "$status|$submission_time|$start_time|$end_time"
}

# Main loop over cluster sizes
for n in "${NODES_LIST[@]}"; do
  echo -e "\n==== Running variant=$VARIANT on $n nodes (machine=$MACHINE_TYPE) ===="

  cluster_name=$(create_cluster "$n")
  trap "delete_cluster '$cluster_name'" EXIT

  res=$(submit_job_async "$cluster_name")
  job_id=$(echo "$res" | cut -d'|' -f1)
  out_gcs=$(echo "$res" | cut -d'|' -f2)

  job_meta=$(wait_and_collect "$job_id" "$cluster_name")
  status=$(echo "$job_meta" | cut -d'|' -f1)
  submission_time=$(echo "$job_meta" | cut -d'|' -f2)
  start_time=$(echo "$job_meta" | cut -d'|' -f3)
  end_time=$(echo "$job_meta" | cut -d'|' -f4)

  run_dir="$LOCAL_RESULTS_DIR/${cluster_name}"
  mkdir -p "$run_dir"

  echo "Attempting to download results from $out_gcs"
  if gsutil -q ls "${out_gcs}/*" 2>/dev/null; then
    gsutil -m cp -r "${out_gcs}/pagerank_topk*" "$run_dir/" || true
    gsutil -m cp -r "${out_gcs}/part-*" "$run_dir/" || true
  else
    echo "No output files found at ${out_gcs}"
  fi

  meta_file="$run_dir/metadata.txt"
  cat > "$meta_file" <<EOF
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

  echo "Saved metadata to $meta_file"

  delete_cluster "$cluster_name"
  trap - EXIT

  echo "Run for $n nodes complete. Results in $run_dir"

done

echo "All runs finished. Local results directory: $LOCAL_RESULTS_DIR"

echo -e "Notes and recommendations:\n- Use the same MACHINE_TYPE for all runs so results are comparable.\n- We set n1-standard-4 by default (4 vCPU/node). With 6 nodes => 24 vCPU total under the 32 vCPU cap.\n- No need to create a bucket per node: use un seul bucket pour jobs/data/outputs.\n- Ensure your pagerank scripts accept the flags: --input, --output, --partitions, --topk and write a compact pagerank_topk.tsv.\n- To avoid shuffle: pré-partition by source node id et map-side joins.\n"
