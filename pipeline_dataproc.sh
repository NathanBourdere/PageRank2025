#!/usr/bin/env bash
# Pipeline multi-cluster Dataproc pour exécuter PageRank (RDD ou DataFrame)
# - Nombre de clusters configurable
# - Exécute un job PySpark par cluster
# - Sauvegarde les résultats dans GCS
# - Supprime TOUS les clusters même en cas d’erreur

set -euo pipefail

###########################
# Chargement optionnel .env
###########################
if [ -f ".env" ]; then
  # shellcheck disable=SC1091
  source .env
fi

###########################
# Defaults (si non overridés par .env)
###########################
PROJECT_ID="${PROJECT_ID:-pagerank2025-479616}"
BUCKET="${BUCKET:-pagerank2025-yannis-bucket}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1}"
IMAGE_VERSION="${IMAGE_VERSION:-2.1-debian11}"

# Paramètres pipeline
NUM_CLUSTERS="${NUM_CLUSTERS:-3}"     # <--- nombre de clusters à créer
MODE="${MODE:-df}"                   # "rdd" ou "df"
INPUT_PATH="gs://${BUCKET}/data/wikilinks_sampled_partition_0.ttl"

# Chemins des jobs
JOB_RDD="gs://${BUCKET}/jobs/page_rank_rdd.py"
JOB_DF="gs://${BUCKET}/jobs/page_rank_df.py"

BASE_OUTPUT="gs://${BUCKET}/outputs/pagerank-$(date +%s)"

# Liste des clusters créés
CLUSTERS=()

TMPDIR="$(mktemp -d)"

#################################
# Cleanup sur exit (réussite/err)
#################################
cleanup() {
  echo ""
  echo "===== CLEANUP ====="
  rm -rf "$TMPDIR"

  for CL in "${CLUSTERS[@]}"; do
    echo "Suppression du cluster : $CL"
    gcloud dataproc clusters delete "$CL" \
        --region="$REGION" --project="$PROJECT_ID" --quiet || true
  done
}
trap cleanup EXIT


###########################
# Vérifications basiques
###########################
command -v gcloud >/dev/null || { echo "gcloud manquant"; exit 1; }
command -v gsutil >/dev/null || { echo "gsutil manquant"; exit 1; }

if [ "$PROJECT_ID" = "YOUR_PROJECT_ID" ] || [ "$BUCKET" = "YOUR_BUCKET_NAME" ]; then
  echo "⚠️ Configure PROJECT_ID et BUCKET dans .env ou en haut du script."
  exit 1
fi


###########################
# Bucket
###########################
echo "Vérification du bucket gs://${BUCKET}"
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
  echo "Bucket inexistant -> création"
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://${BUCKET}"
else
  echo "Bucket OK."
fi

###########################
# Upload jobs PySpark
###########################
echo "Upload des jobs PageRank"
gsutil cp -n "./src/pagerank_rdd.py" "$JOB_RDD"  || true
gsutil cp -n "./src/pagerank_df.py" "$JOB_DF"    || true


###########################
# Sélection job selon mode
###########################
if [ "$MODE" = "rdd" ]; then
  JOB="$JOB_RDD"
elif [ "$MODE" = "df" ]; then
  JOB="$JOB_DF"
else
  echo "MODE inconnu : $MODE (utilise 'rdd' ou 'df')"
  exit 1
fi

echo "Mode PageRank : $MODE"
echo "Job utilisé : $JOB"


########################################
# Création + exécution multi-clusters
########################################

for i in $(seq 1 "$NUM_CLUSTERS"); do
  CLUSTER_NAME="pagerank-cluster-$i-$(date +%s)"
  CLUSTERS+=("$CLUSTER_NAME")

  # Définir nombre de workers (chaque worker = un nœud)
  if [ "$i" -eq 1 ]; then
    NUM_WORKERS=6   # 2 nœuds
  elif [ "$i" -eq 2 ]; then
    NUM_WORKERS=4   # 4 nœuds
  else
    NUM_WORKERS=2   # 6 nœuds
  fi

  echo "Création cluster $i/${NUM_CLUSTERS} : $CLUSTER_NAME avec $NUM_WORKERS workers"

  gcloud dataproc clusters create "$CLUSTER_NAME" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --master-machine-type=e2-standard-4 \
    --num-workers="$NUM_WORKERS" \
    --worker-machine-type=e2-medium \
    --worker-boot-disk-size=50 \
    --image-version="$IMAGE_VERSION"

    echo "Cluster $i créé."

  ########################################
  # Exécution du job
  ########################################
  OUTPUT_PATH="${BASE_OUTPUT}/cluster_${i}"

  echo "Soumission job PageRank -> output : $OUTPUT_PATH"

  gcloud dataproc jobs submit pyspark "$JOB" \
      --cluster="$CLUSTER_NAME" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      -- \
      --input "$INPUT_PATH" \
      --output "$OUTPUT_PATH"

  echo "Job terminé pour cluster $i"
done


########################################
# Téléchargement des résultats
########################################

echo ""
echo "Téléchargement des résultats finaux"
LOCAL_OUT="outputs/pagerank_multi"
mkdir -p "$LOCAL_OUT"

gsutil -m cp -r "${BASE_OUTPUT}" "$LOCAL_OUT/"

echo "Résultats disponibles dans : $LOCAL_OUT/"

echo ""
echo "Pipeline terminé avec succès (tous les clusters seront supprimés par cleanup)"
