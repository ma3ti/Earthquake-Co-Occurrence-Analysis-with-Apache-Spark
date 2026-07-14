#!/bin/bash
set -u

REGION="europe-west1"
BUCKET="gs://terremoti-mfontana-2026"
JAR="${BUCKET}/EarthquakeCoOccurrence-assembly-1.0.jar"
DATASET="${BUCKET}/dataset-earthquakes.csv"
CLASS_V2="EarthquakeCoOccurrenceV2"

CURRENT_CLUSTER=""
cleanup() {
  if [ -n "${CURRENT_CLUSTER}" ]; then
    echo ">>> Interruzione: elimino ${CURRENT_CLUSTER} <<<"
    gcloud dataproc clusters delete "${CURRENT_CLUSTER}" --region="${REGION}" --quiet
    CURRENT_CLUSTER=""
  fi
}
trap cleanup INT TERM

# Uso: esegui_batteria_v2 <num_workers> <spec1> <spec2> ...
#   spec = numero di partizioni oppure "default"
esegui_batteria_v2() {
  local WORKERS=$1
  shift
  local SPECS=("$@")
  local CLUSTER_NAME="cluster-${WORKERS}w"

  echo "======================================================"
  echo ">>> CREAZIONE CLUSTER ${WORKERS} WORKER (approccio V2) <<<"
  echo "======================================================"
  gcloud dataproc clusters create "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --num-workers="${WORKERS}" \
    --master-boot-disk-size=240 \
    --worker-boot-disk-size=240 \
    --master-machine-type=n2-standard-4 \
    --worker-machine-type=n2-standard-4 || {
      echo ">>> ERRORE creazione ${CLUSTER_NAME}, salto <<<"
      return 1
  }
  CURRENT_CLUSTER="${CLUSTER_NAME}"

  for SPEC in "${SPECS[@]}"; do
    local JOB_ID
    local APP_ARGS
    if [ "${SPEC}" == "default" ]; then
      JOB_ID="v2-${WORKERS}w-default"
      APP_ARGS="${DATASET}"
    else
      JOB_ID="v2-${WORKERS}w-${SPEC}p"
      APP_ARGS="${DATASET} ${SPEC}"
    fi

    echo ">>> LANCIO JOB: ${JOB_ID} <<<"
    gcloud dataproc jobs submit spark \
      --id="${JOB_ID}" \
      --cluster="${CLUSTER_NAME}" \
      --region="${REGION}" \
      --class="${CLASS_V2}" \
      --jars="${JAR}" \
      -- ${APP_ARGS} || echo ">>> ATTENZIONE: ${JOB_ID} fallito <<<"
  done

  echo ">>> ELIMINAZIONE CLUSTER ${CLUSTER_NAME} <<<"
  gcloud dataproc clusters delete "${CLUSTER_NAME}" --region="${REGION}" --quiet
  CURRENT_CLUSTER=""
}

# ====================================================================
#  CONFRONTO V1 vs V2
#  Stesse configurazioni già usate per la V1 => confronto diretto, a parità di cluster.
# ====================================================================

# 4 worker: le stesse tre config della V1 (default, 16, 32)
esegui_batteria_v2 4 default 16 32

# 3 worker: default, 12, 24 (il 12p era FALLITO in V1 -> test cruciale)
esegui_batteria_v2 3 default 12 24

# 2 worker: default, 8, 16 (8p e 16p erano FALLITI in V1 -> test cruciale)
esegui_batteria_v2 2 default 8 16

echo "======================================================"
echo ">>> BATTERIA V2 TERMINATA <<<"
echo ">>> VERIFICA 1: l'output di ogni job V2 deve essere IDENTICO a quello della V1."
echo ">>> VERIFICA 2: le config 3w-12p, 2w-8p, 2w-16p fallivano in V1."
echo ">>>             Se ora completano, hai la prova che il collo di bottiglia era il self-join."
echo "======================================================"