#!/bin/bash

# Variabili di base
REGION="europe-west1"
BUCKET="gs://terremoti-mfontana-2026"
JAR="${BUCKET}/EarthquakeCoOccurrence-assembly-1.0.jar"
DATASET="${BUCKET}/dataset-earthquakes.csv"
CLASS="EarthquakeCoOccurrence"


# Funzione automatica: Crea Cluster -> Lancia Job -> Distrugge Cluster
esegui_batteria() {
  WORKERS=$1
  shift
  PARTIZIONI=("$@")
  CLUSTER_NAME="cluster-${WORKERS}w"

  echo "======================================================"
  echo ">>> FASE: CREAZIONE CLUSTER CON ${WORKERS} WORKER <<<"
  echo "======================================================"
  gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${REGION} \
    --num-workers=${WORKERS} \
    --master-boot-disk-size=240 \
    --worker-boot-disk-size=240 \
    --master-machine-type=n2-standard-4 \
    --worker-machine-type=n2-standard-4

  # Job di Default (senza partizioni forzate)
  JOB_ID="test-${WORKERS}w-default"
  echo ">>> LANCIO JOB: ${JOB_ID} <<<"
  gcloud dataproc jobs submit spark \
    --id=${JOB_ID} \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION} \
    --class=${CLASS} \
    --jars=${JAR} \
    -- ${DATASET}

  # Job con partizioni specifiche
  for P in "${PARTIZIONI[@]}"; do
    JOB_ID="test-${WORKERS}w-${P}p"
    echo ">>> LANCIO JOB: ${JOB_ID} <<<"
    gcloud dataproc jobs submit spark \
      --id=${JOB_ID} \
      --cluster=${CLUSTER_NAME} \
      --region=${REGION} \
      --class=${CLASS} \
      --jars=${JAR} \
      -- ${DATASET} ${P}
  done

  echo ">>> ELIMINAZIONE CLUSTER ${CLUSTER_NAME} <<<"
  # Il parametro --quiet evita che chieda conferma (Y/n) bloccando lo script
  gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet
}

# ==========================================
# ESECUZIONE DELLA MATRICE DI TEST
# (Sintassi: esegui_batteria <num_workers> <partizione_1> <partizione_2>)
# ==========================================

# Test a 4 Worker (16 CPU) - Partizioni: 16 e 32
esegui_batteria 4 8 64

# Test a 3 Worker (12 CPU) - Partizioni: 12 e 24
esegui_batteria 3 48

# Test a 2 Worker (8 CPU) - Partizioni: 8 e 16
esegui_batteria 2 32 64

echo "======================================================"
echo ">>> TUTTI I TEST SONO STATI COMPLETATI CON SUCCESSO! <<<"
echo "======================================================"