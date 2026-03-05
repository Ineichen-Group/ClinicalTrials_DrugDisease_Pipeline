#!/bin/bash
#SBATCH --job-name=sapbert_norm
#SBATCH --time=00:30:00
#SBATCH --mem=32G
#SBATCH --gpus=1
#SBATCH --output=logs/linking_%x_%j_%a.out
#SBATCH --error=logs/linking_%x_%j_%a.err
#SBATCH --array=0-9

# ----------- Arguments from terminal -----------
ENTITY_TYPE="$1"
COL_TO_MAP="$2"

if [ -z "$ENTITY_TYPE" ] || [ -z "$COL_TO_MAP" ]; then
    echo "Usage: sbatch script.sh <entity_type> <column_name>"
    echo "Example:"
    echo "  sbatch run_normalize_parallel.sh drug merged_drug_names"
    exit 1
fi
# -----------------------------------------------

DATA_DIR="/shares/animalwelfare.crs.uzh/Preclinical_Pipeline/04_normalization/data/"
TASK_ID="${SLURM_ARRAY_TASK_ID}"

DATASET="clinical"
CHUNK_ID=$(printf "%02d" "$TASK_ID")
INPUT_FILE="./data/aact_with_ner_for_linking/chunks/chunk_${CHUNK_ID}.csv"
OUTPUT_FILE="./data/linked_to_ontologies/chunks/${ENTITY_TYPE}_mapped_${ENTITY_TYPE}_${CHUNK_ID}.csv"
LINKING_STATS_DIR="nen_stats/chunk_${CHUNK_ID}/"

mkdir -p logs timing_logs "$LINKING_STATS_DIR" "$(dirname "$OUTPUT_FILE")"

# --- set terminology + threshold (can tweak per type) ---
if [ "$ENTITY_TYPE" = "disease" ]; then
    TERMINOLOGY="mondo"
    DIST_THRESHOLD=9.65
else
    TERMINOLOGY="umls"
    DIST_THRESHOLD=8.20
fi

echo "===== DEBUG LOG BEGIN ====="
echo "SLURM ARRAY TASK ID: $SLURM_ARRAY_TASK_ID"
echo "DATASET: $DATASET"
echo "CHUNK_ID: $CHUNK_ID"
echo "ENTITY_TYPE: $ENTITY_TYPE"
echo "COL_TO_MAP: $COL_TO_MAP"
echo "DATA_DIR: $DATA_DIR"
echo "INPUT_FILE: $INPUT_FILE"
echo "OUTPUT_FILE: $OUTPUT_FILE"
echo "LINKING_STATS_DIR: $LINKING_STATS_DIR"
echo "TERMINOLOGY: $TERMINOLOGY"
echo "DIST_THRESHOLD: $DIST_THRESHOLD"
echo "===== DEBUG LOG END ====="

START_TIME=$(date +%s)

echo "Starting normalization for ${ENTITY_TYPE} (${DATASET}, chunk ${CHUNK_ID})"
python neural_based_nen.py \
  --type "$ENTITY_TYPE" \
  --col_to_map "$COL_TO_MAP" \
  --data_dir "$DATA_DIR" \
  --input "$INPUT_FILE" \
  --output "$OUTPUT_FILE" \
  --stats_dir "$LINKING_STATS_DIR" \
  --terminology "$TERMINOLOGY" \
  --dist_threshold "$DIST_THRESHOLD"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "${ENTITY_TYPE},${DATASET},chunk_${CHUNK_ID},${DURATION}" >> "timing_logs/${ENTITY_TYPE}_timing.csv"
echo "Finished ${DATASET} chunk ${CHUNK_ID} for ${ENTITY_TYPE} in ${DURATION} seconds"