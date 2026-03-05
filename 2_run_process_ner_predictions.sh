#!/bin/bash
#SBATCH --job-name=postproc_entities
#SBATCH --output=logs/postproc_entities_%A_%a.out
#SBATCH --error=logs/postproc_entities_%A_%a.err
#SBATCH --array=0-9
#SBATCH --time=00:10:00
#SBATCH --mem=16GB
#SBATCH --cpus-per-task=4

mkdir -p logs

CHUNK_ID=$(printf "%02d" "$SLURM_ARRAY_TASK_ID")

# Paths
PRED_DIR="./data/model_predictions"
CHUNK_DIR="./data/aact_for_ner/chunks"
OUT_DIR="./data/model_predictions/processed_entities"
mkdir -p "$OUT_DIR"

# Update this to match your actual prediction filename prefix if different
# Example prediction file name:
# test_annotated_BioLinkBERT-base_tuples_20260221_part_00.csv
PRED_FILE="${PRED_DIR}/test_annotated_BioLinkBERT-base_tuples_20260221_part_${CHUNK_ID}.csv"
TEXT_CHUNK_TEMPLATE="${CHUNK_DIR}/chunk_{part}.csv"

start_time=$(date +%s)

echo "Running post-processing job..."
echo "SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
echo "Chunk ID: $CHUNK_ID"
echo "Predictions file: $PRED_FILE"
echo "Text chunk template: $TEXT_CHUNK_TEMPLATE"
echo "Output directory: $OUT_DIR"

python process_ner_predictions.py \
  --predictions_glob "${PRED_DIR}/test_annotated_BioLinkBERT-base_tuples_20260221_part_*.csv" \
  --text_chunk_template "$TEXT_CHUNK_TEMPLATE" \
  --part "$CHUNK_ID" \
  --pred_col "ner_prediction_BioLinkBERT-base_normalized" \
  --text_col "Text" \
  --id_col "nct_id" \
  --out_dir "$OUT_DIR" \
  --out_prefix "entities" \

end_time=$(date +%s)
duration=$(( (end_time - start_time) / 60 ))
echo "Post-processing completed for part $CHUNK_ID. Duration: $duration minutes."