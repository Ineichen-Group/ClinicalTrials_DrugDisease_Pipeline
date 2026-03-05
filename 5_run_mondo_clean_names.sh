#!/bin/bash
#SBATCH --job-name=mondo_group_clean
#SBATCH --output=logs/mondo_group_clean_%j.out
#SBATCH --error=logs/mondo_group_clean_%j.err
#SBATCH --time=1:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --array=0-9

SCRIPT="mondo_clean_names.py"

IN_DIR="./data/linked_to_ontologies/chunks"
OUT_DIR="./data/linked_to_ontologies/mondo_cleaned_chunks"

mkdir -p logs "$OUT_DIR"

CHUNK_ID=$(printf "%02d" "$SLURM_ARRAY_TASK_ID")

CLINICAL_IN="${IN_DIR}/disease_mapped_disease_${CHUNK_ID}.csv"
CLINICAL_OUT="${OUT_DIR}/disease_mapped_disease_${CHUNK_ID}_mondo_cleaned.csv"

echo "===== MONDO GROUP & CLEAN ====="
echo "Job ID:        ${SLURM_JOB_ID:-local}"
echo "Array task:    ${SLURM_ARRAY_TASK_ID:-local}"
echo "Chunk ID:      $CHUNK_ID"
echo "Clinical in:   $CLINICAL_IN"
echo "Clinical out:  $CLINICAL_OUT"
echo "==============================="

if [ ! -f "$CLINICAL_IN" ]; then
  echo "ERROR: input file not found: $CLINICAL_IN"
  exit 2
fi

START=$(date +%s)

python "$SCRIPT" \
  --clinical_input "$CLINICAL_IN" \
  --clinical_output "$CLINICAL_OUT" \
  --clinical_key nct_id \
  --raw_col disease_mondo_term_norm \
  --id_col disease_mondo_termid \
  --grouped_col disease_term_mondo_clean \
  --out_id_col disease_termid_mondo_clean \
  --verbose

END=$(date +%s)
echo "Finished in $((END - START)) seconds"