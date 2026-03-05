#!/bin/bash
#SBATCH --job-name=inference_jobs       # Job name
#SBATCH --output=logs/%A_%a.out             # %A is the job ID, %a is the array index
#SBATCH --error=logs/%A_%a.err              # Error log for the job
#SBATCH --gres=gpu:1                   # Reserve 1 GPU per job
#SBATCH --array=0-9                   
#SBATCH --time=05:00:00                # Set max runtime for each job
#SBATCH --mem=16GB                     # Memory allocation
#SBATCH --cpus-per-task=4              # Number of CPUs per task

mkdir -p logs

MODEL_NAME="michiyasunaga/BioLinkBERT-base"
MODEL_PATH="ner_model/michiyasunaga_biolinkbert/epochs_15_data_size_100_iter_4"
OUTPUT_DIR="./data/model_predictions/"
mkdir -p "$OUTPUT_DIR"

CHUNK_ID=$(printf "%02d" "$SLURM_ARRAY_TASK_ID")
TEST_DATA_CSV="data/aact_for_ner/chunks/chunk_${CHUNK_ID}.csv"
OUTPUT_FILE_SUFFIX="_part_${CHUNK_ID}"

start_time=$(date +%s)

echo "Running inference job..."
echo "SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
echo "Chunk ID: $CHUNK_ID"
echo "Test file: $TEST_DATA_CSV"
echo "Model: $MODEL_NAME"
echo "Output directory: $OUTPUT_DIR"

python inference_ner_annotations.py \
  --test_data_csv "$TEST_DATA_CSV" \
  --output_dir "$OUTPUT_DIR" \
  --output_file_suffix "$OUTPUT_FILE_SUFFIX" \
  --model_name "$MODEL_NAME" \
  --model_path "$MODEL_PATH"

end_time=$(date +%s)
duration=$(( (end_time - start_time) / 60 ))
echo "Inference completed for $TEST_DATA_CSV. Duration: $duration minutes."
