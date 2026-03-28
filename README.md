# Load and Filter AACT

## 1. Download AACT PostgreSQL Dump

Download the PostgreSQL dump from:

https://aact.ctti-clinicaltrials.org/downloads

For this project we used the historical snapshot: 20251201_clinical_trials_ctgov.zip

Unzip the archive to obtain the `.dmp` file.

## 2. Restore into PostgreSQL

Create a database:

```bash
createdb aact_2025_12
```

Restore the dump:
```bash
pg_restore -c -d aact_2025_12 path_to_downloaded_file.dmp
```
## 2. Create Filtered Materialized View

We restrict to:
- INTERVENTIONAL studies
- Studies with at least one relevant intervention type
- Aggregated intervention names
- Aggregated condition names

```SQL
DROP MATERIALIZED VIEW IF EXISTS ctgov.mv_interventional_drug_studies;

CREATE MATERIALIZED VIEW ctgov.mv_interventional_drug_studies AS
SELECT
    s.nct_id,
    s.brief_title,
    s.official_title AS study_official_title,
    s.start_date,
    s.completion_date,
    s.study_first_submitted_date,
    s.phase,
    s.overall_status,
    b.description AS brief_summary,
    d.intervention_names,
    d.intervention_types,
    c.condition_names
FROM ctgov.studies s
JOIN ctgov.brief_summaries b
    ON s.nct_id = b.nct_id
JOIN (
    SELECT
        nct_id,
        string_agg(DISTINCT name, ' | ' ORDER BY name) AS intervention_names,
        string_agg(DISTINCT intervention_type, ' | ' ORDER BY intervention_type) AS intervention_types
    FROM ctgov.interventions
    WHERE intervention_type IN ('DRUG', 'DIETARY_SUPPLEMENT', 'BIOLOGICAL', 'COMBINATION_PRODUCT', 'GENETIC', 'OTHER')
    GROUP BY nct_id
) d ON s.nct_id = d.nct_id
JOIN (
    SELECT
        nct_id,
        string_agg(DISTINCT name, ' | ' ORDER BY name) AS condition_names
    FROM ctgov.conditions
    GROUP BY nct_id
) c ON s.nct_id = c.nct_id
WHERE s.study_type = 'INTERVENTIONAL';
```

# NER inference
### Step 0: Prepare AACT texts

Run: [./0_prepare_aact_texts.py](./0_prepare_aact_texts.py).

This step prepares clinical trial texts for NER inference:
- Combines study_official_title and brief_summary into a single Text field
- Fills missing titles using brief_title
- Removes duplicate entries
- Splits the dataset into sequential chunks

The output is a set of chunked CSV files (chunk_00.csv, chunk_01.csv, …), enabling parallel processing during inference.

### Step 1: Download NER model and run NER inference
Dwnlaod the model from [https://zenodo.org/records/19290637](https://zenodo.org/records/19290637). Place it locally, for example: MODEL_PATH="ner_model/michiyasunaga_biolinkbert/epochs_15_data_size_100_iter_4". Alternatively, update the MODEL_PATH variable in the inference script to match your setup.

Run: [.1_run_ner_drug_disease.sh](./1_run_ner_drug_disease.sh).

This script:
- Processes each chunk in parallel (e.g., via SLURM array jobs)
- Loads the fine-tuned BioLinkBERT NER model
- Annotates drug and disease entities
- Saves predictions as CSV files in the output directory


# NER cleaning

The script merges AACT clinical trial data with NER-extracted drug and condition entities. It combines predictions from multiple files, joins them to AACT records by nct_id, and creates merged drug and condition lists from both sources. 

Rows without any predictions are removed, missing drug predictions are filled from AACT intervention names when the intervention type is a drug, and missing condition predictions are filled from AACT conditions when possible. 

Rows still missing drug predictions are saved separately and excluded. The final cleaned dataset is then saved as a single CSV and optionally split into chunks for downstream processing.

```bash
python 3_combine_aact_with_ner.py \
  --aact_csv ./data/raw_aact/mv_interventional_drug_studies_20260302.csv \
  --predictions_glob "./data/model_predictions/processed_entities/entities_*_part_*.csv" \
  --out_dir ./data/aact_with_ner_for_linking \
  --out_name aact_with_ner.csv \
  --pred_drug_col unique_interventions \
  --pred_cond_col unique_conditions
  ```


# Ontologies Linking


Download the named entity linking resources from [https://zenodo.org/records/19287944](https://zenodo.org/records/19287944). These files contain precomputed ontology embeddings and mappings required for drug and disease normalization.

After downloading, place them in the repository for example as follows:

```
data/entity_linking/
                ├── mondo/
                │   ├── embeddings/
                │   ├── mondo_term_id_pairs.json
                │   └── mondo_id_to_term_map.json
                └── umls/
                    ├── embeddings/
                    ├── umls_term_id_pairs_combined.json
                    └── umls_id_to_term_map.json 
```

The path to the data (DATA_DIR) has to be adjusted in [./4_run_normalize_parallel.sh](./4_run_normalize_parallel.sh).

```bash
sbatch 4_run_normalize_parallel.sh drug ner_predicted_drugs

sbatch 4_run_normalize_parallel.sh disease ner_predicted_conditions
```

# Merge and clean

```bash
sbatch 5_run_mondo_clean_names.sh
```

```bash
python 6_merge_cleaned_chunks.py \
  --disease_glob "./data/linked_to_ontologies/mondo_cleaned_chunks/disease_mapped_disease_*_mondo_cleaned.csv" \
  --drug_glob "./data/linked_to_ontologies/chunks/drug_mapped_drug_*.csv" \
  --disease_output "./data/linked_to_ontologies/mapped_clinical_data_mondo_cleaned.csv" \
  --drug_output "./data/linked_to_ontologies/mapped_clinical_data_umls.csv" \
  --dedupe_on_id
```

# Map to parent

```bash
sbatch 7_run_umls_map_to_parent.sh

sbatch 8_run_mondo_map_to_parent.sh
```

# Merge final
```bash
python 9_join_drug_disease_entities.py \
  --disease_csv "./data/linked_to_ontologies/mapped_clinical_data_disease_cleaned_with_mondo_parents.csv" \
  --drug_csv "./data/linked_to_ontologies/mapped_clinical_drug_data_with_umls_parents.csv" \
  --output_csv "./data/linked_to_ontologies/entities_drug_disease_clin.csv"
```