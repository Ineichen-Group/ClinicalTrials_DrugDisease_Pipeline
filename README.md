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
First [./0_prepare_aact_texts.py](./0_prepare_aact_texts.py).

Then [.1_run_ner_drug_disease.sh](./1_run_ner_drug_disease.sh).


# NER cleaning

Total merged rows: 293,949
Rows with at least one prediction: 282,260 (filtered out 11,689 rows with no predictions)
Filled 19,063 empty pred_drug from AACT where intervention_types contains DRUG
Saved 64,772 rows with empty pred_drug to: ./data/aact_with_ner_for_linking/rows_with_empty_pred_drug.csv
Rows after filling pred_cond and dropping empty pred_drug: 217,488 (filled 23,857 pred_cond, dropped 64,772 rows with empty pred_drug)

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

```bash
sbatch 4_run_normalize_parallel.sh drug ner_predicted_drugs

sbatch 4_run_normalize_parallel.sh disease ner_predicted_conditions
```

# Mege and clean

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

# Mege final
```bash
python 9_join_drug_disease_entities.py \
  --disease_csv "./data/linked_to_ontologies/mapped_clinical_data_disease_cleaned_with_mondo_parents.csv" \
  --drug_csv "./data/linked_to_ontologies/mapped_clinical_drug_data_with_umls_parents.csv" \
  --output_csv "./data/linked_to_ontologies/entities_drug_disease_clin.csv"
```