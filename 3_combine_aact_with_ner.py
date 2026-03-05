#!/usr/bin/env python3
"""
Combine cleaned NER predictions, join with AACT export on nct_id, CLEAN entity strings,
and save ONLY drug/disease info as:
  - one full CSV (optional)
  - and/or chunked CSVs (chunk_00.csv ... chunk_09.csv)

Output columns (source-aware):
- nct_id
- aact_drug_names
- aact_condition_names
- ner_predicted_drugs
- ner_predicted_conditions
- merged_drug_names
- merged_condition_names

Example (write 10 chunks, no full file):
python combine_aact_with_ner_chunks.py \
  --aact_csv ./data/raw_aact/mv_interventional_drug_studies_20260221.csv \
  --predictions_glob "./data/processed_entities/entities_*_part_*.csv" \
  --out_dir ./data/aact_with_ner_for_linking \
  --num_chunks 10 \
  --write_full 0 \
  --pred_drug_col unique_interventions \
  --pred_cond_col unique_conditions

Example (write both full + chunks):
python combine_aact_with_ner_chunks.py \
  --aact_csv ./data/raw_aact/mv_interventional_drug_studies_20260221.csv \
  --predictions_glob "./data/processed_entities/entities_*_part_*.csv" \
  --out_dir ./data/aact_with_ner_for_linking \
  --out_name aact_drug_disease_with_ner_20260221.csv \
  --num_chunks 10 \
  --write_full 1 \
  --pred_drug_col unique_interventions \
  --pred_cond_col unique_conditions
"""

import argparse
import glob
import os
import re
from typing import List, Set

import numpy as np
import pandas as pd

SEP = " | "  # output separator


# -------------------------
# Cleaning logic
# -------------------------
_G_PER_KG = r"\b\d+(\.\d+)?\s*g\s*/\s*k[gG]\b"
_SPLIT_IN_ENTITY = re.compile(r"\s*(?:\+|\band\b|&)\s*", flags=re.IGNORECASE)


def split_pipe_list(s: object) -> List[str]:
    if not isinstance(s, str):
        return []
    s = s.strip()
    if not s:
        return []
    return [p.strip() for p in re.split(r"\s*\|\s*", s) if p.strip()]


def normalize_entity(ent: str) -> str:
    ent = ent.strip()
    ent = re.sub(r"\s+", " ", ent)
    return ent


def clean_one_entity(ent: str) -> List[str]:
    """
    Clean a single entity string and possibly split into multiple entities.
    - Split on '+', 'and', '&'
    - Remove %, g/kg, mg/g/etc dosages, and bare numbers
    """
    if not isinstance(ent, str):
        return []
    ent = ent.strip()
    if not ent:
        return []

    parts = [p.strip() for p in _SPLIT_IN_ENTITY.split(ent) if p.strip()]
    cleaned: List[str] = []

    for p in parts:
        p = re.sub(_G_PER_KG, "", p, flags=re.IGNORECASE)               # 0.25 g/Kg etc.
        p = re.sub(r"\b\d+(\.\d+)?\s*%\b", "", p)                      # 10%
        p = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|µg|ug|g|kg)\b", "", p, flags=re.IGNORECASE)  # 10 mg / 0.5 g
        p = re.sub(r"\b\d+(\.\d+)?\b", "", p)                          # any remaining numbers
        p = p.replace("()", " ").replace("[]", " ").replace("%", "")
        p = re.sub(r"\s+", " ", p).strip(" -:,;")

        p = normalize_entity(p)
        if p:
            cleaned.append(p)

    return cleaned


def clean_entity_list_string(s: object) -> str:
    """
    Takes a ' | '-separated entity string and returns a cleaned, deduped ' | '-separated string.
    """
    ents = split_pipe_list(s)

    out: List[str] = []
    seen: Set[str] = set()

    for ent in ents:
        for c in clean_one_entity(ent):
            key = c.lower().strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(c)

    return SEP.join(out)


def norm_token(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = x.strip()
    x = re.sub(r"\s+", " ", x)
    return x.lower()


def union_entities(a: object, b: object) -> str:
    a_parts = split_pipe_list(a)
    b_parts = split_pipe_list(b)

    seen: Set[str] = set()
    out: List[str] = []

    for item in a_parts + b_parts:
        key = norm_token(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item.strip())

    return SEP.join(out)


# -------------------------
# Reading predictions
# -------------------------
def read_predictions(predictions_glob: str, id_col: str, drug_col: str, cond_col: str) -> pd.DataFrame:
    files = sorted(glob.glob(predictions_glob))
    if not files:
        raise FileNotFoundError(f"No prediction files matched: {predictions_glob}")

    dfs = []
    for fp in files:
        df = pd.read_csv(fp)
        missing = [c for c in [id_col, drug_col, cond_col] if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns {missing} in prediction file: {fp}")
        dfs.append(df[[id_col, cond_col, drug_col]])

    pred = pd.concat(dfs, ignore_index=True)

    # Collapse duplicates per ID by concatenating strings, then we clean+dedupe later
    pred = (
        pred.groupby(id_col, as_index=False)
        .agg({
            cond_col: lambda s: SEP.join([x for x in s.astype(str) if x and x != "nan"]),
            drug_col: lambda s: SEP.join([x for x in s.astype(str) if x and x != "nan"]),
        })
    )

    # Normalize separators
    pred[cond_col] = pred[cond_col].apply(lambda x: SEP.join(split_pipe_list(x)))
    pred[drug_col] = pred[drug_col].apply(lambda x: SEP.join(split_pipe_list(x)))

    return pred


def write_chunks(df: pd.DataFrame, out_dir: str, num_chunks: int, prefix: str = "chunk_") -> None:
    chunks = np.array_split(df, num_chunks)
    for i, chunk_df in enumerate(chunks):
        out_path = os.path.join(out_dir, f"{prefix}{i:02d}.csv")
        chunk_df.to_csv(out_path, index=False)
        print(f"Wrote chunk: {out_path} ({len(chunk_df)} rows)")
        
def _is_empty(x) -> bool:
    """Treat NaN/None/empty/whitespace as empty."""
    if pd.isna(x):
        return True
    if isinstance(x, str) and x.strip() == "":
        return True
    return False

def main():
    p = argparse.ArgumentParser(
        description="Join AACT with cleaned NER predictions and save only drug/disease fields (optionally chunked)."
    )

    p.add_argument("--aact_csv", required=True, help="Path to AACT CSV (mv_interventional_drug_studies_*.csv).")
    p.add_argument("--predictions_glob", required=True, help="Glob for cleaned prediction CSVs to combine.")
    p.add_argument("--out_dir", required=True, help="Output directory.")

    # Full output file (optional)
    p.add_argument("--out_name", default="aact_drug_disease_with_ner.csv", help="Full output filename.")
    p.add_argument("--write_full", type=int, default=1, help="Write full CSV as well (1=yes, 0=no).")

    # Chunking
    p.add_argument("--num_chunks", type=int, default=10, help="Number of chunks to write (default: 10).")
    p.add_argument("--write_chunks", type=int, default=1, help="Write chunk CSVs (1=yes, 0=no).")
    p.add_argument("--chunks_subdir", default="chunks", help="Subdirectory name under out_dir for chunks.")

    p.add_argument("--id_col", default="nct_id", help="Join key column name (default: nct_id).")

    # Column names in AACT
    p.add_argument("--aact_drug_col", default="intervention_names", help="AACT drug column (default: intervention_names).")
    p.add_argument("--aact_cond_col", default="condition_names", help="AACT condition column (default: condition_names).")

    # Column names in predictions
    p.add_argument("--pred_drug_col", default="unique_interventions", help="Predicted drug column.")
    p.add_argument("--pred_cond_col", default="unique_conditions", help="Predicted condition column.")

    # Output column names (source-aware)
    p.add_argument("--out_aact_drug_col", default="aact_drug_names", help="Renamed AACT drug column in output.")
    p.add_argument("--out_aact_cond_col", default="aact_condition_names", help="Renamed AACT condition column in output.")
    p.add_argument("--out_pred_drug_col", default="ner_predicted_drugs", help="Renamed predicted drug column in output.")
    p.add_argument("--out_pred_cond_col", default="ner_predicted_conditions", help="Renamed predicted condition column.")
    p.add_argument("--out_merged_drug_col", default="merged_drug_names", help="Merged drug union column name.")
    p.add_argument("--out_merged_cond_col", default="merged_condition_names", help="Merged condition union column name.")

    # Cleaning toggle
    p.add_argument("--clean_entities", type=int, default=1, help="Apply entity cleaning (1=yes, 0=no).")

    args = p.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    full_out_path = os.path.join(args.out_dir, args.out_name)

    print(f"Reading AACT: {args.aact_csv}")
    aact = pd.read_csv(args.aact_csv)

    needed_aact = [args.id_col, args.aact_drug_col, args.aact_cond_col, 'intervention_types']
    missing_aact = [c for c in needed_aact if c not in aact.columns]
    if missing_aact:
        raise ValueError(f"Missing columns {missing_aact} in AACT CSV: {args.aact_csv}")

    # Keep ONLY needed AACT fields
    aact = aact[needed_aact].copy()

    print(f"Reading predictions: {args.predictions_glob}")
    pred = read_predictions(args.predictions_glob, args.id_col, args.pred_drug_col, args.pred_cond_col)

    print(f"AACT rows: {len(aact):,}")
    print(f"Unique predicted IDs: {pred[args.id_col].nunique():,}")

    print("Joining on nct_id...")
    merged = aact.merge(pred, on=args.id_col, how="left")

    # Rename source columns
    merged = merged.rename(columns={
        args.aact_drug_col: args.out_aact_drug_col,
        args.aact_cond_col: args.out_aact_cond_col,
        args.pred_drug_col: args.out_pred_drug_col,
        args.pred_cond_col: args.out_pred_cond_col,
    })

    print("Creating merged union columns...")
    merged[args.out_merged_drug_col] = merged.apply(
        lambda r: union_entities(r.get(args.out_aact_drug_col), r.get(args.out_pred_drug_col)),
        axis=1,
    )
    merged[args.out_merged_cond_col] = merged.apply(
        lambda r: union_entities(r.get(args.out_aact_cond_col), r.get(args.out_pred_cond_col)),
        axis=1,
    )

    # Optional: clean merged columns too (helpful if union introduces combos)
    if args.clean_entities == 1:
        merged[args.out_merged_drug_col] = merged[args.out_merged_drug_col].apply(clean_entity_list_string)
        merged[args.out_merged_cond_col] = merged[args.out_merged_cond_col].apply(clean_entity_list_string)

    # Keep only desired columns
    out_cols = [
        args.id_col,
        "intervention_types",
        args.out_aact_drug_col,
        args.out_aact_cond_col,
        args.out_pred_drug_col,
        args.out_pred_cond_col,
        args.out_merged_drug_col,
        args.out_merged_cond_col,
    ]
    merged = merged[out_cols]

    print(f"Total merged rows: {len(merged):,}")
    # Ensure we treat empty strings as missing for easier logic
    for col in [args.out_pred_drug_col, args.out_pred_cond_col]:
        merged[col] = merged[col].astype("string")  # keeps <NA> properly
        merged[col] = merged[col].str.strip()
        merged.loc[merged[col] == "", col] = pd.NA

    # 1) If BOTH predictions are empty -> filter out those nct_ids (rows)
    both_empty = merged[args.out_pred_drug_col].isna() & merged[args.out_pred_cond_col].isna()
    removed_no_preds = int(both_empty.sum())
    merged = merged.loc[~both_empty].copy()
    print(f"Rows with at least one prediction: {len(merged):,} (filtered out {removed_no_preds:,} rows with no predictions)")

    # 2) If pred_drug is empty but AACT intervention_types includes DRUG -> fill pred_drug with AACT drug names
    # Normalize intervention_types for safe contains
    merged["intervention_types"] = merged["intervention_types"].astype("string").fillna("").str.upper()
    drug_type_mask = merged["intervention_types"].str.contains(r"DRUG", na=False)
    fill_pred_drug_from_aact_mask = merged[args.out_pred_drug_col].isna() & drug_type_mask
    filled_pred_drug = int(fill_pred_drug_from_aact_mask.sum())

    merged.loc[fill_pred_drug_from_aact_mask, args.out_pred_drug_col] = merged.loc[
        fill_pred_drug_from_aact_mask, args.out_aact_drug_col
    ]

    print(f"Filled {filled_pred_drug:,} empty pred_drug from AACT where intervention_types contains DRUG")
    
    # 2.5) If pred_drug not empty but pred_cond empty -> fill pred_cond with AACT condition
    fill_mask = merged[args.out_pred_drug_col].notna() & merged[args.out_pred_cond_col].isna()
    filled_pred_cond = int(fill_mask.sum())
    merged.loc[fill_mask, args.out_pred_cond_col] = merged.loc[fill_mask, args.out_aact_cond_col]
    

    # 3) Drop remaining empty pred_drug
    drop_mask = merged[args.out_pred_drug_col].isna()
    dropped_empty_pred_drug = int(drop_mask.sum())
    # Save them separately 
    empty_pred_drug_df = merged.loc[drop_mask].copy()
    empty_out_path = os.path.join(args.out_dir, "rows_with_empty_pred_drug.csv")
    empty_pred_drug_df.to_csv(empty_out_path, index=False)

    print(f"Saved {dropped_empty_pred_drug:,} rows with empty pred_drug to: {empty_out_path}")

    # Now drop them

    merged = merged.loc[~drop_mask].copy()

    print(
        f"Rows after filling pred_cond and dropping empty pred_drug: {len(merged):,} "
        f"(filled {filled_pred_cond:,} pred_cond, dropped {dropped_empty_pred_drug:,} rows with empty pred_drug)"
    )
    # Write full CSV (optional)
    if args.write_full == 1:
        print(f"Writing full file: {full_out_path}")
        merged.to_csv(full_out_path, index=False)

    # Write chunks (optional)
    if args.write_chunks == 1:
        chunks_dir = os.path.join(args.out_dir, args.chunks_subdir)
        os.makedirs(chunks_dir, exist_ok=True)
        print(f"Writing {args.num_chunks} chunks to: {chunks_dir}")
        write_chunks(merged, chunks_dir, args.num_chunks, prefix="chunk_")

    has_pred = merged[args.out_pred_drug_col].notna() | merged[args.out_pred_cond_col].notna()
    print(f"Rows with any predictions attached: {int(has_pred.sum()):,} / {len(merged):,}")
    print("Done.")


if __name__ == "__main__":
    main()