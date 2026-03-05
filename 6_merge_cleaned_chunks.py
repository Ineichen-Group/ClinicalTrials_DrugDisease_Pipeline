#!/usr/bin/env python3

import argparse
import glob
from pathlib import Path
import pandas as pd


def merge_files(files_glob: str, output_csv: str, id_col: str, dedupe_on_id: bool, label: str):
    files = sorted(glob.glob(files_glob))
    if not files:
        print(f"[WARN] No {label} files matched: {files_glob}")
        return

    print(f"\n=== MERGING {label.upper()} ===")
    print(f"Found {len(files)} files")

    dfs = [pd.read_csv(fp, dtype=str) for fp in files]
    merged = pd.concat(dfs, ignore_index=True)

    rows_before = len(merged)
    unique_before = merged[id_col].nunique() if id_col in merged.columns else "N/A"

    if dedupe_on_id and id_col in merged.columns:
        merged = merged.drop_duplicates(subset=[id_col], keep="first")

    rows_after = len(merged)
    unique_after = merged[id_col].nunique() if id_col in merged.columns else "N/A"

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_csv, index=False)

    print(f"Output written to: {output_csv}")
    print(f"Rows before dedupe: {rows_before}")
    print(f"Rows after dedupe : {rows_after}")
    print(f"Unique {id_col} before: {unique_before}")
    print(f"Unique {id_col} after : {unique_after}")


def main():
    parser = argparse.ArgumentParser(description="Merge disease and drug chunk outputs separately.")

    parser.add_argument("--disease_glob", required=True,
                        help="Glob pattern for disease chunks.")
    parser.add_argument("--drug_glob", required=True,
                        help="Glob pattern for drug chunks.")

    parser.add_argument("--disease_output", required=True,
                        help="Merged disease output CSV.")
    parser.add_argument("--drug_output", required=True,
                        help="Merged drug output CSV.")

    parser.add_argument("--id_col", default="nct_id",
                        help="ID column for optional deduplication.")
    parser.add_argument("--dedupe_on_id", action="store_true",
                        help="Drop duplicate IDs (keep first).")

    args = parser.parse_args()
    print("\n=== MERGE CLEANED CHUNKS ===")
    print(f"Disease glob: {args.disease_glob}")
    merge_files(args.disease_glob, args.disease_output, args.id_col, args.dedupe_on_id, "disease")
    print(f"\nDrug glob: {args.drug_glob}")
    merge_files(args.drug_glob, args.drug_output, args.id_col, args.dedupe_on_id, "drug")

    print("\nDone.")


if __name__ == "__main__":
    main()