#!/usr/bin/env python3

import argparse
import glob
from pathlib import Path
import pandas as pd


def main():
    p = argparse.ArgumentParser(
        description="Merge MONDO-cleaned chunk CSVs and print minimal stats."
    )

    p.add_argument("--input_glob", required=True,
                   help='Glob for cleaned chunk CSVs, e.g. "./.../*_mondo_cleaned.csv"')
    p.add_argument("--output_csv", required=True,
                   help="Path to write merged CSV.")
    p.add_argument("--id_col", default="nct_id",
                   help="ID column for optional deduplication.")
    p.add_argument("--dedupe_on_id", action="store_true",
                   help="If set, drop duplicate IDs (keep first).")

    args = p.parse_args()

    files = sorted(glob.glob(args.input_glob))
    if not files:
        raise FileNotFoundError(f"No files matched: {args.input_glob}")

    print(f"Found {len(files)} chunk files.")

    dfs = []
    for fp in files:
        df = pd.read_csv(fp, dtype=str)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    rows_before = len(merged)
    unique_ids_before = merged[args.id_col].nunique() if args.id_col in merged.columns else "N/A"

    if args.dedupe_on_id and args.id_col in merged.columns:
        merged = merged.drop_duplicates(subset=[args.id_col], keep="first")

    rows_after = len(merged)
    unique_ids_after = merged[args.id_col].nunique() if args.id_col in merged.columns else "N/A"

    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output_csv, index=False)

    print("-----")
    print(f"Rows before dedupe: {rows_before}")
    print(f"Rows after dedupe : {rows_after}")
    print(f"Unique IDs before: {unique_ids_before}")
    print(f"Unique IDs after : {unique_ids_after}")
    print("Merged file written to:", args.output_csv)
    print("Done.")


if __name__ == "__main__":
    main()