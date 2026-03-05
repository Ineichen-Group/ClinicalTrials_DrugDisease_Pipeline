#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--disease_csv", required=True)
    ap.add_argument("--drug_csv", required=True)
    ap.add_argument("--output_csv", required=True)
    ap.add_argument("--id_col", default="nct_id")
    args = ap.parse_args()

    dis = pd.read_csv(args.disease_csv, dtype=str)
    drg = pd.read_csv(args.drug_csv, dtype=str)

    # Drop unwanted columns (ignore if missing)
    dis = dis.drop(
        columns=[
            "linkbert_mondo_names",
            "disease_mondo_closest_3",
            "disease_mondo_cdist",
        ],
        errors="ignore",
    )

    drg = drg.drop(
        columns=[
            "linkbert_umls_names",
            "drug_umls_closest_3",
            "drug_umls_cdist",
        ],
        errors="ignore",
    )

    # Deduplicate
    dis = dis.drop_duplicates(subset=[args.id_col])
    drg = drg.drop_duplicates(subset=[args.id_col])

    # Check nct_id coverage
    dis_ids = set(dis[args.id_col].dropna())
    drg_ids = set(drg[args.id_col].dropna())

    if dis_ids != drg_ids:
        print(f"[WARN] nct_id sets differ:")
        print(f"  disease only: {len(dis_ids - drg_ids)}")
        print(f"  drug only:    {len(drg_ids - dis_ids)}")

    # Inner join to guarantee matching IDs
    merged = dis.merge(drg, on=args.id_col, how="inner")

    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output_csv, index=False)

    print(f"Saved: {args.output_csv}")
    print(f"Rows: {len(merged)} | Unique {args.id_col}: {merged[args.id_col].nunique()}")


if __name__ == "__main__":
    main()