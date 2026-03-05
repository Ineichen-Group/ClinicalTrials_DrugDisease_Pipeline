#!/usr/bin/env python3

import argparse
import os
import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Prepare AACT texts and split into sequential chunks for parallel inference."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV file"
    )

    parser.add_argument(
        "--output_dir",
        required=True,
        help="Directory where chunk CSV files will be saved"
    )

    parser.add_argument(
        "--num_chunks",
        type=int,
        default=10,
        help="Number of chunks to create (default: 10)"
    )

    args = parser.parse_args()

    print(f"\nReading: {args.input}")
    df = pd.read_csv(args.input)

    # Fill missing official titles
    df["study_official_title"] = df["study_official_title"].fillna(df["brief_title"])

    # Keep relevant columns and remove duplicates
    aact_data = (
        df[["nct_id", "study_official_title", "brief_summary"]]
        .drop_duplicates()
        .copy()
    )

    # Create combined text column
    aact_data["Text"] = (
        aact_data["study_official_title"].fillna("").astype(str)
        + " | "
        + aact_data["brief_summary"].fillna("").astype(str)
    )

    # Keep only Text column
    text_data = aact_data[["nct_id", "Text"]]

    total_rows = len(text_data)
    print(f"Total rows: {total_rows}")

    os.makedirs(args.output_dir, exist_ok=True)

    # Sequential split
    chunks = np.array_split(text_data, args.num_chunks)

    for i, chunk_df in enumerate(chunks):
        output_path = os.path.join(args.output_dir, f"chunk_{i:02d}.csv")
        print(f"Writing: {output_path} ({len(chunk_df)} rows)")
        chunk_df.to_csv(output_path, index=False)

    print("\nDone.\n")


if __name__ == "__main__":
    main()