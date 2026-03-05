#!/usr/bin/env python3

import argparse
import glob
import os
import re
from typing import Dict, Optional, Tuple, Any, List

import pandas as pd

from abbreviations import schwartz_hearst


# -----------------------------
# Text cleaning
# -----------------------------
def remove_spaces_around_apostrophe_and_dash(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace(" ' ", "'")
    text = text.replace("' s", "'s")
    text = text.replace(" - ", "-")
    text = text.replace("- ", "-")
    text = text.replace(" / ", "/")
    text = text.replace("( ", "(")
    text = text.replace(" )", ")")
    text = text.replace("[ ", "[")
    text = text.replace(" ]", "]")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# -----------------------------
# Abbreviations (Schwartz-Hearst)
# -----------------------------
def extract_abbreviation_definition_pairs(doc_text: str) -> Dict[str, str]:
    if not isinstance(doc_text, str) or not doc_text.strip():
        return {}
    return schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=doc_text)


# -----------------------------
# Annotation parsing
# -----------------------------
def safe_eval_annotation_list(annotation_list_str: str) -> Optional[list]:
    if not isinstance(annotation_list_str, str):
        return None
    s = annotation_list_str.strip()
    if not s or len(s) <= 2:
        return None
    try:
        return eval(s)
    except Exception:
        return None


def extract_unique_entities_from_annotations(
    ann_str: str,
    abbrev_pairs: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    Returns (unique_conditions_str, unique_interventions_str) as '|' separated strings.
    Only keeps entity_type in {'COND','DISEASE','DRUG'}.
    Applies abbreviation expansion if abbrev_pairs provided.
    """
    unique_conditions = set()
    unique_interventions = set()

    abbrev_pairs = abbrev_pairs or {}

    ann_list = safe_eval_annotation_list(ann_str)
    if not ann_list:
        return "", ""

    for ann in ann_list:
        # expected: (_, _, entity_type, entity_name)
        try:
            _, _, entity_type, entity_name = ann
        except Exception:
            continue

        if not isinstance(entity_name, str) or not entity_name:
            continue
        if entity_name.startswith("##"):
            continue
        if len(entity_name) == 1:
            continue

        # Abbreviation expansion
        if entity_name in abbrev_pairs:
            entity_name = abbrev_pairs[entity_name]
        elif entity_name.upper() in abbrev_pairs:
            entity_name = abbrev_pairs[entity_name.upper()]

        entity_name = entity_name.lower().strip()
        if not entity_name:
            continue

        if entity_type == "DISEASE" or entity_type == "COND":
            unique_conditions.add(entity_name)
        elif entity_type == "DRUG":
            unique_interventions.add(entity_name)

    return "|".join(sorted(unique_conditions)), "|".join(sorted(unique_interventions))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def infer_part_from_filename(path: str) -> Optional[str]:
    """
    Extracts the part token from filenames like:
      ..._part_00.csv  -> "00"
    Returns None if not found.
    """
    base = os.path.basename(path)
    m = re.search(r"_part_(\d+)", base)
    return m.group(1) if m else None


def process_one_part(
    pred_path: str,
    text_chunk_path: str,
    *,
    pred_col: str,
    text_col: str,
    id_col: Optional[str],
    out_dir: str,
    out_prefix: str,
    drop_empty: bool,
) -> str:
    pred_df = pd.read_csv(pred_path)
    text_df = pd.read_csv(text_chunk_path)

    if pred_col not in pred_df.columns:
        raise ValueError(f"Missing prediction column '{pred_col}' in: {pred_path}")
    if text_col not in text_df.columns:
        raise ValueError(f"Missing text column '{text_col}' in: {text_chunk_path}")

    # Clean prediction strings a bit (optional but helpful)
    pred_df[pred_col] = pred_df[pred_col].apply(remove_spaces_around_apostrophe_and_dash)

    # Align by row order (no shuffle assumption)
    if len(pred_df) != len(text_df):
        raise ValueError(
            f"Row count mismatch for part:\n"
            f"  predictions: {pred_path} -> {len(pred_df)} rows\n"
            f"  text chunk:  {text_chunk_path} -> {len(text_df)} rows\n"
            f"These must match if aligning by row order."
        )

    # Abbreviation pairs per row from Text
    abbrev_series = text_df[text_col].apply(extract_abbreviation_definition_pairs)

    # Extract entities row-wise
    def _extract(i: int) -> Tuple[str, str]:
        pairs = abbrev_series.iat[i]
        return extract_unique_entities_from_annotations(pred_df[pred_col].iat[i], pairs)

    extracted = [ _extract(i) for i in range(len(pred_df)) ]
    out_entities = pd.DataFrame(extracted, columns=["unique_conditions", "unique_interventions"])

    # Build output
    out_df = out_entities

    # Add id column if available (from predictions preferred; else from text chunk if exists)
    if id_col:
        if id_col in pred_df.columns:
            out_df.insert(0, id_col, pred_df[id_col].values)
        elif id_col in text_df.columns:
            out_df.insert(0, id_col, text_df[id_col].values)
        else:
            # fallback: add row_id so you can still trace back
            out_df.insert(0, "row_id", range(len(out_df)))
    else:
        out_df.insert(0, "row_id", range(len(out_df)))

    if drop_empty:
        out_df = out_df[
            (out_df["unique_conditions"].astype(str).str.len() > 0)
            | (out_df["unique_interventions"].astype(str).str.len() > 0)
        ].copy()

    ensure_dir(out_dir)
    pred_base = os.path.splitext(os.path.basename(pred_path))[0]
    out_path = os.path.join(out_dir, f"{out_prefix}_{pred_base}.csv")
    out_df.to_csv(out_path, index=False)
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Process chunked prediction files and matching text chunks (Text is in chunk_XX.csv, not in predictions)."
    )

    parser.add_argument(
        "--predictions_glob",
        required=True,
        help='Glob for prediction CSVs, e.g. "preds/test_annotated_*_part_*.csv"',
    )

    parser.add_argument(
        "--text_chunk_template",
        required=True,
        help=(
            "Path template for the matching text chunk file. Use {part} placeholder. "
            'Example: "scratch/.../chunks/chunk_{part}.csv"'
        ),
    )

    parser.add_argument(
        "--part",
        default=None,
        help="Optional part selector like 00, 01, ... If set, processes only that part.",
    )

    parser.add_argument(
        "--pred_col",
        default="ner_prediction_BioLinkBERT-base_normalized",
        help="Prediction column containing annotation tuples list as a string.",
    )

    parser.add_argument(
        "--text_col",
        default="Text",
        help="Text column name in the chunk file (default: Text).",
    )

    parser.add_argument(
        "--id_col",
        default=None,
        help="Optional ID column to include in output (e.g., nct_id or PMID). Looked up in predictions first, then chunk.",
    )

    parser.add_argument("--out_dir", required=True, help="Directory to write outputs.")
    parser.add_argument("--out_prefix", default="entities", help="Prefix for output files.")
    parser.add_argument(
        "--drop_empty",
        action="store_true",
        help="Drop rows where both unique_conditions and unique_interventions are empty.",
    )

    args = parser.parse_args()

    pred_files = sorted(glob.glob(args.predictions_glob))
    if not pred_files:
        raise FileNotFoundError(f"No files matched: {args.predictions_glob}")

    # Filter by --part if provided
    if args.part is not None:
        needle = f"_part_{args.part}"
        pred_files = [p for p in pred_files if needle in os.path.basename(p)]
        if not pred_files:
            raise FileNotFoundError(f"No prediction files matched part '{args.part}' (needle '{needle}').")

    ensure_dir(args.out_dir)

    print(f"Found {len(pred_files)} prediction file(s) to process.")

    for pred_path in pred_files:
        part = infer_part_from_filename(pred_path)
        if part is None:
            raise ValueError(f"Could not infer part from filename (expected _part_XX): {pred_path}")

        text_chunk_path = args.text_chunk_template.format(part=part)

        if not os.path.exists(text_chunk_path):
            raise FileNotFoundError(
                f"Text chunk file not found for part {part}:\n"
                f"  expected: {text_chunk_path}\n"
                f"  from template: {args.text_chunk_template}"
            )

        print(f"\nProcessing part {part}")
        print(f"  predictions: {pred_path}")
        print(f"  text chunk:  {text_chunk_path}")

        out_path = process_one_part(
            pred_path,
            text_chunk_path,
            pred_col=args.pred_col,
            text_col=args.text_col,
            id_col=args.id_col,
            out_dir=args.out_dir,
            out_prefix=args.out_prefix,
            drop_empty=args.drop_empty,
        )

        print(f"  wrote: {out_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()