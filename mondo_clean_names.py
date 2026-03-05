#!/usr/bin/env python3
"""
If --clinical_input is a folder, merge all CSVs that start with "disease_" inside it
(also supports a file glob). Then run the MONDO parent-grouping pipeline.

Examples:

1) Single file in, single file out:
python mondo_clean_names.py \
  --clinical_input scratch/Clinical_Pipeline/data/linked_to_ontologies/full/clinical_disease_linked.csv \
  --clinical_output scratch/Clinical_Pipeline/data/linked_to_ontologies/full/clinical_disease_linked_parent.csv \
  --verbose

2) Folder of chunks in (merge disease_*.csv first), then output:
python mondo_clean_names.py \
  --clinical_input scratch/Clinical_Pipeline/data/linked_to_ontologies/chunks \
  --clinical_output scratch/Clinical_Pipeline/data/linked_to_ontologies/full/clinical_disease_linked_parent.csv \
  --prefix disease_ \
  --verbose

3) Glob of files in:
python mondo_clean_names.py \
  --clinical_input "scratch/.../chunks/disease_*.csv" \
  --clinical_output scratch/.../full/clinical_disease_linked_parent.csv \
  --verbose
"""

import argparse
import re
import glob
from pathlib import Path
import pandas as pd
import time


# -------------------------
# Core helpers (your code, unchanged)
# -------------------------

def build_disease_nodes(df, col="disease_term_mondo_norm"):
    diseases = (
        df[col]
        .fillna("")
        .astype(str)
        .str.split("|")
        .explode()
        .str.strip()
    )
    diseases = diseases[diseases != ""]
    diseases = diseases.drop_duplicates().reset_index(drop=True)
    return pd.DataFrame({"node_idx": range(len(diseases)), "node_name": diseases})


def group_similar_diseases(disease_nodes: pd.DataFrame):
    groups = []
    seen = set()
    idx2group = {}
    no = set()

    BAD_ROOTS = {
        "mild to moderate", "moderate to severe", "post", "late", "early",
        "acute", "chronic", "type", "risk"
    }

    TAIL_NOISE_RE = re.compile(r"\s*(?:and|or)\s*[\.\,\-:;]*\s*$", re.I)

    def strip_trailing_noise(name: str) -> str:
        name = TAIL_NOISE_RE.sub("", str(name).strip())
        name = re.sub(r"\s+", " ", name)
        return name

    def normalize_name_for_comparison(name: str) -> str:
        name = strip_trailing_noise(name)
        name = re.sub(r"[^\w\s]", "", name.lower()).strip()
        parts = name.split()
        if not parts:
            return ""
        last = parts[-1]
        if last.endswith("s") and len(last) > 3 and not last.endswith(("ss", "us", "is")):
            parts[-1] = last[:-1]
        return " ".join(parts)

    ROMAN_RE = r"(?:M{0,3}(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{0,3}))"

    def isroman(s: str) -> bool:
        return bool(re.fullmatch(ROMAN_RE, s))

    def issingleletter(s: str) -> bool:
        return len(s) == 1 and s.isalpha()

    def is_stage_token(tok: str) -> bool:
        tok = str(tok).strip()
        if not tok:
            return False
        if tok.isnumeric() or isroman(tok) or issingleletter(tok):
            return True
        if re.fullmatch(r"[A-Za-z]\d{1,3}[A-Za-z]?", tok):
            return True
        if re.fullmatch(r"\d{1,3}[A-Za-z]?", tok):
            return True
        return False

    def norm_root(s: str) -> str:
        s = re.sub(r"\s+", " ", s.lower()).strip()
        s = re.sub(r"[,\-\s]+$", "", s)
        return s

    EXTRACT_PATTERNS = [
        re.compile(r"^(?P<root>.+?)\s*,\s*(?:type\s+)?(?P<stage>[A-Za-z0-9]+|" + ROMAN_RE + r")$", re.I),
        re.compile(r"^(?P<root>.+?)\s+(?:type\s+)?(?P<stage>[A-Za-z0-9]+|" + ROMAN_RE + r")$", re.I),
    ]

    def extract_root_and_stage(name: str):
        name = strip_trailing_noise(name)
        for pat in EXTRACT_PATTERNS:
            m = pat.match(name)
            if m:
                root_orig = m.group("root").strip()
                stage = m.group("stage").strip()
                if is_stage_token(stage):
                    root_norm = norm_root(root_orig)
                    if root_norm in BAD_ROOTS:
                        return None, None, None
                    display_root = re.sub(r"[,\-\s]+$", "", root_orig).strip()
                    return root_norm, stage, display_root
        return None, None, None

    # exclusions
    for i in range(disease_nodes.shape[0]):
        i_name = str(disease_nodes.loc[i, "node_name"])
        i_idx = disease_nodes.loc[i, "node_idx"]
        for w in ["monosomy", "disomy", "trisomy", "trisomy/tetrasomy", "chromosome"]:
            if w.lower() in i_name.lower():
                no.add(i_idx)

    for i in range(disease_nodes.shape[0]):
        i_idx = disease_nodes.loc[i, "node_idx"]
        if i_idx in seen or i_idx in no:
            continue

        i_name = str(disease_nodes.loc[i, "node_name"])
        i_root_norm, _, i_display_root = extract_root_and_stage(i_name)
        if not i_root_norm:
            continue

        main_root_norm = i_root_norm
        main_display_root = i_display_root

        matches_idx = [i_idx]
        match_found = False

        for j in range(disease_nodes.shape[0]):
            j_idx = disease_nodes.loc[j, "node_idx"]
            if j_idx in no:
                continue
            j_name = str(disease_nodes.loc[j, "node_name"])
            j_root_norm, _, j_display_root = extract_root_and_stage(j_name)

            if j_root_norm and normalize_name_for_comparison(j_root_norm) == normalize_name_for_comparison(main_root_norm):
                matches_idx.append(j_idx)
                match_found = True
                if j_display_root and len(j_display_root) < len(main_display_root):
                    main_display_root = j_display_root

        if match_found:
            matches_idx = sorted(set(matches_idx))
            if len(matches_idx) <= 1:
                continue
            for x in matches_idx:
                seen.add(x)
                idx2group[x] = main_display_root
            groups.append((main_display_root, matches_idx))

    return {"groups": groups, "idx2group": idx2group, "seen": seen, "excluded": no}


MONDO_RE = re.compile(r"^MONDO:\d+$")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())

def build_name2group_and_group2id(
    df: pd.DataFrame,
    *,
    raw_col: str,
    id_col: str,
    verbose: bool = True,
    log_every: int = 10000,
):
    start_time = time.time()

    if verbose:
        print(f"[INFO] Building disease nodes from column: {raw_col}")
    disease_nodes = build_disease_nodes(df, col=raw_col)

    if verbose:
        print(f"[INFO] Total unique raw disease nodes: {len(disease_nodes):,}")
        print("[INFO] Grouping similar diseases...")

    grp = group_similar_diseases(disease_nodes)
    idx2group = grp["idx2group"]

    if verbose:
        print(f"[INFO] Grouped nodes: {len(grp['groups']):,}")
        print("[INFO] Building name → group mapping...")

    idx2name = dict(zip(disease_nodes["node_idx"], disease_nodes["node_name"]))
    name2group = {name: idx2group.get(idx, name) for idx, name in idx2name.items()}

    parent_id = {}
    first_valid_child = {}

    total_rows = len(df)

    if verbose:
        print(f"[INFO] Iterating over {total_rows:,} rows to build ID mappings...")

    for i, (_, row) in enumerate(df.iterrows(), start=1):

        if verbose and i % log_every == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            print(
                f"[PROGRESS] {i:,}/{total_rows:,} rows "
                f"({100*i/total_rows:.1f}%) | "
                f"{rate:,.0f} rows/sec | "
                f"elapsed: {elapsed/60:.1f} min"
            )

        raw_terms = [_norm(x) for x in str(row.get(raw_col, "")).split("|")]
        raw_ids = [x.strip() for x in str(row.get(id_col, "")).split("|")]

        if len(raw_terms) != len(raw_ids):
            if len(raw_ids) < len(raw_terms):
                raw_ids = raw_ids + ["-1"] * (len(raw_terms) - len(raw_ids))
            else:
                raw_ids = raw_ids[:len(raw_terms)]

        for term, tid in zip(raw_terms, raw_ids):
            if not term:
                continue
            group = _norm(name2group.get(term, term))

            if term == group and MONDO_RE.match(tid or "") and group not in parent_id:
                parent_id[group] = tid

            if MONDO_RE.match(tid or "") and group not in first_valid_child:
                first_valid_child[group] = tid

    if verbose:
        print("[INFO] Finalizing group → ID mapping...")

    group2id = {}
    for g in set(name2group.values()):
        g_norm = _norm(g)
        if g_norm in parent_id:
            group2id[g_norm] = parent_id[g_norm]
        elif g_norm in first_valid_child:
            group2id[g_norm] = first_valid_child[g_norm]
        else:
            group2id[g_norm] = "-1"

    if verbose:
        total_time = time.time() - start_time
        print(f"[DONE] Mapping built in {total_time/60:.2f} minutes")
        print(f"[DONE] Total parent IDs: {len(parent_id):,}")
        print(f"[DONE] Total groups: {len(group2id):,}")

    return name2group, group2id


def apply_grouping_and_ids(df: pd.DataFrame, name2group: dict, group2id: dict, *, raw_col: str, grouped_col: str, out_id_col: str):
    def remap_terms_pipe(raw_string: str) -> str:
        parts = [p.strip() for p in str(raw_string).split("|") if p.strip()]
        out, seen_local = [], set()
        for p in parts:
            g = name2group.get(p, p)
            if g not in seen_local:
                out.append(g)
                seen_local.add(g)
        return "|".join(out)

    def ids_for_grouped_pipe(grouped_string: str) -> str:
        parts = [p.strip() for p in str(grouped_string).split("|") if p.strip()]
        ids = [group2id.get(_norm(p), "-1") for p in parts]
        return "|".join(ids)

    df = df.copy()
    df[grouped_col] = df[raw_col].fillna("").apply(remap_terms_pipe)
    df[out_id_col] = df[grouped_col].apply(ids_for_grouped_pipe)
    return df


def unique_disease_count(df: pd.DataFrame, col: str) -> int:
    return (
        df[col]
        .fillna("")
        .astype(str)
        .str.split("|")
        .explode()
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )


def check_term_id_length_alignment(df, term_col, id_col):
    term_counts = df[term_col].fillna("").apply(lambda x: len([p for p in str(x).split("|") if p.strip()]))
    id_counts = df[id_col].fillna("").apply(lambda x: len([p for p in str(x).split("|") if p.strip()]))

    mismatched_mask = term_counts != id_counts
    mismatched = df.loc[mismatched_mask, [term_col, id_col]].copy()
    mismatched.insert(0, "row_index", mismatched.index)
    mismatched["term_count"] = term_counts[mismatched_mask].values
    mismatched["id_count"] = id_counts[mismatched_mask].values
    return mismatched


def process_dataset(df_full: pd.DataFrame, *, join_key: str, raw_col: str, id_col: str, grouped_col: str, out_id_col: str, verbose: bool):
    slim = df_full[[join_key, raw_col, id_col]].copy()

    name2group, group2id = build_name2group_and_group2id(slim, raw_col=raw_col, id_col=id_col)

    grouped = apply_grouping_and_ids(
        slim, name2group, group2id,
        raw_col=raw_col, grouped_col=grouped_col, out_id_col=out_id_col
    )

    n_before = unique_disease_count(grouped, raw_col)
    n_after = unique_disease_count(grouped, grouped_col)
    mismatches = check_term_id_length_alignment(grouped, grouped_col, out_id_col)

    if verbose:
        print(f"[{join_key}] unique before: {n_before}")
        print(f"[{join_key}] unique after : {n_after}")
        print(f"[{join_key}] reduced by  : {n_before - n_after}")
        print(f"[{join_key}] mismatches  : {len(mismatches)}")

    stats = {
        "unique_before": n_before,
        "unique_after": n_after,
        "reduction": n_before - n_after,
        "n_mismatches": len(mismatches),
    }
    return grouped, stats, mismatches


# -------------------------
# New: input loader (folder / glob / file)
# -------------------------
def load_clinical_input(path_or_glob: str, prefix: str, dtype=str) -> pd.DataFrame:
    """
    If path_or_glob is:
      - a directory: read all CSVs in it that start with prefix (default 'disease_')
      - a glob: read all matching CSVs
      - a file: read it directly
    Returns concatenated df (deduped by all columns).
    """
    p = Path(path_or_glob)

    if p.exists() and p.is_dir():
        files = sorted(p.glob(f"{prefix}*.csv"))
    elif any(ch in path_or_glob for ch in ["*", "?", "["]):
        files = [Path(x) for x in sorted(glob.glob(path_or_glob))]
    else:
        files = [p]

    if not files:
        raise FileNotFoundError(f"No input files found for: {path_or_glob} (prefix='{prefix}')")

    dfs = []
    for fp in files:
        df = pd.read_csv(fp, dtype=dtype)
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates()
    return out


def main():
    p = argparse.ArgumentParser(
        description="Group MONDO disease labels. If --clinical_input is a folder, merges disease_*.csv first."
    )

    p.add_argument("--clinical_input", required=True, help="File, folder, or glob.")
    p.add_argument("--clinical_output", required=True)

    p.add_argument("--prefix", default="disease_", help="Prefix for chunk files when clinical_input is a folder.")
    p.add_argument("--clinical_key", default="nct_id")

    p.add_argument("--raw_col", default="merged_mondo_label")
    p.add_argument("--id_col", default="merged_mondo_termid")

    p.add_argument("--grouped_col", default="disease_term_mondo_parent_clean")
    p.add_argument("--out_id_col", default="disease_termid_mondo_parent_clean")

    p.add_argument("--save_mismatches", default="", help="Optional folder to save mismatch CSVs.")
    p.add_argument("--verbose", action="store_true")

    args = p.parse_args()

    df_c = load_clinical_input(args.clinical_input, prefix=args.prefix, dtype=str)

    grouped_c, stats_c, mism_c = process_dataset(
        df_c,
        join_key=args.clinical_key,
        raw_col=args.raw_col,
        id_col=args.id_col,
        grouped_col=args.grouped_col,
        out_id_col=args.out_id_col,
        verbose=args.verbose,
    )

    # merge back into the full (merged) input df
    df_c_out = df_c.merge(
        grouped_c[[args.clinical_key, args.grouped_col, args.out_id_col]],
        on=args.clinical_key,
        how="left",
    )

    Path(args.clinical_output).parent.mkdir(parents=True, exist_ok=True)
    df_c_out.to_csv(args.clinical_output, index=False)

    if args.save_mismatches:
        outdir = Path(args.save_mismatches)
        outdir.mkdir(parents=True, exist_ok=True)
        mism_c.to_csv(outdir / "clinical_mondo_grouping_mismatches.csv", index=False)

    if args.verbose:
        print("[clinical] stats:", stats_c)


if __name__ == "__main__":
    main()