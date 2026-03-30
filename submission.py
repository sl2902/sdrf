"""
Submission Builder
------------------
Reads extraction cache and builds the final submission CSV.

Usage:
    from sdrf.submission import build_submission
    sub_df = build_submission(results)
    sub_df.to_csv("/kaggle/working/submission.csv", index=False)
"""

import os
import pandas as pd
from .normalization import normalize_value

SAMPLE_SUB = "/kaggle/input/competitions/harmonizing-the-data-of-your-data/SampleSubmission.csv"
OUTPUT_DIR = "/kaggle/working"
EXT_DIR = "/kaggle/input/datasets/laxmsun/sdrfref"

# Columns assigned programmatically — skip from metadata loop
_SKIP_COLS = {"comment[fractionidentifier]", "characteristics[biologicalreplicate]"}


NEVER_GLOBAL = {
    "Characteristics[Age]", "Characteristics[AncestryCategory]",
    "Characteristics[Bait]", "Characteristics[CellLine]",
    "Characteristics[CellPart]", "Characteristics[Compound]",
    "Characteristics[Depletion]", "Characteristics[GrowthRate]",
    "Characteristics[PooledSample]", "Characteristics[SamplingTime]",
    "Characteristics[Strain]", "Characteristics[SyntheticPeptide]",
    "Characteristics[Temperature]", "Characteristics[Time]",
    "Characteristics[Treatment]", "Characteristics[TumorSize]",
    "Characteristics[TumorGrade]", "Characteristics[TumorStage]",
    "Characteristics[TumorCellularity]", "Characteristics[TumorSite]",
    "Characteristics[AnatomicSiteTumor]",
    "Characteristics[GeneticModification]", "Characteristics[Genotype]",
    "Characteristics[NumberOfBiologicalReplicates]",
    "Characteristics[NumberOfSamples]",
    "Characteristics[NumberOfTechnicalReplicates]",
    "Characteristics[OriginSiteDisease]",
    "Comment[CollisionEnergy]", "Comment[NumberOfFractions]",
    "Comment[EnrichmentMethod]",
    "Characteristics[Modification].3", "Characteristics[Modification].4",
    "Characteristics[Modification].5", "Characteristics[Modification].6",
    "Characteristics[Sex]", "Characteristics[DevelopmentalStage]",
    "Characteristics[Disease]", "Characteristics[OrganismPart]",
    "Characteristics[CellType]", "Characteristics[Specimen]",
    "Characteristics[SpikedCompound]",
}

HEDGE_COLS = {"characteristics[materialtype]", "characteristics[disease]",
                "characteristics[organismpart]", "characteristics[celltype]",
                "characteristics[cellline]", "comment[acquisitionmethod]"}

CONFIDENT_COLS = {
    "Characteristics[Organism]",
    "Comment[Instrument]", 
    "Characteristics[Label]",
    "Characteristics[CleavageAgent]",
    "Characteristics[Modification]",
    "Characteristics[Modification].1",
    "Characteristics[Modification].2",
    "Comment[FragmentationMethod]",
    "Comment[PrecursorMassTolerance]",
    "Comment[FragmentMassTolerance]",
}


# def _build_global_modes(sub_df: pd.DataFrame):
#     """Build global mode fallback from training.csv."""
#     from collections import Counter
#     TRAIN_CSV = "/kaggle/input/competitions/harmonizing-the-data-of-your-data/Training_SDRFs/HarmonizedFiles/training.csv"
#     train_df = pd.read_csv(TRAIN_CSV, low_memory=False, dtype=str)
#     n_train_pxds = train_df["PXD"].nunique()

#     NA_VALS = ["Not Applicable", "not applicable", "NA", "nan", "TextSpan", ""]

#     global_modes = {}
#     non_na_ratio = {}

#     for col in sub_df.columns:
#         if col in ["ID", "PXD", "Raw Data File", "Usage"]:
#             continue
#         if col not in train_df.columns:
#             global_modes[col] = "Not Applicable"
#             non_na_ratio[col] = 0
#             continue

#         # Mode from all rows
#         vals = train_df[col].dropna().astype(str)
#         vals = vals[~vals.isin(NA_VALS)]

#         # # Get per-PXD representative value (mode within each PXD)
#         # def pxd_mode(x):
#         #     clean = x[~x.isin(NA_VALS)].dropna()
#         #     if len(clean) == 0:
#         #         return None
#         #     return clean.mode().iloc[0]

#         # per_pxd = train_df.groupby("PXD")[col].agg(pxd_mode).dropna()
#         # counter = Counter(per_pxd.tolist())
#         # non_na_ratio[col] = len(per_pxd) / n_train_pxds

#         # Mode from all rows (captures most common value correctly)
#         counter = Counter(vals.tolist())

#         if counter:
#             global_modes[col] = counter.most_common(1)[0][0]
#         else:
#             global_modes[col] = "Not Applicable"
        
#         pxds_with_value = train_df[
#             train_df[col].notna() &
#             ~train_df[col].isin(NA_VALS)
#         ]["PXD"].nunique()
#         non_na_ratio[col] = pxds_with_value / n_train_pxds
        
        

#     del train_df
#     return global_modes, non_na_ratio
def _build_global_modes(sub_df: pd.DataFrame):
    """Build global mode fallback from training.csv."""
    from collections import Counter
    TRAIN_CSV = "/kaggle/input/competitions/harmonizing-the-data-of-your-data/Training_SDRFs/HarmonizedFiles/training.csv"
    train_df = pd.read_csv(TRAIN_CSV, low_memory=False, dtype=str)
    n_train_pxds = train_df["PXD"].nunique() if "PXD" in train_df.columns else 103
 
    global_modes = {}
    non_na_ratio = {}
    for col in sub_df.columns:
        if col in ["ID", "PXD", "Raw Data File", "Usage"]:
            continue
        if col in train_df.columns:
            vals = train_df[col].dropna().astype(str)
            vals = vals[~vals.isin(["Not Applicable", "not applicable", "NA", "nan", "TextSpan", ""])]
            counter = Counter(vals.tolist())
        else:
            counter = Counter()
        total = sum(counter.values())
        if total > 0:
            global_modes[col] = counter.most_common(1)[0][0]
            non_na_ratio[col] = total / n_train_pxds
        else:
            global_modes[col] = "Not Applicable"
            non_na_ratio[col] = 0
 
    del train_df
    return global_modes, non_na_ratio


def build_submission(results: dict, two_pass: bool = False) -> pd.DataFrame:
    sub_df = pd.read_csv(SAMPLE_SUB)
    sub_df = sub_df.loc[:, ~sub_df.columns.str.match(r'^Unnamed')]

    # Reset all metadata columns to Not Applicable
    for col in sub_df.columns:
        if col not in ["ID", "PXD", "Raw Data File", "Usage"]:
            sub_df[col] = "Not Applicable"

    # Build global mode fallback from training data
    global_modes, non_na_ratio = _build_global_modes(sub_df)

    # Case-insensitive column lookup
    col_map = {c.lower().strip(): c for c in sub_df.columns}

    for cache_key, result in results.items():
        if result.get("status") != "ok":
            continue

        # Resolve PXD id from cache key (strip _pass2 suffix if present)
        pxd_id = cache_key.replace("_pass2", "")

        metadata = result["metadata"]
        # Handle case where model returned a list of dicts
        if isinstance(metadata, list):
            if metadata:
                metadata = metadata[0]
            else:
                continue
        mask     = sub_df["PXD"] == pxd_id

        if not mask.any():
            print(f"  Warning: {pxd_id} not in submission template")
            continue

        # Extract fraction and replicate counts
        try:
            n_fractions = int(str(metadata.get("comment[fractionidentifier]", "1")).strip())
        except:
            n_fractions = 1
        try:
            n_replicates = int(str(metadata.get("characteristics[biologicalreplicate]", "1")).strip())
        except:
            n_replicates = 1

        # Fill metadata columns
        for extracted_col, value in metadata.items():
            col_key = extracted_col.lower().strip()
            if col_key in _SKIP_COLS:
                continue
            if not value or str(value).strip().lower() in ["not applicable", "n/a", ""]:
                continue

            target = col_map.get(col_key)
            if not target:
                # Try base key without .N suffix
                base = col_key.split(".")[0]
                target = col_map.get(base)
            if target and target in sub_df.columns:
                sub_df.loc[mask, target] = normalize_value(col_key, value)

        # # Fill metadata columns (hedge-aware)
        # hedged_cols = {}  # track which columns have hedged values
        # for extracted_col, value in metadata.items():
        #     col_key = extracted_col.lower().strip()
        #     if col_key in _SKIP_COLS:
        #         continue
        #     if not value or str(value).strip().lower() in ["not applicable", "n/a", ""]:
        #         continue

        #     target = col_map.get(col_key)
        #     if not target:
        #         base = col_key.split(".")[0]
        #         target = col_map.get(base)
        #     if target and target in sub_df.columns:
        #         str_value = str(value)
        #         if "|" in str_value:
        #             # Hedged value — store both for alternating assignment
        #             parts = str_value.split("|", 1)
        #             hedged_cols[target] = (
        #                 normalize_value(col_key, parts[0].strip()),
        #                 normalize_value(col_key, parts[1].strip()),
        #             )
        #         else:
        #             sub_df.loc[mask, target] = normalize_value(col_key, value)

        # # Apply hedged values — alternate across rows so both appear as unique values
        # indices = sub_df.index[mask].tolist()
        # for target, (val_a, val_b) in hedged_cols.items():
        #     if val_a == val_b:
        #         sub_df.loc[mask, target] = val_a
        #         continue
        #     for rank, idx in enumerate(indices):
        #         if rank == 0:
        #             sub_df.at[idx, target] = val_b  # put secondary value on first row
        #         else:
        #             sub_df.at[idx, target] = val_a  # primary on remaining rows
        
        # Default IonizationType if still empty
        ioni_col = col_map.get("comment[ionizationtype]")
        if ioni_col:
            ioni_vals = sub_df.loc[mask, ioni_col].unique()
            if all(str(v).strip() in ["Not Applicable", "nan", ""] for v in ioni_vals):
                sub_df.loc[mask, ioni_col] = "nanoESI"

        # Assign fractionidentifier and biologicalreplicate per row
        frac_col = col_map.get("comment[fractionidentifier]")
        brep_col = col_map.get("characteristics[biologicalreplicate]")
        indices  = sub_df.index[mask].tolist()
        for rank, idx in enumerate(indices):
            if frac_col:
                sub_df.at[idx, frac_col] = str((rank % n_fractions) + 1)
            if brep_col:
                sub_df.at[idx, brep_col] = str((rank % n_replicates) + 1)

        # # Assign per-file overrides, then fall back to modulo
        # per_file = result.get("per_file", {})
        # frac_col  = col_map.get("comment[fractionidentifier]")
        # brep_col  = col_map.get("characteristics[biologicalreplicate]")
        # label_col = col_map.get("characteristics[label]")
        # disease_col = col_map.get("characteristics[disease]")
 
        # indices = sub_df.index[mask].tolist()
        # for rank, idx in enumerate(indices):
        #     raw_file = sub_df.at[idx, "Raw Data File"]
        #     overrides = per_file.get(raw_file, {})
 
        #     # Fraction
        #     if frac_col:
        #         if "fraction" in overrides:
        #             sub_df.at[idx, frac_col] = overrides["fraction"]
        #         else:
        #             sub_df.at[idx, frac_col] = str((rank % n_fractions) + 1)
 
        #     # Biological replicate
        #     if brep_col:
        #         if "replicate" in overrides:
        #             sub_df.at[idx, brep_col] = overrides["replicate"]
        #         else:
        #             sub_df.at[idx, brep_col] = str((rank % n_replicates) + 1)
 
        #     # Label channel (only override if per-file has it)
        #     if label_col and "label" in overrides:
        #         sub_df.at[idx, label_col] = normalize_value("characteristics[label]", overrides["label"])
 
            # # Condition → disease (only override if per-file has it)
            # if disease_col and "condition" in overrides:
            #     condition = overrides["condition"]
            #     cond_lower = condition.lower()
            #     # Map common filename tokens to disease values
            #     if cond_lower in ("wt", "ctrl", "control", "normal", "healthy", "mock"):
            #         sub_df.at[idx, disease_col] = "normal"
            #     elif cond_lower in ("ko", "treated", "tumor", "disease", "infected"):
            #         # Keep the global disease value — condition just confirms it's a disease sample
            #         pass
            #     else:
            #         # Unknown condition — use as-is
            #         sub_df.at[idx, disease_col] = condition

    # Global mode fallback — fill remaining Not Applicable cells
    # Only for high-frequency columns (>75% of training papers have a value)
    # Never applied to study-specific columns
    for idx in sub_df.index:
        for col in sub_df.columns:
            if col in ["ID", "PXD", "Raw Data File", "Usage"]:
                continue
            if sub_df.at[idx, col] != "Not Applicable":
                continue
            if col in NEVER_GLOBAL:
                continue
            if non_na_ratio.get(col, 0) > 0.75:
                sub_df.at[idx, col] = global_modes[col]
    
    # # Unpoisoning — blank out columns where baseline was already perfect
    # metrics = pd.read_csv(f"{EXT_DIR}/detailed_evaluation_metrics.csv")
    # one_pairs = metrics[metrics["f1"] == 1.0]
    # safe_pairs = set()
    # for _, row in one_pairs.iterrows():
    #     safe_pairs.add((row["pxd"], row["AnnotationType"]))

    # for idx in sub_df.index:
    #     pxd = sub_df.at[idx, "PXD"]
    #     for col in sub_df.columns:
    #         if col in ["ID", "PXD", "Raw Data File", "Usage"]:
    #             continue
    #         if (pxd, col) in safe_pairs:
    #             sub_df.at[idx, col] = "Not Applicable"


    # Summary
    non_na = (sub_df.drop(columns=["ID", "PXD", "Raw Data File", "Usage"]) != "Not Applicable").sum().sum()
    print(f"Submission shape : {sub_df.shape}")
    print(f"Non-NA values    : {non_na}")
    return sub_df

def merge_results(*result_dicts) -> dict:
    """
    Merge multiple extraction result dicts.
    First dict is primary — subsequent dicts fill NA gaps.
    """
    merged = {}
    all_pxds = set()
    for r in result_dicts:
        all_pxds.update(r.keys())

    for pxd in all_pxds:
        # Find first successful result as base
        base = None
        for r in result_dicts:
            if r.get(pxd, {}).get("status") == "ok":
                base = r[pxd]
                break
        if not base:
            continue

        meta = base["metadata"]
        if isinstance(meta, list):
            meta = meta[0] if meta else {}
        merged_meta = dict(meta)

        # # Fill gaps from remaining dicts + hedge where models disagree
        # for r in result_dicts[1:]:
        #     entry = r.get(pxd, {})
        #     if entry.get("status") != "ok":
        #         continue
        #     entry_meta = entry["metadata"]
        #     if isinstance(entry_meta, list):
        #         entry_meta = entry_meta[0] if entry_meta else {}
        #     for col, val in entry_meta.items():
        #         if not val or str(val).lower() in ["not applicable", "n/a", ""]:
        #             continue
        #         existing = merged_meta.get(col, "Not Applicable")
        #         if str(existing).lower() in ["not applicable", "n/a", "", "none"]:
        #             # Gap fill as before
        #             merged_meta[col] = val
        #         elif col.split(".")[0] in HEDGE_COLS and existing != val:
        #             # Hedge: store both values pipe-separated
        #             if "|" not in str(existing):  # don't triple-hedge
        #                 merged_meta[col] = f"{existing}|{val}"

        # Fill gaps from remaining dicts
        for r in result_dicts[1:]:
            entry = r.get(pxd, {})
            if entry.get("status") != "ok":
                continue
            entry_meta = entry["metadata"]
            if isinstance(entry_meta, list):
                entry_meta = entry_meta[0] if entry_meta else {}
            for col, val in entry_meta.items():
                if merged_meta.get(col, "Not Applicable") in ["Not Applicable", "N/A", "", None]:
                    if val and str(val).lower() not in ["not applicable", "n/a", ""]:
                        merged_meta[col] = val

        # # Merge per_file from all sources
        # merged_per_file = {}
        # for r in result_dicts[1:]:
        #     entry = r.get(pxd, {})
        #     if entry.get("status") == "ok" and entry.get("per_file"):
        #         for fname, overrides in entry["per_file"].items():
        #             if fname not in merged_per_file:
        #                 merged_per_file[fname] = {}
        #             for k, v in overrides.items():
        #                 if k not in merged_per_file[fname]:
        #                     merged_per_file[fname][k] = v

        merged[pxd] = {
            "metadata": merged_meta,
            "raw_files": base["raw_files"],
            # "per_file": merged_per_file,
            "status": "ok",
        }

    return merged


def save_submission(results: dict, two_pass: bool = False, path: str = None) -> str:
    if path is None:
        path = os.path.join(OUTPUT_DIR, "submission.csv")
    sub_df = build_submission(results, two_pass=two_pass)
    sub_df.to_csv(path, index=False)

    # Validate
    assert not any(c.startswith("Unnamed") for c in sub_df.columns), "Unnamed columns found!"
    assert "PXD" in sub_df.columns, "PXD column missing!"
    assert sub_df["PXD"].notna().all(), "Null PXD values!"

    print(f"Saved to {path}")
    print(f"Columns : {len(sub_df.columns)} | Rows: {len(sub_df)}")
    print("Done ✓")
    return path