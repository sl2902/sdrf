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

# Columns assigned programmatically — skip from metadata loop
_SKIP_COLS = {"comment[fractionidentifier]", "characteristics[biologicalreplicate]"}


def build_submission(results: dict, two_pass: bool = False) -> pd.DataFrame:
    sub_df = pd.read_csv(SAMPLE_SUB)
    sub_df = sub_df.loc[:, ~sub_df.columns.str.match(r'^Unnamed')]

    # Reset all metadata columns to Not Applicable
    for col in sub_df.columns:
        if col not in ["ID", "PXD", "Raw Data File", "Usage"]:
            sub_df[col] = "Not Applicable"

    # Case-insensitive column lookup
    col_map = {c.lower().strip(): c for c in sub_df.columns}

    for cache_key, result in results.items():
        if result.get("status") != "ok":
            continue

        # Resolve PXD id from cache key (strip _pass2 suffix if present)
        pxd_id = cache_key.replace("_pass2", "")

        metadata = result["metadata"]
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

        # Assign fractionidentifier and biologicalreplicate per row
        frac_col = col_map.get("comment[fractionidentifier]")
        brep_col = col_map.get("characteristics[biologicalreplicate]")
        indices  = sub_df.index[mask].tolist()
        for rank, idx in enumerate(indices):
            if frac_col:
                sub_df.at[idx, frac_col] = str((rank % n_fractions) + 1)
            if brep_col:
                sub_df.at[idx, brep_col] = str((rank % n_replicates) + 1)

    # Summary
    non_na = (sub_df.drop(columns=["ID", "PXD", "Raw Data File", "Usage"]) != "Not Applicable").sum().sum()
    print(f"Submission shape : {sub_df.shape}")
    print(f"Non-NA values    : {non_na}")
    return sub_df


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
