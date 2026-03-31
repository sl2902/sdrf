"""
Submission builder patch for V2 per-file integration.

Usage in notebook:
    
    # 1. Load V1 caches and merge as usual
    merged = merge_results(gemini_results, gpt4o_results)
    
    # 2. Run per-file extraction
    from v2_perfile import *  # or from sdrf.v2_perfile import *
    from v2_pipeline import run_perfile_extraction
    
    perfile_results = await run_perfile_extraction(
        v1_results=gemini_results,  # use primary model's cache
        model=gemini_model,         # or gemini_flash for cheaper
        prompt_module=v2_perfile_module,
    )
    
    # 3. Build submission with per-file overrides
    sub_df = build_submission_v2(merged, perfile_results)
    sub_df.to_csv("/kaggle/working/submission.csv", index=False)
"""

import pandas as pd
from sdrf.normalization import normalize_value
from sdrf.submission import _build_global_modes, NEVER_GLOBAL, SAMPLE_SUB

_SKIP_COLS = {"comment[fractionidentifier]", "characteristics[biologicalreplicate]"}


def build_submission_v2(results: dict, perfile_results: dict, two_pass: bool = False) -> pd.DataFrame:
    """Build submission with V2 per-file overrides for fraction/replicate/label."""
    sub_df = pd.read_csv(SAMPLE_SUB)
    sub_df = sub_df.loc[:, ~sub_df.columns.str.match(r'^Unnamed')]

    # Reset all metadata columns
    for col in sub_df.columns:
        if col not in ["ID", "PXD", "Raw Data File", "Usage"]:
            sub_df[col] = "Not Applicable"

    global_modes, non_na_ratio = _build_global_modes(sub_df)
    col_map = {c.lower().strip(): c for c in sub_df.columns}

    for cache_key, result in results.items():
        if result.get("status") != "ok":
            continue

        pxd_id = cache_key.replace("_pass2", "")
        metadata = result["metadata"]
        if isinstance(metadata, list):
            metadata = metadata[0] if metadata else {}
        mask = sub_df["PXD"] == pxd_id

        if not mask.any():
            print(f"  Warning: {pxd_id} not in submission template")
            continue

        # Fill global metadata (skip fraction/replicate — handled by V2)
        for extracted_col, value in metadata.items():
            col_key = extracted_col.lower().strip()
            if col_key in _SKIP_COLS:
                continue
            if not value or str(value).strip().lower() in ["not applicable", "n/a", ""]:
                continue
            target = col_map.get(col_key)
            if not target:
                base = col_key.split(".")[0]
                target = col_map.get(base)
            if target and target in sub_df.columns:
                sub_df.loc[mask, target] = normalize_value(col_key, value)

        # Apply V2 per-file overrides
        per_file = perfile_results.get(pxd_id, {})
        frac_col = col_map.get("comment[fractionidentifier]")
        brep_col = col_map.get("characteristics[biologicalreplicate]")
        label_col = col_map.get("characteristics[label]")

        # Get fallback values from global extraction
        try:
            n_fractions = int(str(metadata.get("comment[fractionidentifier]", "1")).strip())
        except:
            n_fractions = 1
        try:
            n_replicates = int(str(metadata.get("characteristics[biologicalreplicate]", "1")).strip())
        except:
            n_replicates = 1

        indices = sub_df.index[mask].tolist()
        for rank, idx in enumerate(indices):
            raw_file = sub_df.at[idx, "Raw Data File"]
            overrides = per_file.get(raw_file, {})

            # Fraction
            if frac_col:
                if "fraction" in overrides:
                    sub_df.at[idx, frac_col] = overrides["fraction"]
                else:
                    sub_df.at[idx, frac_col] = str((rank % n_fractions) + 1)

            # Biological replicate
            if brep_col:
                if "replicate" in overrides:
                    sub_df.at[idx, brep_col] = overrides["replicate"]
                else:
                    sub_df.at[idx, brep_col] = str((rank % n_replicates) + 1)

            # Label (only override for multiplexed experiments)
            if label_col and "label" in overrides:
                sub_df.at[idx, label_col] = normalize_value("characteristics[label]", overrides["label"])

    # Global mode fallback
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

    # nanoESI default for IonizationType
    ioni_col = col_map.get("comment[ionizationtype]")
    if ioni_col:
        for pxd in sub_df["PXD"].unique():
            mask = sub_df["PXD"] == pxd
            ioni_vals = sub_df.loc[mask, ioni_col].unique()
            if all(str(v).strip() in ["Not Applicable", "nan", ""] for v in ioni_vals):
                sub_df.loc[mask, ioni_col] = "nanoESI"

    # Summary
    non_na = (sub_df.drop(columns=["ID", "PXD", "Raw Data File", "Usage"]) != "Not Applicable").sum().sum()
    print(f"Submission shape : {sub_df.shape}")
    print(f"Non-NA values    : {non_na}")
    return sub_df