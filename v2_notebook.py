"""
Notebook integration — Run V2 per-file on top of V1 cached results.

Copy these cells into your Kaggle notebook.
"""

# ── Cell 1: Import and setup ──────────────────────────────────
# Copy v2_perfile.py, v2_pipeline.py, v2_submission.py to /kaggle/working/
# or into your sdrf/ package

import json
import v2_perfile
import v2_pipeline
from v2_submission import build_submission_v2
from sdrf.submission import merge_results

# Your existing model setup
from sdrf.models import GeminiClient
gemini = GeminiClient(...)  # your existing setup


# ── Cell 2: Load V1 caches ────────────────────────────────────
cache_dir = "/kaggle/working"

with open(f"{cache_dir}/extraction_cache_gemini_pro.json") as f:
    gemini_results = json.load(f)
with open(f"{cache_dir}/extraction_cache_gpt_4o.json") as f:
    gpt4o_results = json.load(f)

# Merge V1 results (Gemini primary, GPT fills gaps)
merged = merge_results(gemini_results, gpt4o_results)
print(f"V1 merged: {len(merged)} PXDs")


# ── Cell 3: Run V2 per-file extraction ────────────────────────
# This sends ONLY filenames + experiment context to Gemini
# No paper text = cheap token cost

perfile_results = await v2_pipeline.run_perfile_extraction(
    v1_results=gemini_results,      # use primary model's V1 cache
    model=gemini,                    # reuse same model
    prompt_module=v2_perfile,        # the per-file prompt module
    max_concurrent=2,
    cache_dir=cache_dir,
    chunk_size=40,
)


# ── Cell 4: Inspect V2 results ───────────────────────────────
for pxd, per_file in perfile_results.items():
    if per_file:
        print(f"\n{pxd}: {len(per_file)} files")
        for fname, overrides in list(per_file.items())[:3]:
            print(f"  {fname}: {overrides}")
        if len(per_file) > 3:
            print(f"  ... and {len(per_file) - 3} more")


# ── Cell 5: Build and save submission ─────────────────────────
sub_df = build_submission_v2(merged, perfile_results)
sub_df.to_csv(f"{cache_dir}/submission.csv", index=False)

# Validate
assert not any(c.startswith("Unnamed") for c in sub_df.columns)
assert "PXD" in sub_df.columns
assert sub_df["PXD"].notna().all()
print(f"Saved submission.csv")
print(f"Columns: {len(sub_df.columns)} | Rows: {len(sub_df)}")
print("Done ✓")