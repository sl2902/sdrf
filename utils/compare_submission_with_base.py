import pandas as pd

metrics = pd.read_csv("/kaggle/input/datasets/laxmsun/sdrfref/detailed_evaluation_metrics.csv")
sub_df = pd.read_csv("/kaggle/working/submission.csv")

# F1=0 pairs = columns where ground truth has values but SampleSubmission has "Not Applicable"
zero_pairs = metrics[metrics["f1"] == 0]

filled = 0
still_empty = 0
for _, row in zero_pairs.iterrows():
    pxd = row["pxd"]
    col = row["AnnotationType"]
    if col not in sub_df.columns:
        still_empty += 1
        continue
    mask = sub_df["PXD"] == pxd
    if not mask.any():
        still_empty += 1
        continue
    vals = sub_df.loc[mask, col].unique()
    non_na = [v for v in vals if str(v).strip() not in ["Not Applicable", "nan", ""]]
    if non_na:
        filled += 1
        # Show what you're submitting
        print(f"  FILLED: {pxd} | {col} → {str(non_na[0])[:50]}")
    else:
        still_empty += 1
        print(f"  EMPTY:  {pxd} | {col}")

print(f"\n{'='*50}")
print(f"F1=0 pairs in baseline: {len(zero_pairs)}")
print(f"Your submission fills:  {filled}")
print(f"Still empty:            {still_empty}")
print(f"Coverage:               {filled}/{len(zero_pairs)} = {filled/len(zero_pairs):.1%}")