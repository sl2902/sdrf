from sdrf.submission import _build_global_modes, NEVER_GLOBAL
import pandas as pd

sub_df = pd.read_csv("/kaggle/input/competitions/harmonizing-the-data-of-your-data/SampleSubmission.csv")

global_modes, non_na_ratio = _build_global_modes(sub_df)

for col in sorted(global_modes.keys()):
    if col in ["ID", "PXD", "Raw Data File", "Usage"]:
        continue
    filled = non_na_ratio.get(col, 0) > 0.75 and col not in NEVER_GLOBAL
    ratio = non_na_ratio.get(col, 0)
    if filled:
        print(f"  FILLED:  {col} → {str(global_modes[col])[:60]}  ({ratio:.2f})")
    else:
        reason = "NEVER_GLOBAL" if col in NEVER_GLOBAL else f"low ratio ({ratio:.2f})"
        print(f"  EMPTY:   {col}  [{reason}]")