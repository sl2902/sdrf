"""
Verify submission values against training data format.
Catches format mismatches betwen training SDRF and submission per column.

Usage:
    python verify-against-training.py --submission sub.csv --training-dir ./Training_SDRFs/HarmonizedFiles/

Shows: values that don't match any training format (potential F1=0).
"""

import argparse
import csv
import difflib
import os
import sys
from collections import defaultdict, Counter

# sys.stdout.reconfigure(encoding='utf-8')


def normalize(v):
    """Normalize value like the scorer does (extract NT= part)."""
    v = str(v).strip()
    if 'NT=' in v:
        parts = [r for r in v.split(';') if 'NT=' in r]
        return parts[0].replace('NT=', '').strip() if parts else v.strip()
    return v.strip()


def load_training(training_dir):
    """Load all unique normalized values per column from training SDRFs."""
    training = defaultdict(Counter)
    for fname in os.listdir(training_dir):
        if not fname.endswith('.csv'):
            continue
        with open(os.path.join(training_dir, fname), 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                for col, val in row.items():
                    if val and val not in ('Not Applicable', 'Text Span', ''):
                        training[col][normalize(val)] += 1
    return training


def main():
    parser = argparse.ArgumentParser(description="Verify submission against training format")
    parser.add_argument("--submission", "-s", required=True, help="Submission CSV")
    parser.add_argument("--training-dir", "-t", required=True, help="Directory with training Harmonized SDRFs")
    parser.add_argument("--threshold", type=float, default=0.80, help="Fuzzy match threshold")
    args = parser.parse_args()

    print("Loading training data...")
    training = load_training(args.training_dir)
    print(f"Loaded {len(training)} columns from training")

    print("Loading submission...")
    sub_vals = defaultdict(set)
    with open(args.submission, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            for col, val in row.items():
                if col in ('ID', 'PXD', 'Raw Data File', 'Usage'):
                    continue
                if val not in ('Not Applicable', 'not available', ''):
                    sub_vals[col].add(normalize(val))

    # Check each submission value against training
    mismatches = []
    matches = 0
    not_in_training = 0

    for col in sorted(sub_vals.keys()):
        train_vals = set(training.get(col, {}).keys())
        if not train_vals:
            for val in sub_vals[col]:
                not_in_training += 1
            continue

        for val in sub_vals[col]:
            # Find best match
            best_sim = 0
            best_match = ''
            for tv in train_vals:
                sim = difflib.SequenceMatcher(None, val, tv).ratio()
                if sim > best_sim:
                    best_sim = sim
                    best_match = tv

            if best_sim >= args.threshold:
                matches += 1
            elif best_sim > 0:
                mismatches.append((col, val, best_match, best_sim))
            else:
                not_in_training += 1

    # Report
    print(f"\n{'='*60}")
    print(f"FORMAT VERIFICATION REPORT")
    print(f"{'='*60}")
    print(f"Matches (>={args.threshold}): {matches}")
    print(f"Mismatches (<{args.threshold}): {len(mismatches)}")
    print(f"Not in training (new values): {not_in_training}")

    if mismatches:
        print(f"\nMISMATCHES (potential F1=0):")
        for col, val, best, sim in sorted(mismatches, key=lambda x: x[3]):
            print(f"  {sim:.3f} | {col}: {repr(val)[:30]} → best training: {repr(best)[:30]}")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    main()