#!/usr/bin/env python3
"""
Order-key simulator & duplicate detector
=======================================

* Injects synthetic ACTIVE rows for orphaned CANCELLED rows
* Builds ORDER_KEY per-customer using YAML definitions
* Reports duplicates & suggests extra columns to resolve them
"""

import argparse, yaml, pandas as pd, sys
from pathlib import Path
from typing import List, Dict


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def load_rules(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def inject_missing_actives(df: pd.DataFrame) -> pd.DataFrame:
    """For each AAG ORDER NUMBER that has only CANCELLED rows,
    create a synthetic ACTIVE line (flagged via _simulated)."""
    cancelled_only = (
        df.groupby("AAG ORDER NUMBER")["ORDER TYPE"]
          .apply(lambda s: set(s) == {"CANCELLED"})
          .pipe(lambda s: s[s].index)
    )
    synthetics = (
        df[df["AAG ORDER NUMBER"].isin(cancelled_only)]
          .drop_duplicates("AAG ORDER NUMBER")
          .assign(**{
              "ORDER TYPE": "ACTIVE",
              "_simulated": True
          })
    )
    return pd.concat([df, synthetics], ignore_index=True)

def make_order_key(row: pd.Series, cols: List[str]) -> str:
    """Tuple-string so it’s hashable and easy to read in a csv dump."""
    return "|".join(str(row.get(c, "")).strip() for c in cols)

def duplicate_report(df: pd.DataFrame,
                     key_cols: List[str],
                     customer_name: str,
                     suggest_top_n: int = 3) -> pd.DataFrame:
    """Return duplicates + suggestion of extra columns that would break the tie."""
    df["ORDER_KEY"] = df.apply(lambda r: make_order_key(r, key_cols), axis=1)
    dups = df[df["ORDER_KEY"].duplicated(keep=False)].copy()

    if not dups.empty and suggest_top_n:
        # Heuristic: columns with highest cardinality _inside_ the dup set
        numeric_like = {"int64", "float64"}
        candidate_cols = [
            c for c in df.columns
            if c not in key_cols
            and df[c].dtype.name not in numeric_like
            and dups[c].nunique() > 1
        ]
        cardinality = (
            {c: dups[c].astype(str).nunique() for c in candidate_cols}
        )
        suggestions = sorted(cardinality, key=cardinality.get, reverse=True)[:suggest_top_n]
        print(f"\n⚠️  {customer_name}: {len(dups)} duplicate rows "
              f"({dups['ORDER_KEY'].nunique()} keys). "
              f"Top columns to break the tie → {suggestions or 'n/a'}",
              file=sys.stderr)
    return dups

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main(args):
    df = pd.read_csv(args.csv)
    rules = load_rules(args.yaml)

    if args.inject_missing_active:
        df = inject_missing_actives(df)

    all_dups = []

    for cust, rule in rules["customers"].items():
        cust_df = df[df["CUSTOMER NAME"].str.upper() == cust.upper()].copy()
        key_cols = rule["unique_keys"]

        # Optional: dynamically extend keys with extra_checks from YAML
        if "extra_checks" in rule:
            key_cols = key_cols + rule["extra_checks"]

        dups = duplicate_report(cust_df, key_cols, cust)
        if not dups.empty:
            all_dups.append(dups.assign(CUSTOMER_NAME=cust))

    if all_dups:
        dup_df = pd.concat(all_dups, ignore_index=True)
        if args.report_only_duplicates:
            dup_df.to_csv("duplicate_keys.csv", index=False)
            print("\n⚠️  Duplicate details written to duplicate_keys.csv")
        else:
            print("\n===== FULL DUPLICATE ROWS =====")
            print(dup_df.head(20).to_markdown())
    else:
        print("✅  No duplicate ORDER_KEYs with current YAML definitions!")

# ----------------------------------------------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Order-key simulator")
    p.add_argument("--csv", required=True, help="Source order CSV")
    p.add_argument("--yaml", required=True, help="Customer-rules YAML")
    p.add_argument("--inject-missing-active", action="store_true",
                   help="Create synthetic ACTIVE rows where only CANCELLED exist")
    p.add_argument("--report-only-duplicates", action="store_true",
                   help="Write duplicate rows to CSV instead of full dump")
    main(p.parse_args())
