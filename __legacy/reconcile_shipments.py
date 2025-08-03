#!/usr/bin/env python3
"""
reconcile_shipments.py   –  PRIORITY-2 (shipments only)

• Escalates each customer’s SHIP_KEY from unique_keys → +extra_checks,
  tracking which extra(s) resolved each collision.
• Guarantees no duplicate SHIP_KEY overall per-customer.
• Prints a reconciliation summary.
• Writes per-customer `resolved_<CANON>_ship.csv` and a global
  `shipments_with_keys.csv`.
"""

import argparse
import sys
import yaml
import pandas as pd
from pathlib import Path

# defaults for shipment_key_config if missing in YAML
DEFAULT_SHIP_UNIQUE = ["Customer_PO", "Shipping_Method", "Style", "Color"]
DEFAULT_SHIP_EXTRA  = ["Size", "Season", "shippingCountry", "CartonID"]

def sanitise(c): 
    return c.partition("#")[0].strip()

def norm(col: pd.Series) -> pd.Series:
    return col.fillna("").astype(str).str.upper().str.strip()

def concat_cols(df, cols):
    present = [sanitise(c) for c in cols if sanitise(c) in df.columns]
    if not present:
        raise ValueError(f"None of {cols} present in shipments file!")
    return df[present].fillna("").astype(str).agg("|".join, axis=1)

def add_defaults(canon):
    for rec in canon["customers"]:
        cfg = rec.setdefault("shipment_key_config", {})
        cfg.setdefault("unique_keys", list(DEFAULT_SHIP_UNIQUE))
        cfg.setdefault("extra_checks", list(DEFAULT_SHIP_EXTRA))
    return canon

def process_customer(rec, ships):
    cfg       = rec["shipment_key_config"]
    uniq_cols = cfg["unique_keys"]
    extras    = cfg.get("extra_checks", [])
    if extras is None:
        extras = []

    # filter to this customer’s shipments
    shipped_name = rec.get("shipped","")
    mask = norm(ships["Customer"]) == norm(pd.Series([shipped_name])).iloc[0]
    sub = ships.loc[mask].copy()
    # Ignore rows where SIZE or Size == 'SMS' (case-insensitive column name)
    size_col = None
    for col in sub.columns:
        if col.strip().lower() == "size":
            size_col = col
            break
    if size_col:
        sub = sub[sub[size_col].astype(str).str.upper() != "SMS"]
    if sub.empty:
        return None

    # AGGREGATE: group by key fields + Shipped_Date, sum Qty, ignore CartonID and SKUBarcode
    group_fields = uniq_cols + ["Shipped_Date"]
    group_fields = [c for c in group_fields if c in sub.columns]
    agg_dict = {col: 'first' for col in sub.columns if col not in group_fields + ["CartonID", "SKUBarcode", "Qty"]}
    agg_dict["Qty"] = "sum"
    sub = sub.groupby(group_fields, dropna=False, as_index=False).agg(agg_dict)

    # baseline key
    key = concat_cols(sub, uniq_cols)
    sub["SHIP_KEY"] = key
    dup_mask = sub["SHIP_KEY"].duplicated(keep=False)

    # track which extras resolve each duplicate
    resolved_by = pd.Series([[] for _ in sub.index], index=sub.index)

    # escalate with extra checks
    for ex in extras:
        if not dup_mask.any():
            break
        if sanitise(ex) not in sub.columns:
            continue
        candidate = concat_cols(sub, uniq_cols + [ex])
        newly = dup_mask & ~candidate.duplicated(keep=False)
        for idx in sub.index[newly]:
            resolved_by.at[idx].append(ex)
        sub["SHIP_KEY"] = candidate
        dup_mask = candidate.duplicated(keep=False)

    # stats
    coll_unique = concat_cols(sub, uniq_cols).duplicated(keep=False).sum()
    coll_final  = dup_mask.sum()
    used_extras = sorted({e for lst in resolved_by for e in lst})

    # DO NOT assign back to ships.loc[sub.index, ...] after aggregation
    # ships.loc[sub.index, "SHIP_KEY"]    = sub["SHIP_KEY"]
    # ships.loc[sub.index, "RESOLVED_BY"] = resolved_by.map(lambda L: ",".join(L))

    # build summary + resolved subset
    summary = {
        "cust"        : rec["canonical"],
        "shipped"     : rec.get("shipped","N/A"),
        "rows"        : len(sub),
        "uniq_rows"   : sub["SHIP_KEY"].nunique(),
        "coll_unique" : coll_unique,
        "used_extras" : used_extras,
        "coll_final"  : coll_final,
    }
    resolved_df = (
        sub.loc[resolved_by.map(bool), uniq_cols + extras]
           .assign(RESOLVED_BY=resolved_by.map(lambda L: ",".join(L)))
    )
    return summary, resolved_df

def print_summary(s):
    print("\n" + "="*48)
    print("SHIPMENTS RECONCILIATION SUMMARY")
    print("="*48)
    print(f"CUSTOMER NAME:        {s['cust']}")
    print(f"shipped key prefix:   {s['shipped']}")
    print(f"# of records (shipments):    {s['rows']}")
    print(f"# of unique SHIP_KEYs:       {s['uniq_rows']}")
    print(f"# duplicates w/ unique_keys: {s['coll_unique']}")
    print(f"# extras used to fix dups:    {len(s['used_extras'])}")
    if s["used_extras"]:
        print("Extras applied to resolve duplicates:")
        for e in s["used_extras"]:
            print(f"  - {e}")
    print(f"# duplicates REMAINING:       {s['coll_final']}")
    print("="*48)

def main(canon_yaml, ships_csv, customers=None):
    # load & inject YAML defaults
    canon_raw = yaml.safe_load(Path(canon_yaml).read_text())
    canon     = add_defaults(canon_raw.copy())
    Path("customers_merged.yaml").write_text(
        yaml.safe_dump(canon, sort_keys=False, width=120)
    )
    print(f"✅ Canonical YAML written to customers_merged.yaml")

    ships = pd.read_csv(ships_csv, dtype=str, low_memory=False)
    ships["RESOLVED_BY"] = ships.get("RESOLVED_BY","")
    # Remove all rows where SIZE or Size == 'SMS' (case-insensitive column name)
    size_col = None
    for col in ships.columns:
        if col.strip().lower() == "size":
            size_col = col
            break
    if size_col:
        ships = ships[ships[size_col].astype(str).str.upper() != "SMS"]

    lookup = {rec["canonical"].upper(): rec for rec in canon["customers"]}
    any_remaining = False
    all_dups = []

    for cust in (customers or lookup.keys()):
        rec = lookup.get(cust.upper())
        if not rec:
            print(f"[SKIP] no match for '{cust}'")
            continue
        out = process_customer(rec, ships)
        if out is None:
            print(f"[SKIP] no shipments for {rec['canonical']}")
            continue

        summary, resolved_df = out
        print_summary(summary)

        fn = f"resolved_{rec['canonical'].replace(' ','_')}_ship.csv"
        resolved_df.to_csv(fn, index=False)
        print(f"  → wrote {len(resolved_df)} resolved rows to {fn}")

        # collect any still-duplicated rows from the full aggregated sub DataFrame
        # Re-run process_customer to get the full sub DataFrame
        # (or refactor process_customer to return sub as well)
        # For now, re-run aggregation logic here for clarity
        shipped_name = rec.get("shipped","")
        mask = norm(ships["Customer"]) == norm(pd.Series([shipped_name])).iloc[0]
        sub = ships.loc[mask].copy()
        size_col = None
        for col in sub.columns:
            if col.strip().lower() == "size":
                size_col = col
                break
        if size_col:
            sub = sub[sub[size_col].astype(str).str.upper() != "SMS"]
        if not sub.empty:
            group_fields = rec['shipment_key_config']['unique_keys'] + ["Shipped_Date"]
            group_fields = [c for c in group_fields if c in sub.columns]
            agg_dict = {col: 'first' for col in sub.columns if col not in group_fields + ["CartonID", "SKUBarcode", "Qty"]}
            agg_dict["Qty"] = "sum"
            sub = sub.groupby(group_fields, dropna=False, as_index=False).agg(agg_dict)
            key = concat_cols(sub, rec['shipment_key_config']['unique_keys'])
            sub["SHIP_KEY"] = key
            dups = sub[sub["SHIP_KEY"].duplicated(keep=False)]
            if not dups.empty:
                any_remaining = True
                all_dups.append(dups)

    if any_remaining:
        final = pd.concat(all_dups, ignore_index=True)
        final.to_csv("duplicate_keys_ship.csv", index=False)
        raise AssertionError(f"🚨 {len(final)} rows still collide – see duplicate_keys_ship.csv")
    else:
        print("\n✅ No duplicate SHIP_KEYs across processed customers")

    ships.to_csv("shipments_with_keys.csv", index=False)
    print("→ shipments_with_keys.csv written")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--canon-yaml", required=True)
    p.add_argument("--ships-csv", required=True)
    p.add_argument("--customers", nargs="*", help="limit to these canonical names")
    args = p.parse_args()
    try:
        main(args.canon_yaml, args.ships_csv, args.customers)
    except AssertionError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
