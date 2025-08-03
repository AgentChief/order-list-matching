#!/usr/bin/env python3
"""
summarise_shipments_vs_orders.py

- Connects to the database using db_helper.py (same as reconcile_order_list.py)
- Loads orders from ORDER_LIST (or ORDERS_UNIFIED) and shipments from FM_orders_shipped
- For each customer, for each shipped_date (last 3 days), aggregates shipments (excluding qty)
- Compares shipment keys to order keys (using YAML config for key columns)
- Produces a daily Markdown summary: one row per day per customer (total matched, total duplicates, total no match)
"""
import argparse, sys, yaml, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from utils.db_helper import run_query

def sanitise(c): return c.partition("#")[0].strip()

def norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.upper().str.strip()

def concat_cols(df, cols):
    present = [sanitise(c) for c in cols if sanitise(c) in df.columns]
    if not present:
        raise ValueError(f"None of {cols} present!")
    return df[present].fillna("").astype(str).agg("|".join, axis=1)

def add_defaults(canon):
    for rec in canon["customers"]:
        cfg = rec.setdefault("order_key_config", {})
        cfg.setdefault("unique_keys", ["AAG ORDER NUMBER", "PLANNED DELIVERY METHOD", "CUSTOMER STYLE"])
        cfg.setdefault("extra_checks", ["PO NUMBER", "ORDER TYPE", "ALIAS/RELATED ITEM", "CUSTOMER ALT PO"])
        ship_cfg = rec.setdefault("shipment_key_config", {})
        ship_cfg.setdefault("unique_keys", ["Customer_PO", "Shipping_Method", "Style", "Color"])
        ship_cfg.setdefault("extra_checks", ["Size", "Season", "shippingCountry", "CartonID"])
    return canon

def main(canon_yaml, days=3, output_md="shipments_vs_orders_summary.md"):
    # Load YAML and inject defaults
    canon_raw = yaml.safe_load(Path(canon_yaml).read_text())
    canon = add_defaults(canon_raw.copy())
    lookup = {rec["canonical"].upper(): rec for rec in canon["customers"]}


    # Load orders and shipments from DB
    orders = run_query("SELECT * FROM ORDERS_UNIFIED", db_key="orders")
    shipments = run_query("SELECT * FROM FM_orders_shipped", db_key="orders")

    # Only consider last N days
    today = pd.Timestamp.today().normalize()
    min_date = today - pd.Timedelta(days=days-1)
    shipments["Shipped_Date"] = pd.to_datetime(shipments["Shipped_Date"], errors="coerce")
    shipments = shipments[shipments["Shipped_Date"] >= min_date]

    # DEBUG: Show GREYSON orders and shipments DataFrames
    greyson_rec = None
    for rec in canon["customers"]:
        if rec["canonical"].upper() == "GREYSON":
            greyson_rec = rec
            break
    if greyson_rec:
        import os, yaml as pyyaml
        mlist = greyson_rec.get("master_order_list", "")
        order_mask = norm(orders["CUSTOMER NAME"]) == norm(pd.Series([mlist])).iloc[0]
        greyson_orders = orders.loc[order_mask].copy()
        shipped_name = greyson_rec.get("shipped", "")
        ship_mask = norm(shipments["Customer"]) == norm(pd.Series([shipped_name])).iloc[0]
        greyson_shipments = shipments.loc[ship_mask].copy()
        # Show unique_keys + extra_checks columns from YAML, plus shipped_date for shipments
        order_cols = [sanitise(c) for c in (greyson_rec["order_key_config"]["unique_keys"] + greyson_rec["order_key_config"].get("extra_checks", [])) if sanitise(c) in greyson_orders.columns]
        # For shipments, ignore Size in aggregation and display
        ship_keys = [sanitise(c) for c in greyson_rec["shipment_key_config"]["unique_keys"] if sanitise(c) in greyson_shipments.columns and sanitise(c).lower() != "size"]
        ship_extras = [sanitise(c) for c in greyson_rec["shipment_key_config"].get("extra_checks", []) if sanitise(c) in greyson_shipments.columns and sanitise(c).lower() != "size"]
        ship_cols = ship_keys + ship_extras
        # Always include Shipped_Date if present
        if "Shipped_Date" in greyson_shipments.columns and "Shipped_Date" not in ship_cols:
            ship_cols.append("Shipped_Date")
        # Aggregate shipments without Size
        group_fields = ship_keys.copy()
        if "Shipped_Date" in greyson_shipments.columns:
            group_fields.append("Shipped_Date")
        agg_dict = {col: 'first' for col in greyson_shipments.columns if col not in group_fields + ["Qty"]}
        if "Qty" in greyson_shipments.columns:
            agg_dict["Qty"] = "sum"
        greyson_shipments_agg = greyson_shipments.groupby(group_fields, dropna=False, as_index=False).agg(agg_dict)

        # Output folder for GREYSON
        out_dir = Path("customers/GREYSON")
        out_dir.mkdir(parents=True, exist_ok=True)
        # Save DataFrames
        greyson_orders.to_csv(out_dir / "orders_full.csv", index=False)
        greyson_shipments.to_csv(out_dir / "shipments_full.csv", index=False)
        greyson_shipments_agg.to_csv(out_dir / "shipments_agg.csv", index=False)

        # For testing: match on PO, exclude CANCELLED, and match on YAML keys
        summary_metrics = {}

        if "Customer_PO" in greyson_shipments.columns and "PO NUMBER" in greyson_orders.columns:
            customer_pos = set(greyson_shipments["Customer_PO"].dropna().unique())
            # Exclude CANCELLED
            not_cancelled = greyson_orders["ORDER TYPE"].astype(str).str.upper() != "CANCELLED" if "ORDER TYPE" in greyson_orders.columns else pd.Series([True]*len(greyson_orders))
            filtered_orders = greyson_orders.loc[not_cancelled].copy()
            matching_orders = filtered_orders[filtered_orders["PO NUMBER"].isin(customer_pos)]

            # Now match using YAML keys (unique_keys) between aggregated shipments and orders
            order_key_cols = [sanitise(c) for c in greyson_rec["order_key_config"]["unique_keys"] if sanitise(c) in filtered_orders.columns]
            ship_key_cols = [sanitise(c) for c in greyson_rec["shipment_key_config"]["unique_keys"] if sanitise(c).lower() != "size" and sanitise(c) in greyson_shipments_agg.columns]
            # Build keys for both
            filtered_orders = filtered_orders.copy()
            filtered_orders.loc[:, "ORDER_KEY"] = filtered_orders[order_key_cols].fillna("").astype(str).agg("|".join, axis=1)
            greyson_shipments_agg = greyson_shipments_agg.copy()
            greyson_shipments_agg.loc[:, "SHIP_KEY"] = greyson_shipments_agg[ship_key_cols].fillna("").astype(str).agg("|".join, axis=1)
            # Summarise matches
            order_keys_set = set(filtered_orders["ORDER_KEY"])
            ship_keys = greyson_shipments_agg["SHIP_KEY"]
            n_matched = sum(k in order_keys_set for k in ship_keys)
            n_total = len(ship_keys)
            n_no_match = n_total - n_matched
            n_dups = greyson_shipments_agg.duplicated(subset=ship_key_cols).sum()
            summary_metrics["Total rows"] = n_total
            summary_metrics["Total matched"] = n_matched
            summary_metrics["Total not matched"] = n_no_match
            summary_metrics["Total duplicates"] = int(n_dups)

            # Key field matches: for each key, count and %
            key_match_stats = {}
            for key in ["Customer_PO", "Style", "Color", "Shipping_Method"]:
                ship_col = key
                order_col = None
                # Map shipment key to order key using column_mapping if needed
                colmap = greyson_rec.get("column_mapping", {})
                for k, v in colmap.items():
                    if v == key:
                        order_col = k
                        break
                if ship_col in greyson_shipments_agg.columns and order_col and order_col in filtered_orders.columns:
                    ship_vals = set(norm(greyson_shipments_agg[ship_col]))
                    order_vals = set(norm(filtered_orders[order_col]))
                    n_key_match = len(ship_vals & order_vals)
                    pct = (n_key_match / max(1, len(ship_vals))) * 100
                    key_match_stats[key] = {"matches": n_key_match, "percent": round(pct, 1)}
            summary_metrics["Key field matches"] = key_match_stats

            # Output summary as CSV (row per metric)
            import csv
            with open(out_dir / "summary_metrics.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Metric", "Value"])
                for k, v in summary_metrics.items():
                    if k == "Key field matches":
                        for key, stats in v.items():
                            writer.writerow([f"{key} matches", stats["matches"]])
                            writer.writerow([f"{key} %", stats["percent"]])
                    else:
                        writer.writerow([k, v])

            # Output unmatched keys/files summary in YAML
            unmatched_keys = [k for k in ship_keys if k not in order_keys_set]
            unmatched_yaml = {
                "unmatched_shipments": unmatched_keys,
                "unmatched_count": len(unmatched_keys)
            }
            with open(out_dir / "unmatched_summary.yaml", "w", encoding="utf-8") as f:
                pyyaml.dump(unmatched_yaml, f, default_flow_style=False, allow_unicode=True)

        print(f"\nGREYSON DataFrames and summary written to {out_dir}")

    # Prepare summary rows
    summary_rows = []
    for rec in canon["customers"]:
        cust = rec["canonical"]
        # Orders for this customer
        mlist = rec.get("master_order_list", "")
        order_mask = norm(orders["CUSTOMER NAME"]) == norm(pd.Series([mlist])).iloc[0]
        orders_sub = orders.loc[order_mask].copy()
        order_keys = []
        if not orders_sub.empty:
            order_keys = set(concat_cols(orders_sub, rec["order_key_config"]["unique_keys"]))
        # Shipments for this customer
        shipped_name = rec.get("shipped", "")
        ship_mask = norm(shipments["Customer"]) == norm(pd.Series([shipped_name])).iloc[0]
        ships_sub = shipments.loc[ship_mask].copy()
        if ships_sub.empty:
            continue
        # Aggregate shipments by shipped_date and key columns (excluding qty)
        key_cols = rec["shipment_key_config"]["unique_keys"]
        group_fields = key_cols + ["Shipped_Date"]
        group_fields = [c for c in group_fields if c in ships_sub.columns]
        ships_sub = ships_sub.groupby(group_fields, dropna=False, as_index=False).first()
        # For each shipped_date, summarise
        for shipped_date, group in ships_sub.groupby("Shipped_Date"):
            group_keys = concat_cols(group, key_cols)
            # Duplicates: keys that appear more than once in this group
            dup_counts = group_keys.value_counts()
            n_dups = (dup_counts > 1).sum()
            # Matched: keys that exist in order_keys
            n_matched = sum(k in order_keys for k in group_keys)
            # No match: keys not in order_keys
            n_no_match = sum(k not in order_keys for k in group_keys)
            summary_rows.append({
                "Customer": cust,
                "Shipped_Date": shipped_date.date() if pd.notnull(shipped_date) else "N/A",
                "Total_Shipments": len(group_keys),
                "Matched": n_matched,
                "Duplicates": n_dups,
                "No_Match": n_no_match
            })
    # Write markdown summary
    with open(output_md, "w", encoding="utf-8") as f:
        f.write("# Shipments vs Orders Daily Summary (Last {} Days)\n\n".format(days))
        f.write("| Customer | Shipped Date | Total Shipments | Matched | Duplicates | No Match |\n")
        f.write("|----------|--------------|----------------|---------|------------|----------|\n")
        for row in summary_rows:
            f.write(f"| {row['Customer']} | {row['Shipped_Date']} | {row['Total_Shipments']} | {row['Matched']} | {row['Duplicates']} | {row['No_Match']} |\n")
    print(f"→ Markdown summary written to {output_md}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--canon-yaml", required=True)
    parser.add_argument("--days", type=int, default=3, help="Number of days to summarise (default: 3)")
    parser.add_argument("--output-md", default="shipments_vs_orders_summary.md", help="Output markdown file")
    args = parser.parse_args()
    main(args.canon_yaml, args.days, args.output_md)
