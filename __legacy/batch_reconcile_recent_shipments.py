#!/usr/bin/env python3
"""
batch_reconcile_recent_shipments.py

- Loads shipments from FM_orders_shipped for the last 3 days
- Finds distinct customers and up to 2 POs per customer
- Runs reconcile_orders_enhanced.py for each (customer, PO) pair
- Outputs .md reports in ./reports for each reconciliation
"""

import pandas as pd
import subprocess
from datetime import datetime, timedelta
from utils.db_helper import run_query
import yaml
import os
import argparse

DAYS = 3
MAX_POS_PER_CUSTOMER = 2
REPORTS_DIR = './reports'


def main():
    parser = argparse.ArgumentParser(description='Batch reconcile recent shipments.')
    parser.add_argument('--date-from', default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date-to', default=None, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()

    print(f"Loading shipments from the last {DAYS} days...")
    shipments = run_query("SELECT * FROM FM_orders_shipped", db_key="orders")
    print("Columns in shipments DataFrame:", list(shipments.columns))
    if "Customer_PO" not in shipments.columns:
        print("[ERROR] 'Customer_PO' column not found in FM_orders_shipped. Available columns:", list(shipments.columns))
        print("Please check your database schema or update the script to use the correct PO column name.")
        return
    shipments["Shipped_Date"] = pd.to_datetime(shipments["Shipped_Date"], errors="coerce")
    # Date filtering
    if args.date_from:
        shipments = shipments[shipments["Shipped_Date"] >= pd.to_datetime(args.date_from)]
    if args.date_to:
        shipments = shipments[shipments["Shipped_Date"] <= pd.to_datetime(args.date_to)]
    if not args.date_from and not args.date_to:
        today = pd.Timestamp.today().normalize()
        min_date = today - pd.Timedelta(days=DAYS-1)
        shipments = shipments[shipments["Shipped_Date"] >= min_date]

    recent_shipments = shipments
    print(f"Found {len(recent_shipments)} recent shipment rows.")
    # Get distinct (Customer, Customer_PO) pairs (max 2 POs per customer)

    # Extract up to MAX_POS_PER_CUSTOMER POs per customer, robustly
    pairs = (
        recent_shipments.dropna(subset=["Customer", "Customer_PO"])
        .groupby("Customer", group_keys=False)
        .apply(lambda df: df[["Customer", "Customer_PO"]].drop_duplicates().head(MAX_POS_PER_CUSTOMER))
        .reset_index(drop=True)
    )
    print(f"Will process {len(pairs)} (customer, PO) pairs.")


    # Load canonical_customers.yaml once
    with open('canonical_customers.yaml', 'r') as f:
        canonical_data = yaml.safe_load(f)
    customer_configs = {c['canonical'].upper(): c for c in canonical_data.get('customers', [])}

    for idx, row in pairs.iterrows():
        customer = row["Customer"]
        po = row["Customer_PO"]
        print(f"→ Reconciling Customer: {customer}, PO: {po}")

        # Find canonical config for this customer (by alias or canonical)
        config = None
        for c in canonical_data.get('customers', []):
            aliases = [a.upper() for a in c.get('aliases', [])]
            if customer.upper() in aliases or customer.upper() == c.get('canonical', '').upper():
                config = c
                break
        shipment_key_config = config.get('shipment_key_config', {}) if config else {}
        unique_keys = shipment_key_config.get('unique_keys', [])
        extra_checks = shipment_key_config.get('extra_checks', [])

        # Run reconciliation
        try:
            cmd = [
                "python", "reconcile_orders_enhanced.py",
                "--customer", str(customer),
                "--po", str(po),
                "--fuzzy-threshold", "90",
                "--output-dir", REPORTS_DIR
            ]
            if args.date_from:
                cmd += ["--date-from", args.date_from]
            if args.date_to:
                cmd += ["--date-to", args.date_to]
            result = subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"  [ERROR] Failed for {customer} PO {po}: {e}")
            continue

        # Find the latest summary report for this customer/PO
        summary_files = [f for f in os.listdir(REPORTS_DIR) if f.startswith(f"{customer}_{po}_") and f.endswith("_summary.md")]
        if not summary_files:
            print(f"  [WARN] No summary report found for {customer} PO {po}")
            continue
        latest_summary = max(summary_files, key=lambda fn: os.path.getmtime(os.path.join(REPORTS_DIR, fn)))
        summary_path = os.path.join(REPORTS_DIR, latest_summary)

        # Parse summary report for confidence checks and failed values
        with open(summary_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        def extract_percent(line):
            import re
            m = re.search(r'(\d+\.?\d*)%', line)
            return float(m.group(1)) if m else None

        # Default to PASS
        style_pass = color_pass = delivery_pass = key_pass = True
        style_conf = color_conf = delivery_conf = key_conf = None
        failed_styles = []
        failed_colors = []
        failed_delivery = []
        failed_keys = []
        in_unmatched_styles = in_unmatched_colors = in_failed_delivery = False
        for idx, line in enumerate(lines):
            if 'Matched styles:' in line:
                style_conf = extract_percent(line)
                style_pass = style_conf is not None and style_conf >= 95
            if 'Matched style+color pairs:' in line or 'Matched exactly:' in line:
                color_conf = extract_percent(line)
                color_pass = color_conf is not None and color_conf >= 95
            if 'Rows with delivery method match:' in line:
                delivery_conf = extract_percent(line)
                delivery_pass = delivery_conf is not None and delivery_conf >= 95
            if 'Rows with full match:' in line:
                key_conf = extract_percent(line)
                key_pass = key_conf is not None and key_conf >= 95
            # Capture failed values
            if 'Unmatched styles:' in line:
                in_unmatched_styles = True
                continue
            if in_unmatched_styles and line.strip().startswith('- '):
                failed_styles.append(line.strip()[2:])
            elif in_unmatched_styles and not line.strip():
                in_unmatched_styles = False
            if 'No matches found:' in line:
                in_unmatched_colors = True
                continue
            if in_unmatched_colors and line.strip().startswith('- '):
                failed_colors.append(line.strip()[2:])
            elif in_unmatched_colors and not line.strip():
                in_unmatched_colors = False
            # Delivery method failures (if present)
            if 'Delivery method mismatches:' in line:
                in_failed_delivery = True
                continue
            if in_failed_delivery and line.strip().startswith('- '):
                failed_delivery.append(line.strip()[2:])
            elif in_failed_delivery and not line.strip():
                in_failed_delivery = False

        print(f"  [CHECK] Style match:   {'PASS' if style_pass else 'FAIL'} ({style_conf if style_conf is not None else 'N/A'}%)")
        print(f"  [CHECK] Color match:   {'PASS' if color_pass else 'FAIL'} ({color_conf if color_conf is not None else 'N/A'}%)")
        print(f"  [CHECK] Delivery method: {'PASS' if delivery_pass else 'FAIL'} ({delivery_conf if delivery_conf is not None else 'N/A'}%)")
        print(f"  [CHECK] Key match:     {'PASS' if key_pass else 'FAIL'} ({key_conf if key_conf is not None else 'N/A'}%)")
        if not (style_pass and color_pass and delivery_pass and key_pass):
            print(f"  [FAIL] {customer} PO {po} did not meet 95% confidence on all keys.")
            # Append failed values to markdown
            with open(summary_path, 'a', encoding='utf-8') as f:
                f.write('\n## Failed Checks\n')
                if not style_pass and failed_styles:
                    f.write(f"\n### Unmatched Styles\n- " + "\n- ".join(failed_styles) + "\n")
                if not color_pass and failed_colors:
                    f.write(f"\n### Unmatched Style+Color Pairs\n- " + "\n- ".join(failed_colors) + "\n")
                if not delivery_pass and failed_delivery:
                    f.write(f"\n### Delivery Method Mismatches\n- " + "\n- ".join(failed_delivery) + "\n")
        else:
            print(f"  [PASS] {customer} PO {po} met 95%+ confidence on all keys.")

        # Export matching ORDERS DataFrame for that PO
        from reconcile_orders_enhanced import build_order_query, run_query as rq
        order_query = build_order_query(customer, po)
        orders_df = rq(order_query, db_key='orders')
        orders_csv_path = os.path.join(REPORTS_DIR, f"{customer}_{po}_orders.csv")
        orders_df.to_csv(orders_csv_path, index=False, encoding='utf-8')
        print(f"  [INFO] Exported matching ORDERS DataFrame to {orders_csv_path}")

    print("\nAll reconciliations complete. Check the ./reports directory for outputs.")

if __name__ == "__main__":
    main()
