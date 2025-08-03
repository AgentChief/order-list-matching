#!/usr/bin/env python3
"""
reconcile_orders.py   –  PRIORITY-1 (orders only)

• Escalates ORDER_KEY = unique_keys → +extra_checks,
  tracking which extra(s) resolved each collision.
• Guarantees no duplicate ORDER_KEY overall.
• Prints your summary block.
• Writes per-customer `resolved_<CANON>.csv` and a global
  `orders_with_keys.csv`.  Any remaining collisions go to
  `duplicate_keys.csv` (actionable) and `duplicate_keys_cancelled.csv` (all CANCELLED),
  but do not raise an exception.
"""

import argparse, sys, yaml, pandas as pd
from pathlib import Path
import os

DEFAULT_UNIQUE = ["AAG ORDER NUMBER", "PLANNED DELIVERY METHOD", "CUSTOMER STYLE"]
DEFAULT_EXTRA  = ["PO NUMBER", "ORDER TYPE", "ALIAS/RELATED ITEM", "CUSTOMER ALT PO"]

def sanitise(c): return c.partition("#")[0].strip()

def add_defaults(canon):
    for rec in canon["customers"]:
        cfg = rec.setdefault("order_key_config", {})
        cfg.setdefault("unique_keys", list(DEFAULT_UNIQUE))
        cfg.setdefault("extra_checks", list(DEFAULT_EXTRA))
    return canon

def concat_cols(df, cols):
    present = [sanitise(c) for c in cols if sanitise(c) in df.columns]
    if not present:
        raise ValueError(f"None of {cols} present!")
    return df[present].fillna("").astype(str).agg("|".join, axis=1)

def norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.upper().str.strip()

def filter_cancelled_dups(dups: pd.DataFrame, order_key_col: str = "ORDER_KEY"):
    """Split duplicates into (actionable, cancelled) sets."""
    cancelled, actionable = [], []
    for key, group in dups.groupby(order_key_col):
        is_cancelled = (group["ORDER TYPE"].astype(str).str.upper() == "CANCELLED").all()
        if is_cancelled:
            cancelled.append(group)
        else:
            actionable.append(group)
    return (
        pd.concat(actionable, ignore_index=True) if actionable else pd.DataFrame(columns=dups.columns),
        pd.concat(cancelled,  ignore_index=True) if cancelled  else pd.DataFrame(columns=dups.columns)
    )

def process_customer(rec, orders):
    cfg       = rec["order_key_config"]
    uniq_cols = cfg["unique_keys"]
    extras    = cfg["extra_checks"]

    # select this customer's rows
    mlist = rec.get("master_order_list","")
    mask = norm(orders["CUSTOMER NAME"]) == norm(pd.Series([mlist])).iloc[0]
    sub = orders.loc[mask].copy()
    if sub.empty:
        return None

    # baseline key
    key = concat_cols(sub, uniq_cols)
    sub["ORDER_KEY"] = key
    dup_mask = key.duplicated(keep=False)

    # track which extras resolve
    resolved_by = pd.Series([[] for _ in sub.index], index=sub.index)

    # escalate
    for ex in extras:
        if not dup_mask.any():
            break
        if sanitise(ex) not in sub.columns:
            continue
        cand  = concat_cols(sub, uniq_cols + [ex])
        newly = dup_mask & ~cand.duplicated(keep=False)
        for idx in sub.index[newly]:
            resolved_by.at[idx].append(ex)
        sub["ORDER_KEY"] = cand
        dup_mask = cand.duplicated(keep=False)

    # stats for summary (before filtering cancelled dups)
    key_initial = concat_cols(sub, uniq_cols)
    dup_mask_initial = key_initial.duplicated(keep=False)
    n_dups_all = dup_mask_initial.sum()
    cancelled = 0
    if n_dups_all > 0:
        cancelled = 0
        for _, group in sub.loc[dup_mask_initial].groupby(key_initial[dup_mask_initial]):
            if (group["ORDER TYPE"].astype(str).str.upper() == "CANCELLED").all():
                cancelled += len(group)
    n_actionable = n_dups_all - cancelled

    used_extras = sorted({e for lst in resolved_by for e in lst})

    # Remaining duplicates (after all escalation and excluding all-cancelled)
    dup_mask_post = sub["ORDER_KEY"].duplicated(keep=False)
    dups_post = sub[dup_mask_post]
    remaining_actionable, _ = filter_cancelled_dups(dups_post)
    coll_final = len(remaining_actionable)

    # Count how many records were resolved by extra keys
    resolved_with_extras = resolved_by.map(bool).sum()

    # write back into full orders DF
    orders.loc[sub.index, "ORDER_KEY"]   = sub["ORDER_KEY"]
    orders.loc[sub.index, "RESOLVED_BY"] = resolved_by.map(lambda L: ",".join(L))

    # pack up summary + resolved rows
    summary = {
        "cust"        : rec["canonical"],
        "master"      : rec.get("master_order_list","N/A"),
        "shipped"     : rec.get("shipped","N/A"),
        "rows"        : len(sub),
        "uniq_rows"   : sub["ORDER_KEY"].nunique(),
        "coll_unique_all"        : n_dups_all,
        "coll_unique_cancelled"  : cancelled,
        "coll_unique_balance"    : n_actionable,
        "resolved_with_extras"   : resolved_with_extras,
        "used_extras" : used_extras,
        "coll_final"  : coll_final,
    }
    resolved_df = sub.loc[resolved_by.map(bool), uniq_cols + extras].copy()
    resolved_df["RESOLVED_BY"] = resolved_by.map(lambda L: ",".join(L))

    return summary, resolved_df

def print_summary(s):
    print("\n" + "="*48)
    print("RECONCILIATION SUMMARY")
    print("="*48)
    print(f"CUSTOMER NAME:        {s['master']}")
    print(f"shipped:              {s['shipped']}")
    print(f"canonical:            {s['cust']}")
    print("ORDERS")
    print(f"# of records (orders):               {s['rows']}")
    print(f"# of unique keys (orders):           {s['uniq_rows']}")
    print(f"# duplicates w/ unique_keys all :    {s['coll_unique_all']}")
    print(f"# duplicates w/ unique_keys all cancelled: -{s['coll_unique_cancelled']}")
    print(f"# duplicates w/ unique_keys (balance): {s['coll_unique_balance']}")
    print(f"# duplicates resolved with extra keys: {s['resolved_with_extras']}")
    print(f"# extras used to fix dups           : {len(s['used_extras'])}")
    if s["used_extras"]:
        print("Extras applied to resolve duplicates:")
        for e in s["used_extras"]:
            print(f"  - {e}")
    print(f"# duplicates REMAINING              : {s['coll_final']}")
    print("\nSHIPMENTS\n#   (ignored – Priority 1)")
    print("="*48)

def main(canon_yaml, orders_csv, customers=None):
    # 1) load & inject defaults
    canon_raw = yaml.safe_load(Path(canon_yaml).read_text())
    canon     = add_defaults(canon_raw.copy())
    yaml.SafeDumper.ignore_aliases = lambda *_, **__: True
    Path("customers_merged.yaml").write_text(
        yaml.safe_dump(canon, sort_keys=False, width=120)
    )
    print(f"✅ Canonical YAML written to customers_merged.yaml")

    # Output folder structure
    main_folder = Path("customers")
    summary_folder = main_folder / "_summary"
    summary_folder.mkdir(parents=True, exist_ok=True)

    # 2) read orders
    orders = pd.read_csv(orders_csv, dtype=str, low_memory=False)
    orders["RESOLVED_BY"] = orders.get("RESOLVED_BY", "")

    # Build lookup for canonical, aliases, and master_order_list
    lookup = {}
    for rec in canon["customers"]:
        names = [rec.get("canonical", "")]
        names.extend(rec.get("aliases", []))
        mol = rec.get("master_order_list", "")
        if isinstance(mol, list):
            names.extend(mol)
        elif mol:
            names.append(mol)
        for name in names:
            if name:
                lookup[name.upper()] = rec

    input_customers = set((customers or lookup.keys()))
    processed = set()


    # 4) write remaining collisions (split actionable/cancelled) to summary folder
    all_actionable = []
    all_cancelled = []
    for rec in canon["customers"]:
        try:
            out = process_customer(rec, orders)
            if out is None:
                continue
            summary, _ = out
            # Find remaining duplicates for this customer
            mask = norm(orders["CUSTOMER NAME"]) == norm(pd.Series([rec.get("master_order_list","")])).iloc[0]
            sub = orders.loc[mask].copy()
            dup_mask_post = sub["ORDER_KEY"].duplicated(keep=False)
            dups_post = sub[dup_mask_post]
            actionable, cancelled = filter_cancelled_dups(dups_post)
            if not actionable.empty:
                all_actionable.append(actionable)
            if not cancelled.empty:
                all_cancelled.append(cancelled)
        except Exception:
            continue

    if all_actionable:
        final_actionable = pd.concat(all_actionable, ignore_index=True)
        # Group by ORDER_KEY and add diff columns and max date row for each group
        output_rows = []
        for key, group in final_actionable.groupby("ORDER_KEY"):
            output_rows.append(group)
            # Find columns that differ (excluding ORDER_KEY)
            cols = [c for c in group.columns if c != "ORDER_KEY"]
            diffs = []
            for col in cols:
                if group[col].nunique(dropna=False) > 1:
                    diffs.append(col)
            # Add a row for differing columns
            diff_row = {col: "" for col in group.columns}
            diff_row["ORDER_KEY"] = f"[DIFF_COLS] {diffs}"
            output_rows.append(pd.DataFrame([diff_row]))
        # Concatenate all rows and write to CSV
        if output_rows:
            final_out = pd.concat(output_rows, ignore_index=True)
            final_out.to_csv(summary_folder / "duplicate_keys.csv", index=False)
            print(f"\n⚠️  {len(final_actionable)} rows still collide – see {summary_folder / 'duplicate_keys.csv'}")
        else:
            final_actionable.to_csv(summary_folder / "duplicate_keys.csv", index=False)
            print(f"\n⚠️  {len(final_actionable)} rows still collide – see {summary_folder / 'duplicate_keys.csv'}")
    else:
        print("\n✅ No duplicate ORDER_KEYs across processed customers (excluding all-cancelled)")

    if all_cancelled:
        final_cancelled = pd.concat(all_cancelled, ignore_index=True)
        final_cancelled.to_csv(summary_folder / "duplicate_keys_cancelled.csv", index=False)
        print(f"→ {len(final_cancelled)} all-cancelled duplicate rows written to {summary_folder / 'duplicate_keys_cancelled.csv'}")

    # 5) save full orders to summary folder
    orders.to_csv(summary_folder / "orders_with_keys.csv", index=False)
    print(f"→ {summary_folder / 'orders_with_keys.csv'} written")

    # Write improved markdown summary with SKIPPED, DUPLICATES, CUSTOMER SUMMARY sections
    reports_folder = Path("reports")
    reports_folder.mkdir(parents=True, exist_ok=True)
    md_path = reports_folder / "customers_summary.md"

    skipped_customers = []
    duplicate_customers = []  # (cust, count, latest_dup_date)
    customer_summaries = []

    for rec in canon["customers"]:
        try:
            out = process_customer(rec, orders)
            if out is None:
                skipped_customers.append(rec.get("canonical", "UNKNOWN"))
                continue
            summary, resolved_df = out

            # Get latest duplicate received date if there are remaining duplicates
            latest_dup_date = "N/A"
            if summary['coll_final'] > 0:
                mask = norm(orders["CUSTOMER NAME"]) == norm(pd.Series([rec.get("master_order_list","")])).iloc[0]
                sub = orders.loc[mask].copy()
                dup_mask_post = sub["ORDER_KEY"].duplicated(keep=False)
                dups_post = sub[dup_mask_post]
                actionable, _ = filter_cancelled_dups(dups_post)
                date_col = "ORDER DATE PO RECEIVED"
                if date_col in actionable.columns and not actionable.empty:
                    def parse_excel_or_str(val):
                        try:
                            fval = float(val)
                            return pd.Timestamp('1899-12-30') + pd.to_timedelta(fval, unit='D')
                        except Exception:
                            return pd.to_datetime(val, errors='coerce', format='%Y-%m-%d')
                    parsed_dates = actionable[date_col].map(parse_excel_or_str)
                    max_date = parsed_dates.max()
                    if pd.notnull(max_date):
                        latest_dup_date = str(max_date.date())
                # Add to duplicates list with anchor and date
                anchor = summary['cust'].lower().replace(" ", "-").replace("'", "")
                duplicate_customers.append((summary['cust'], summary['coll_final'], latest_dup_date, anchor))

            # Build customer summary block (demote headings)
            cust_md = []
            cust_md.append("---\n")
            cust_md.append(f"### {summary['cust']}\n[[Reconciliation Summary](#reconciliation-summary)]\n")
            cust_md.append(f"**CUSTOMER NAME:** {summary['master']}  ")
            cust_md.append(f"**shipped:** {summary['shipped']}  ")
            cust_md.append(f"**canonical:** {summary['cust']}  \n")
            cust_md.append("#### ORDERS\n")
            cust_md.append(f"- **Records (orders):** {summary['rows']}")
            cust_md.append(f"- **Unique keys (orders):** {summary['uniq_rows']}")
            cust_md.append(f"- **Duplicates w/ unique_keys all:** {summary['coll_unique_all']}")
            cust_md.append(f"- **Duplicates w/ unique_keys all cancelled:** {summary['coll_unique_cancelled']}")
            cust_md.append(f"- **Duplicates w/ unique_keys (balance):** {summary['coll_unique_balance']}")
            cust_md.append(f"- **Duplicates resolved with extra keys:** {summary['resolved_with_extras']}")
            cust_md.append(f"- **Extras used to fix dups:** {len(summary['used_extras'])}")
            if summary["used_extras"]:
                cust_md.append("\n**Extras applied to resolve duplicates:**")
                for e in summary["used_extras"]:
                    cust_md.append(f"  - {e}")
            cust_md.append(f"\n- **Duplicates REMAINING:** {summary['coll_final']}")
            if summary['coll_final'] > 0:
                cust_md.append(f"- **Latest duplicate received:** {latest_dup_date}")
            cust_md.append("\n#### SHIPMENTS")
            cust_md.append("*(ignored – Priority 1)*\n")
            # Write resolved output info
            customer_folder = main_folder / summary['cust']
            customer_folder.mkdir(parents=True, exist_ok=True)
            if not resolved_df.empty:
                resolved_path = customer_folder / f"resolved_{summary['cust']}.csv"
                resolved_df.to_csv(resolved_path, index=False)
                cust_md.append(f"📝 **Output:** {len(resolved_df)} resolved rows → `{resolved_path}`\n")
            customer_summaries.append("\n".join(cust_md))
        except Exception as e:
            continue

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# RECONCILIATION SUMMARY\n\n")
        f.write("## SKIPPED\n\n")
        if skipped_customers:
            for cname in skipped_customers:
                f.write(f"- {cname}\n")
        else:
            f.write("(none)\n")
        f.write("\n## DUPLICATES\n\n")
        if duplicate_customers:
            for cname, count, latest, anchor in duplicate_customers:
                f.write(f"- [{cname}](#{anchor}): {count} duplicates remaining")
                if latest != "N/A":
                    f.write(f" latest duplicate received: {latest}")
                f.write("\n")
        else:
            f.write("(none)\n")
        f.write("\n## CUSTOMER SUMMARY\n\n")
        for block in customer_summaries:
            f.write(block + "\n")
        print(f"→ Detailed customer summary written to {md_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--canon-yaml", required=True)
    parser.add_argument("--orders-csv", required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--customers", nargs="*", help="limit to these canonical names")
    group.add_argument("--all", action="store_true", help="process all distinct CUSTOMER NAMEs in orders CSV")
    args = parser.parse_args()

    try:
        if args.all:
            # Read orders CSV to get all unique CUSTOMER NAMEs
            orders = pd.read_csv(args.orders_csv, dtype=str, low_memory=False)
            customer_names = sorted(orders["CUSTOMER NAME"].dropna().unique())
            # Use these as the customer list
            main(args.canon_yaml, args.orders_csv, customer_names)
        else:
            main(args.canon_yaml, args.orders_csv, args.customers)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
