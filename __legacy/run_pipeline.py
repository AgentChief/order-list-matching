#!/usr/bin/env python3
import argparse

from reconcile_orders   import main as reconcile_orders
from reconcile_shipments import main as reconcile_shipments
from reconcile_packed    import main as reconcile_packed
from change_detection   import main as change_detection

# Use shared reconciliation utilities
from reconciliation_utils import (
    add_default_key_config,
    duplicate_key_check,
    reconcile_quantities
)
import yaml
from pathlib import Path

def main():
    p = argparse.ArgumentParser(description="Run our AAG pipeline steps")
    p.add_argument("--canon-yaml",  required=True, help="Canonical customers YAML")
    p.add_argument("--orders-csv",  required=True, help="Order lines CSV")
    p.add_argument("--ships-csv",     help="(Optional) Shipments CSV")
    p.add_argument("--packed-csv",    help="(Optional) Packed products CSV")
    p.add_argument("--customers", nargs="*", default=None,
                   help="(Optional) Limit to these canonical names")
    args = p.parse_args()


    # 1) Always run orders reconciliation
    reconcile_orders(
        canon_yaml=args.canon_yaml,
        orders_csv =args.orders_csv,
        customers  =args.customers
    )

    # 2) Conditionally run shipments (when provided)
    if args.ships_csv:
        print("\n▶️  Running shipments…")
        try:
            reconcile_shipments(
                canon_yaml=args.canon_yaml,
                ships_csv  =args.ships_csv,
                customers  =args.customers
            )
        except AssertionError as e:
            print(f"[WARN] {e}")
            # Print reconciliation summary for each customer even if error
            from reconciliation_utils import print_reconciliation_summary
            import pandas as pd
            canon = yaml.safe_load(Path(args.canon_yaml).read_text())
            canon = add_default_key_config(canon)
            orders = pd.read_csv(args.orders_csv, dtype=str, low_memory=False)
            ships = pd.read_csv(args.ships_csv, dtype=str, low_memory=False)
            customers = args.customers
            if customers:
                customers = [x.upper() for x in customers]
            for rec in canon["customers"]:
                cust = rec["canonical"]
                if customers and cust.upper() not in customers:
                    continue
                order_subset = orders[orders["CUSTOMER NAME"].str.upper() == cust.upper()]
                ship_subset = ships[ships["Customer"].str.upper() == cust.upper()]
                if order_subset.empty and ship_subset.empty:
                    continue
                try:
                    from reconciliation_utils import make_key
                    order_key_conf = rec.get("order_key_config", None)
                    order_key_cols = order_key_conf["unique_keys"] if order_key_conf else []
                    order_extra_cols = order_key_conf.get("extra_checks", []) if order_key_conf else []
                    order_cols = [c for c in order_key_cols + order_extra_cols if c in order_subset.columns]
                    if order_cols and not order_subset.empty:
                        if len(order_cols) == 1:
                            order_subset["KEY"] = order_subset[order_cols[0]].fillna("").astype(str)
                        else:
                            order_subset["KEY"] = order_subset[order_cols].fillna("").astype(str).agg("|".join, axis=1)
                    else:
                        order_subset["KEY"] = pd.Series([""] * len(order_subset), index=order_subset.index)
                    order_keys = set(order_subset[order_subset["ORDER TYPE"].str.upper() == "ACTIVE"]["KEY"]) if not order_subset.empty else set()
                    shipment_key_conf = rec.get("shipment_key_config", {})
                    ship_key_cols = shipment_key_conf.get("unique_keys") or []
                    shipment_extra_cols = shipment_key_conf.get("extra_checks", []) or []
                    ship_cols = [c for c in ship_key_cols + shipment_extra_cols if c in ship_subset.columns]
                    if ship_cols and not ship_subset.empty:
                        if len(ship_cols) == 1:
                            ship_subset["KEY"] = ship_subset[ship_cols[0]].fillna("").astype(str)
                        else:
                            ship_subset["KEY"] = ship_subset[ship_cols].fillna("").astype(str).agg("|".join, axis=1)
                    else:
                        ship_subset["KEY"] = pd.Series([""] * len(ship_subset), index=ship_subset.index)
                    ship_keys = set(ship_subset["KEY"]) if not ship_subset.empty else set()
                    col_map = shipment_key_conf.get("column_mapping", {})
                    used_order_keys = [k for k in col_map.keys() if k in order_subset.columns]
                    order_subset["SHIP_KEY"] = (
                        order_subset[used_order_keys].fillna("").astype(str).agg("|".join, axis=1)
                        if used_order_keys else ""
                    )
                    ship_key_fields = [col_map[k] for k in col_map.keys() if col_map[k] in ship_subset.columns]
                    ship_subset["SHIP_KEY"] = (
                        ship_subset[ship_key_fields].fillna("").astype(str).agg("|".join, axis=1)
                        if ship_key_fields else ""
                    )
                    order_ship_keys = set(order_subset[order_subset["ORDER TYPE"].str.upper() == "ACTIVE"]["SHIP_KEY"]) if not order_subset.empty else set()
                    shipment_ship_keys = set(ship_subset["SHIP_KEY"]) if not ship_subset.empty else set()
                    only_in_orders = order_keys - ship_keys
                    only_in_shipments = ship_keys - order_keys
                    only_in_order_ships = order_ship_keys - shipment_ship_keys
                    only_in_shipment_ships = shipment_ship_keys - order_ship_keys
                    print_reconciliation_summary(
                        canon, order_subset, ship_subset, order_keys, ship_keys, only_in_orders, only_in_shipments,
                        order_ship_keys, shipment_ship_keys, only_in_order_ships, only_in_shipment_ships
                    )
                except Exception as e2:
                    print(f"[WARN] Could not print reconciliation summary for {cust}: {e2}")
            raise
    else:
        print("\n⚠️  Skipping shipments step (no --ships-csv provided)")

    # 3) Conditionally run packed
    if args.packed_csv:
        print("\n▶️  Running packed…")
        reconcile_packed(
            canon_yaml=args.canon_yaml,
            packed_csv =args.packed_csv,
            customers  =args.customers
        )
    else:
        print("\n⚠️  Skipping packed step (no --packed-csv provided)")

    # 4) Always perform duplicate check and reconciliation summary if both orders and shipments are present
    canon = yaml.safe_load(Path(args.canon_yaml).read_text())
    canon = add_default_key_config(canon)
    import pandas as pd
    duplicate_key_check(pd.read_csv(args.orders_csv, dtype=str, low_memory=False), canon, customers_to_check=args.customers)
    if args.ships_csv:
        reconcile_quantities(args.orders_csv, args.ships_csv, canon, customers_to_check=args.customers)

        # Print reconciliation summary for each customer
        from reconciliation_utils import print_reconciliation_summary
        orders = pd.read_csv(args.orders_csv, dtype=str, low_memory=False)
        ships = pd.read_csv(args.ships_csv, dtype=str, low_memory=False)
        # Optional: Only process selected customers
        customers = args.customers
        if customers:
            customers = [x.upper() for x in customers]
        for rec in canon["customers"]:
            cust = rec["canonical"]
            if customers and cust.upper() not in customers:
                continue
            order_subset = orders[orders["CUSTOMER NAME"].str.upper() == cust.upper()]
            ship_subset = ships[ships["Customer"].str.upper() == cust.upper()]
            if order_subset.empty and ship_subset.empty:
                continue
            # Use the same logic as in print_reconciliation_summary
            # (You may want to refactor this to a utility function if not already)
            # For now, call the function directly
            # You may need to build the required key sets as in the original function
            # For simplicity, call with empty sets if data is missing
            try:
                from reconciliation_utils import make_key
                # Build keys for orders
                order_key_conf = rec.get("order_key_config", None)
                order_key_cols = order_key_conf["unique_keys"] if order_key_conf else []
                order_extra_cols = order_key_conf.get("extra_checks", []) if order_key_conf else []
                order_cols = [c for c in order_key_cols + order_extra_cols if c in order_subset.columns]
                order_subset["KEY"] = make_key(order_subset.fillna(""), order_cols) if order_cols else ""
                order_keys = set(order_subset[order_subset["ORDER TYPE"].str.upper() == "ACTIVE"]["KEY"]) if not order_subset.empty else set()
                # Build keys for shipments
                shipment_key_conf = rec.get("shipment_key_config", {})
                ship_key_cols = shipment_key_conf.get("unique_keys") or []
                shipment_extra_cols = shipment_key_conf.get("extra_checks", []) or []
                ship_cols = [c for c in ship_key_cols + shipment_extra_cols if c in ship_subset.columns]
                ship_subset["KEY"] = make_key(ship_subset.fillna(""), ship_cols) if ship_cols else ""
                ship_keys = set(ship_subset["KEY"]) if not ship_subset.empty else set()
                # SHIP_KEY logic (robust)
                col_map = shipment_key_conf.get("column_mapping", {})
                used_order_keys = [k for k in col_map.keys() if k in order_subset.columns]
                order_subset["SHIP_KEY"] = (
                    order_subset[used_order_keys].fillna("").astype(str).agg("|".join, axis=1)
                    if used_order_keys else ""
                )
                ship_key_fields = [col_map[k] for k in col_map.keys() if col_map[k] in ship_subset.columns]
                ship_subset["SHIP_KEY"] = (
                    ship_subset[ship_key_fields].fillna("").astype(str).agg("|".join, axis=1)
                    if ship_key_fields else ""
                )
                order_ship_keys = set(order_subset[order_subset["ORDER TYPE"].str.upper() == "ACTIVE"]["SHIP_KEY"]) if not order_subset.empty else set()
                shipment_ship_keys = set(ship_subset["SHIP_KEY"]) if not ship_subset.empty else set()
                only_in_orders = order_keys - ship_keys
                only_in_shipments = ship_keys - order_keys
                only_in_order_ships = order_ship_keys - shipment_ship_keys
                only_in_shipment_ships = shipment_ship_keys - order_ship_keys
                print_reconciliation_summary(
                    canon, order_subset, ship_subset, order_keys, ship_keys, only_in_orders, only_in_shipments,
                    order_ship_keys, shipment_ship_keys, only_in_order_ships, only_in_shipment_ships
                )
            except Exception as e:
                print(f"[WARN] Could not print reconciliation summary for {cust}: {e}")

if __name__=="__main__":
    main()
