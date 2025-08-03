import sys
import pandas as pd

# ---- Default key config for all customers ----
DEFAULT_UNIQUE = [
    "AAG ORDER NUMBER",
    "PLANNED DELIVERY METHOD",
    "CUSTOMER STYLE",
]
DEFAULT_EXTRA = [
    "PO NUMBER",
    "ORDER TYPE",
    "ALIAS / RELATED ITEMS",
    "ALT PO",
]

def sanitise(colname):
    return colname.partition("#")[0].strip()

def add_default_key_config(canon):
    for rec in canon["customers"]:
        if "order_key_config" not in rec:
            rec["order_key_config"] = {
                "unique_keys": list(DEFAULT_UNIQUE),
                "extra_checks": list(DEFAULT_EXTRA),
            }
        else:
            rec["order_key_config"]["unique_keys"] = list(rec["order_key_config"].get("unique_keys", DEFAULT_UNIQUE))
            rec["order_key_config"]["extra_checks"] = list(rec["order_key_config"].get("extra_checks", DEFAULT_EXTRA))
    return canon

def make_key(df, cols):
    clean_cols = [sanitise(c) for c in cols if sanitise(c) in df.columns]
    if not clean_cols:
        raise Exception("No key columns found in dataframe!")
    return df[clean_cols].fillna("").astype(str).apply(lambda r: "|".join(r.values), axis=1)

def duplicate_key_check(df, canon, customers_to_check=None):
    dups = []
    for rec in canon["customers"]:
        cust = rec["canonical"]
        if customers_to_check and cust.upper() not in [x.upper() for x in customers_to_check]:
            continue
        cols = rec["order_key_config"]["unique_keys"] + rec["order_key_config"]["extra_checks"]
        subset = df[df["CUSTOMER NAME"].str.upper() == cust.upper()].copy()
        if subset.empty:
            continue
        clean_cols = [sanitise(c) for c in cols if sanitise(c) in subset.columns]
        missing = set([sanitise(c) for c in cols]) - set(clean_cols)
        if missing:
            print(f"⚠️  {cust}: columns {missing} not in data – ignored", file=sys.stderr)
        subset["ORDER_KEY"] = make_key(subset, clean_cols)
        dup_rows = subset[subset["ORDER_KEY"].duplicated(keep=False)]
        if not dup_rows.empty:
            dups.append(dup_rows.assign(CANONICAL=cust))
    if dups:
        out = pd.concat(dups, ignore_index=True)
        out.to_csv("duplicate_keys.csv", index=False)
        raise AssertionError(
            f"🚨 Found {len(out)} duplicate rows across {out['CANONICAL'].nunique()} customers. See duplicate_keys.csv"
        )
    print("✅ No duplicate ORDER_KEYs")

def get_customer_by_master_order_list(canon, master_name):
    for rec in canon["customers"]:
        mol = rec.get("master_order_list")
        if isinstance(mol, list):
            if any(master_name.strip().upper() == x.strip().upper() for x in mol):
                return rec
        elif isinstance(mol, str):
            if master_name.strip().upper() == mol.strip().upper():
                return rec
    return None

def get_customer_by_shipped(canon, shipped_name):
    for rec in canon["customers"]:
        ship = rec.get("shipped", "")
        if shipped_name.strip().upper() == ship.strip().upper():
            return rec
    return None

def print_reconciliation_summary(
    canon, orders, ships, order_keys, ship_keys, only_in_orders, only_in_shipments,
    order_ship_keys, shipment_ship_keys, only_in_order_ships, only_in_shipment_ships
):
    only_in_order_ships = set(str(k) for k in only_in_order_ships if k is not None and k == k)
    only_in_shipment_ships = set(str(k) for k in only_in_shipment_ships if k is not None and k == k)
    order_ship_keys = set(str(k) for k in order_ship_keys if k is not None and k == k)
    shipment_ship_keys = set(str(k) for k in shipment_ship_keys if k is not None and k == k)

    master_name = orders["CUSTOMER NAME"].iloc[0] if "CUSTOMER NAME" in orders.columns else "N/A"
    shipped_name = ships["Customer"].iloc[0] if "Customer" in ships.columns else "N/A"
    rec = get_customer_by_master_order_list(canon, master_name)
    canonical = rec.get("canonical", "N/A") if rec else "N/A"
    print("\n" + "="*48)
    print(f"RECONCILIATION SUMMARY")
    print("="*48)
    print(f"CUSTOMER NAME:        {master_name}")
    print(f"master_order_list:    {rec.get('master_order_list', 'N/A') if rec else 'N/A'}")
    print(f"shipped:              {rec.get('shipped', 'N/A') if rec else 'N/A'}")
    print(f"canonical:            {canonical}")
    print(f"# of records (orders):      {len(orders)}")
    print(f"# of records (shipments):   {len(ships)}")
    print(f"# of unique keys (orders):  {len(order_keys)}")
    print(f"# of unique keys (shipments): {len(ship_keys)}")
    print(f"# of matching keys:         {len(order_keys & ship_keys)}")
    print(f"# of order keys not matched: {len(order_keys - ship_keys)}")
    print(f"# of shipment keys not in order: {len(ship_keys - order_keys)}")
    print("\n# of unique SHIP_KEYS (orders):      {}".format(len(order_ship_keys)))
    print("# of unique SHIP_KEYS (shipments):   {}".format(len(shipment_ship_keys)))
    print("# of matching SHIP_KEYS:             {}".format(len(order_ship_keys & shipment_ship_keys)))
    print("# of SHIP_KEYS only in orders:       {}".format(len(only_in_order_ships)))
    print("# of SHIP_KEYS only in shipments:    {}".format(len(only_in_shipment_ships)))
    print("\n-- List of unmatched SHIP_KEYS in orders (up to 10):")
    for k in list(sorted(only_in_order_ships))[:10]:
        print(f"  {k}")
    if len(only_in_order_ships) > 10:
        print("  ...")
    print("\n-- List of unmatched SHIP_KEYS in shipments (up to 10):")
    for k in list(sorted(only_in_shipment_ships))[:10]:
        print(f"  {k}")
    if len(only_in_shipment_ships) > 10:
        print("  ...")
    if "AAG ORDER NUMBER" in orders.columns:
        num_dups = orders["AAG ORDER NUMBER"].duplicated(keep=False).sum()
        print(f"\n# of orders with duplicate AAG ORDER NUMBER: {num_dups}")
    else:
        print("\nAAG ORDER NUMBER column not found in orders.")
    print("="*48)

def reconcile_quantities(order_file, ship_file, canon, customers_to_check=None):
    orders = pd.read_csv(order_file, dtype=str, low_memory=False)
    ships  = pd.read_csv(ship_file, dtype=str, low_memory=False)

    if customers_to_check:
        orders = orders[orders["CUSTOMER NAME"].str.upper().isin([x.upper() for x in customers_to_check])]
        ships = ships[ships["Customer"].str.upper().isin([x.upper() for x in customers_to_check])]

    if len(orders) == 0 or len(ships) == 0:
        print("No records to process after customer filtering!")
        return

    master_name = orders["CUSTOMER NAME"].iloc[0]
    shipped_name = ships["Customer"].iloc[0]

    order_rec = get_customer_by_master_order_list(canon, master_name)
    ship_rec = get_customer_by_shipped(canon, shipped_name)
    if order_rec is None:
        raise Exception(f"No customer config found for master_order_list: {master_name}")
    if ship_rec is None:
        raise Exception(f"No customer config found for shipped: {shipped_name}")

    order_key_conf = order_rec.get("order_key_config", None)
    order_key_cols = order_key_conf["unique_keys"] if order_key_conf else DEFAULT_UNIQUE
    order_extra_cols = order_key_conf.get("extra_checks", []) if order_key_conf else DEFAULT_EXTRA
    order_cols = [c for c in order_key_cols + order_extra_cols if c in orders.columns]

    shipment_key_conf = ship_rec.get("shipment_key_config", {})
    col_map = shipment_key_conf.get("column_mapping", {})
    ship_key_cols = shipment_key_conf.get("unique_keys") or []
    shipment_extra_cols = shipment_key_conf.get("extra_checks", []) or []
    ship_cols = [c for c in ship_key_cols + shipment_extra_cols if c in ships.columns]

    print("\n==== Column Mapping Checks ====")
    for src_col, dest_col in col_map.items():
        if src_col not in orders.columns:
            print(f"WARNING: Order file missing source column '{src_col}' for mapping to '{dest_col}'")
        if dest_col not in ships.columns:
            print(f"WARNING: Shipment file missing target column '{dest_col}'")
    print("================================\n")

    print("\n--- DEBUG: Shipment file columns ---")
    print(list(ships.columns))
    print("Using ship_cols:", ship_cols)
    print("Missing ship_cols:", [c for c in ship_key_cols + shipment_extra_cols if c not in ships.columns])
    print("--- END DEBUG ---\n")

    orders["KEY"] = make_key(orders.fillna(""), order_cols)
    ships["KEY"]  = make_key(ships.fillna(""), ship_cols)

    order_keys = set(orders[orders["ORDER TYPE"].str.upper() == "ACTIVE"]["KEY"])
    ship_keys = set(ships["KEY"])

    def build_dynamic_key_order(df, col_map):
        used_keys = [k for k in col_map.keys() if k in df.columns]
        if not used_keys:
            return pd.Series([""], index=df.index)
        key_fields = [df[k].fillna("").astype(str).str.upper().str.strip() for k in used_keys]
        return pd.Series(["|".join(row) for row in zip(*key_fields)], index=df.index)

    def build_dynamic_key_ship(df, ship_key_fields):
        used_keys = [c for c in ship_key_fields if c in df.columns]
        if not used_keys:
            return pd.Series([""], index=df.index)
        key_fields = [df[c].fillna("").astype(str).str.upper().str.strip() for c in used_keys]
        return pd.Series(["|".join(row) for row in zip(*key_fields)], index=df.index)

    orders["SHIP_KEY"] = build_dynamic_key_order(orders, col_map)
    ship_key_fields = [col_map[k] for k in col_map.keys() if col_map[k] in ships.columns]
    ships["SHIP_KEY"] = build_dynamic_key_ship(ships, ship_key_fields)

    print(f"Orders SHIP_KEY used columns: {[k for k in col_map.keys() if k in orders.columns]}")
    print(f"Shipments SHIP_KEY used columns: {ship_key_fields}")

    order_ship_keys = set(orders[orders["ORDER TYPE"].str.upper() == "ACTIVE"]["SHIP_KEY"])
    shipment_ship_keys = set(ships["SHIP_KEY"])

    only_in_orders = order_keys - ship_keys
    only_in_shipments = ship_keys - order_keys
    only_in_order_ships = order_ship_keys - shipment_ship_keys
    only_in_shipment_ships = shipment_ship_keys - order_ship_keys

    with open("shipment_key_diffs.txt", "w") as f:
        f.write("Order keys NOT shipped:\n")
        for k in sorted(str(x) for x in only_in_orders):
            f.write(k + "\n")
        f.write("\nShipment keys NOT in order list:\n")
        for k in sorted(str(x) for x in only_in_shipments):
            f.write(k + "\n")
        f.write("\nOrder SHIP_KEYS NOT in shipments:\n")
        for k in sorted(str(x) for x in only_in_order_ships):
            f.write(k + "\n")
        f.write("\nShipment SHIP_KEYS NOT in orders:\n")
        for k in sorted(str(x) for x in only_in_shipment_ships):
            f.write(k + "\n")

    print_reconciliation_summary(
        canon, orders, ships, order_keys, ship_keys, only_in_orders, only_in_shipments,
        order_ship_keys, shipment_ship_keys, only_in_order_ships, only_in_shipment_ships
    )
