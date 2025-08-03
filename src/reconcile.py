#!/usr/bin/env python3
import argparse, datetime as dt
from pathlib import Path
from ruamel.yaml import YAML
import pandas as pd
from tqdm import tqdm

from core import extractor, normalise, match_exact, match_fuzzy, match_llm, reporter
from llm_analysis_client_batched import analyze_reconciliation_patterns

yaml = YAML(typ="safe")
# Get the path to the project root from this file's location
project_root = Path(__file__).parent.parent
CUSTOMERS_DATA = yaml.load((project_root / "config" / "canonical_customers.yaml").read_text())
CUSTOMERS = CUSTOMERS_DATA["customers"]
GLOBAL_CONFIG = CUSTOMERS_DATA.get("global_config", {})
CFG = yaml.load((project_root / "config" / "config.yaml").read_text())

def get_cfg(name): 
    """Get customer configuration with global fallbacks"""
    customer_cfg = next(c for c in CUSTOMERS if c["canonical"]==name)
    
    # Apply global fallbacks if customer doesn't have specific configs
    if "map" not in customer_cfg and "map" in GLOBAL_CONFIG:
        customer_cfg["map"] = GLOBAL_CONFIG["map"].copy()
        print(f"ℹ️  Using global map for {name}")
    
    if "order_key_config" not in customer_cfg and "order_key_config" in GLOBAL_CONFIG:
        customer_cfg["order_key_config"] = GLOBAL_CONFIG["order_key_config"].copy()
        print(f"ℹ️  Using global order_key_config for {name}")
    
    if "shipment_key_config" not in customer_cfg and "shipment_key_config" in GLOBAL_CONFIG:
        customer_cfg["shipment_key_config"] = GLOBAL_CONFIG["shipment_key_config"].copy()
        print(f"ℹ️  Using global shipment_key_config for {name}")
    
    return customer_cfg

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None
    
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d']
    for fmt in formats:
        try:
            return dt.datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    raise ValueError(f"Unable to parse date: {date_str}")

def reconcile_by_individual_dates(customer, date_from, date_to, use_llm=False, llm_analysis=False):
    """Reconcile a customer by individual shipment dates, creating one report per date"""
    
    # Parse dates
    start_date = dt.datetime.strptime(parse_date(date_from), '%Y-%m-%d')
    end_date = dt.datetime.strptime(parse_date(date_to), '%Y-%m-%d')
    
    print(f"🗓️  Processing {customer} by individual dates: {date_from} to {date_to}")
    
    cfg = get_cfg(customer)
    
    # Get all shipments in the date range first
    all_ships = extractor.shipments_by_date_range(cfg["aliases"], date_from, date_to)
    
    if len(all_ships) == 0:
        print(f"No shipments found for {customer} in date range {date_from} to {date_to}")
        return
    
    # Get unique shipment dates
    all_ships['Shipped_Date_parsed'] = pd.to_datetime(all_ships['Shipped_Date']).dt.date
    unique_dates = sorted(all_ships['Shipped_Date_parsed'].unique())
    
    print(f"Found shipments on {len(unique_dates)} dates: {', '.join(str(d) for d in unique_dates)}")
    
    # Get all orders for the customer (no date filter on orders)
    # Use master_order_list if available, otherwise fall back to aliases
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    orders = extractor.orders(order_customer_names)
    print(f"   Querying orders for: {order_customer_names}")
    print(f"   Found {len(orders)} orders")
    
    total_processed = 0
    results_summary = []
    
    for ship_date in unique_dates:
        date_str = ship_date.strftime('%Y-%m-%d')
        print(f"\n📅 Processing {customer} for {date_str}")
        
        # Filter shipments for this specific date
        ships_for_date = all_ships[all_ships['Shipped_Date_parsed'] == ship_date].copy()
        ships_for_date = ships_for_date.drop('Shipped_Date_parsed', axis=1)  # Remove helper column
        
        print(f"   {len(ships_for_date)} shipments on {date_str}")
        
        # Normalize data
        orders_norm = normalise.orders(orders, customer)
        ships_norm = normalise.shipments(ships_for_date, customer)
        
        print(f"   After normalization: {len(ships_norm)} shipments remaining")
        
        # Get join columns for reporting
        ships_for_join_check = ships_norm.rename(columns={v: k for k, v in cfg["map"].items()})
        join_cols = [order_col for order_col in cfg["map"].keys() 
                     if order_col in orders_norm.columns and order_col in ships_for_join_check.columns]
        
        print(f"   Join columns: {join_cols}")
        print(f"   Orders available: {len(orders_norm)}")
        
        # Matching stages
        exact, ships_left = match_exact.match(orders_norm, ships_norm, cfg)
        print(f"   After exact matching: {len(exact)} matched, {len(ships_left)} remaining")
        
        fuzzy, ships_left = match_fuzzy.match(orders_norm, ships_left, cfg)
        print(f"   After fuzzy matching: {len(fuzzy)} fuzzy matched, {len(ships_left)} remaining")
        
        llm = pd.DataFrame()
        if use_llm and not ships_left.empty:
            llm, ships_left = match_llm.match(orders_norm, ships_left)
        
        # Prepare results for reporting
        results_data = {
            "exact_matches": exact,
            "fuzzy_matches": pd.concat([fuzzy, llm], ignore_index=True) if len(llm) > 0 else fuzzy,
            "unmatched": ships_left,
            "orders": orders_norm,
            "join_cols": join_cols
        }
        
        # Generate report with date-specific identifier
        identifier = f"shipped {date_str}"
        reporter.print_summary(customer, identifier, results_data)
        
        # Create date-specific report
        report_id = f"shipped_{date_str}"
        report_file = reporter.generate_markdown_report(customer, report_id, results_data)
        
        # Generate enhanced CSV report
        enhanced_csv_file = reporter.generate_enhanced_csv_report(customer, report_id, results_data)
        
        # Save traditional CSV files
        matched = pd.concat([exact, fuzzy, llm], ignore_index=True)
        out_dir = Path(CFG["report_root"]) / customer
        out_dir.mkdir(parents=True, exist_ok=True)
        
        if len(matched) > 0:
            csv_file = out_dir / f"{report_id}_matches.csv"
            matched.to_csv(csv_file, index=False)
            print(f"💾 CSV matches saved: {csv_file}")
            
        if not ships_left.empty:
            csv_file = out_dir / f"{report_id}_unmatched.csv"
            ships_left.to_csv(csv_file, index=False)
            print(f"💾 CSV unmatched saved: {csv_file}")
        
        # Collect summary for overall report
        total_shipments = len(exact) + len(fuzzy) + len(llm) + len(ships_left)
        total_matched = len(exact) + len(fuzzy) + len(llm)
        
        results_summary.append({
            "date": date_str,
            "total_shipments": total_shipments,
            "exact_matches": len(exact),
            "fuzzy_matches": len(fuzzy) + len(llm),
            "unmatched": len(ships_left),
            "match_rate": (total_matched / total_shipments * 100) if total_shipments > 0 else 0
        })
        
        total_processed += 1
    
    # Print overall summary
    print(f"\n🎯 {customer} Date Range Summary ({date_from} to {date_to}):")
    print(f"   Dates processed: {total_processed}")
    
    total_all_shipments = sum(r["total_shipments"] for r in results_summary)
    total_all_matched = sum(r["exact_matches"] + r["fuzzy_matches"] for r in results_summary)
    
    if total_all_shipments > 0:
        print(f"   Total shipments: {total_all_shipments:,}")
        print(f"   Total matched: {total_all_matched:,} ({total_all_matched/total_all_shipments*100:.1f}%)")
        print(f"   By date:")
        for result in results_summary:
            print(f"     {result['date']}: {result['total_shipments']} ships, {result['match_rate']:.1f}% matched")

def reconcile_all_customers_by_date(date_from=None, date_to=None, use_llm=False, llm_analysis=False):
    """Reconcile all customers for a given date range"""
    
    # Parse dates
    if date_from:
        date_from = parse_date(date_from)
    if date_to:
        date_to = parse_date(date_to)
    
    if not date_from and not date_to:
        raise ValueError("Must specify at least one date (--date-from or --date-to)")
    
    print(f"🔄 Processing all customers for date range: {date_from or 'start'} to {date_to or 'end'}")
    
    # Get customers that have proper configuration (shipped status)
    valid_customers = [c for c in CUSTOMERS if c.get("shipped")]
    
    print(f"Found {len(valid_customers)} customers with shipping data")
    
    total_processed = 0
    results_summary = []
    
    for customer_cfg in tqdm(valid_customers, desc="Processing customers"):
        customer_name = customer_cfg["canonical"]
        
        try:
            # Get enriched config with global fallbacks
            cfg = get_cfg(customer_name)
            
            # Check if customer has required mapping
            if "map" not in cfg:
                print(f"⚠️  Skipping {customer_name}: No map configuration")
                continue
            
            # Get shipments for this customer in date range
            ships = extractor.shipments_by_date_range(cfg["aliases"], date_from, date_to)
            
            if len(ships) == 0:
                continue
                
            print(f"\n📦 Processing {customer_name}: {len(ships)} shipments")
            
            # Process this customer
            reconcile(customer_name, None, date_from, date_to, use_llm, llm_analysis)
            
            total_processed += 1
            results_summary.append({
                "customer": customer_name,
                "shipments": len(ships)
            })
            
        except Exception as e:
            print(f"❌ Error processing {customer_name}: {e}")
            continue
    
    print(f"\n✅ Completed processing {total_processed} customers")
    print("\n📊 Summary:")
    for result in sorted(results_summary, key=lambda x: x["shipments"], reverse=True):
        print(f"   {result['customer']}: {result['shipments']} shipments")

def reconcile(customer=None, po=None, date_from=None, date_to=None, use_llm=False, llm_analysis=False):
    # Parse and validate dates
    if date_from:
        date_from = parse_date(date_from)
    if date_to:
        date_to = parse_date(date_to)

    # If no customer specified, get all customers that have shipments in date range
    if not customer:
        if not (date_from or date_to):
            raise ValueError("Must specify either --customer or date range (--date-from/--date-to)")
        
        # Get all customers with shipments in date range
        all_customers = [c["canonical"] for c in CUSTOMERS if c.get("shipped")]
        
        print(f"🔍 Finding shipments for date range: {date_from or 'start'} to {date_to or 'end'}")
        
        # Process each customer that has shipments in the date range
        processed_customers = 0
        for customer_name in all_customers:
            try:
                cfg = get_cfg(customer_name)
                ships_in_range = extractor.shipments_by_date_range(cfg["aliases"], date_from, date_to)
                
                if len(ships_in_range) > 0:
                    print(f"\n📦 Processing {customer_name}: {len(ships_in_range)} shipments")
                    reconcile(customer_name, None, date_from, date_to, use_llm, llm_analysis)
                    processed_customers += 1
                    
            except Exception as e:
                print(f"⚠️  Error processing {customer_name}: {e}")
                continue
        
        print(f"\n✅ Completed reconciliation for {processed_customers} customers with shipments in date range")
        return
    
    # Single customer reconciliation
    cfg = get_cfg(customer)

    # 1. Extract data based on parameters
    # Use master_order_list if available, otherwise fall back to aliases
    order_customer_names = [cfg.get("master_order_list")] if cfg.get("master_order_list") else cfg["aliases"]
    
    if po:
        # PO-specific reconciliation with optional date filtering
        orders = extractor.orders(order_customer_names, po)
        ships = extractor.shipments(cfg["aliases"], po, date_from, date_to)
        identifier = f"PO {po}"
        if date_from or date_to:
            date_range = f" ({date_from or 'start'} to {date_to or 'end'})"
            identifier += date_range
    elif date_from or date_to:
        # Date-range reconciliation (all POs in date range)
        orders = extractor.orders(order_customer_names)  # All orders for customer
        ships = extractor.shipments_by_date_range(cfg["aliases"], date_from, date_to)
        identifier = f"shipped {date_from or 'start'} to {date_to or 'end'}"
    else:
        raise ValueError("Must specify either --po or date range (--date-from/--date-to)")

    print(f"Querying orders for: {order_customer_names}")
    print(f"Loaded {len(orders)} orders, {len(ships)} shipments for {customer} {identifier}")

    if len(ships) == 0:
        print("No shipments found for the specified criteria.")
        return

    # 2. Normalise
    orders = normalise.orders(orders, customer)
    ships = normalise.shipments(ships, customer)

    # Get join columns for reporting
    ships_for_join_check = ships.rename(columns={v: k for k, v in cfg["map"].items()})
    join_cols = [order_col for order_col in cfg["map"].keys() 
                 if order_col in orders.columns and order_col in ships_for_join_check.columns]

    # 3. Matching stages
    exact, ships_left = match_exact.match(orders, ships, cfg)
    fuzzy, ships_left = match_fuzzy.match(orders, ships_left, cfg)

    llm = pd.DataFrame()
    if use_llm and not ships_left.empty:
        llm, ships_left = match_llm.match(orders, ships_left)

    # 4. Prepare results for comprehensive reporting
    results_data = {
        "exact_matches": exact,
        "fuzzy_matches": pd.concat([fuzzy, llm], ignore_index=True) if len(llm) > 0 else fuzzy,
        "unmatched": ships_left,
        "orders": orders,
        "join_cols": join_cols
    }

    # 5. Generate comprehensive markdown report
    reporter.print_summary(customer, identifier, results_data)
    
    # Create report identifier for file naming
    if po:
        report_id = po
        if date_from or date_to:
            report_id += f"_{date_from or 'start'}_{date_to or 'end'}"
    else:
        report_id = f"daterange_{date_from or 'start'}_{date_to or 'end'}"
    
    report_file = reporter.generate_markdown_report(customer, report_id, results_data)

    # Generate enhanced CSV report
    enhanced_csv_file = reporter.generate_enhanced_csv_report(customer, report_id, results_data)

    # 6. Also save traditional CSV outputs
    matched = pd.concat([exact, fuzzy, llm], ignore_index=True)
    out_dir = Path(CFG["report_root"]) / customer
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.date.today().strftime("%Y%m%d")
    
    if len(matched) > 0:
        csv_file = out_dir / f"{report_id}_{stamp}_matches.csv"
        matched.to_csv(csv_file, index=False)
        print(f"[CSV] CSV matches saved: {csv_file}")
        
    if not ships_left.empty:
        csv_file = out_dir / f"{report_id}_{stamp}_unmatched.csv"
        ships_left.to_csv(csv_file, index=False)
        print(f"[CSV] CSV unmatched saved: {csv_file}")

    # Show some useful statistics for date-based reconciliation
    if not po and len(ships) > 0:
        print(f"\n📊 Date Range Analysis:")
        po_breakdown = ships.groupby('Customer_PO').size().sort_values(ascending=False)
        print(f"   POs in date range: {len(po_breakdown)}")
        print(f"   Top POs by shipment count:")
        for po_num, count in po_breakdown.head(5).items():
            print(f"     PO {po_num}: {count} shipments")

    # 7. LLM Analysis (if requested)
    if llm_analysis:
        print(f"\n🧠 Performing LLM pattern analysis for {customer}...")
        
        # Run LLM analysis on the results
        analysis_result = analyze_reconciliation_patterns(customer, results_data, identifier)
        
        if analysis_result:
            print(f"✅ LLM analysis completed:")
            if analysis_result.get("report_path"):
                print(f"   📄 Analysis report: {analysis_result['report_path']}")
        else:
            print(f"❌ LLM analysis failed for {customer}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Reconcile orders and shipments by PO or date range")
    p.add_argument("--customer", help="Customer name (optional - if not specified, processes all customers)")
    p.add_argument("--po", help="Specific PO number")
    p.add_argument("--date-from", help="Start date for shipments (YYYY-MM-DD)")
    p.add_argument("--date-to", help="End date for shipments (YYYY-MM-DD)")
    p.add_argument("--by-date", action="store_true", help="Generate one report per shipment date (requires --customer and date range)")
    p.add_argument("--use-llm", action="store_true", help="Use LLM for additional matching")
    p.add_argument("--llm-analysis", action="store_true", help="Use LLM for pattern analysis and customer insights")
    
    args = p.parse_args()
    
    # Handle by-date processing
    if args.by_date:
        if not args.customer:
            p.error("--by-date requires --customer to be specified")
        if not (args.date_from or args.date_to):
            p.error("--by-date requires date range (--date-from and/or --date-to)")
        reconcile_by_individual_dates(args.customer, args.date_from, args.date_to, args.use_llm, args.llm_analysis)
    elif not args.customer:
        # Process all customers by date range
        if not (args.date_from or args.date_to):
            p.error("When no customer specified, must provide date range (--date-from/--date-to)")
        reconcile_all_customers_by_date(args.date_from, args.date_to, args.use_llm, args.llm_analysis)
    else:
        # Single customer processing
        if not args.po and not (args.date_from or args.date_to):
            p.error("When specifying --customer, must also specify either --po or date range (--date-from/--date-to)")
        reconcile(args.customer, args.po, args.date_from, args.date_to, args.use_llm, args.llm_analysis)
