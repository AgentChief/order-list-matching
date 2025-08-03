# ── src/core/reporter.py ──────────────────────────────────────────────
"""
Comprehensive reporting for order-shipment reconciliation.
Generates markdown reports with detailed breakdowns.
"""
import pandas as pd
from datetime import datetime
from pathlib import Path

def analyze_partial_matches(unmatched_ships, orders, join_cols):
    """Analyze unmatched shipments to see which columns do match"""
    partial_matches = []
    
    for _, ship in unmatched_ships.iterrows():
        best_match = {"ship_id": ship.iloc[0], "matching_cols": [], "ship_data": {}}
        
        # Store key shipment data for reporting
        for col in join_cols:
            if col in ship.index:
                best_match["ship_data"][col] = ship[col]
        
        # Check each order to see which columns match
        for _, order in orders.iterrows():
            matching_cols = []
            for col in join_cols:
                if col in ship.index and col in order.index:
                    if pd.notna(ship[col]) and pd.notna(order[col]):
                        if str(ship[col]).strip() == str(order[col]).strip():
                            matching_cols.append(col)
            
            # Keep track of best partial match for this shipment
            if len(matching_cols) > len(best_match["matching_cols"]):
                best_match["matching_cols"] = matching_cols
        
        partial_matches.append(best_match)
    
    return partial_matches

def generate_markdown_report(customer, po, results_data, output_dir="../reports"):
    """Generate comprehensive markdown report"""
    
    # Extract data
    exact_matches = results_data["exact_matches"]
    fuzzy_matches = results_data["fuzzy_matches"] 
    unmatched = results_data["unmatched"]
    orders = results_data["orders"]
    join_cols = results_data["join_cols"]
    
    # Analyze partial matches for unmatched shipments
    partial_matches = analyze_partial_matches(unmatched, orders, join_cols)
    
    # Calculate totals
    total_shipments = len(exact_matches) + len(fuzzy_matches) + len(unmatched)
    total_matched = len(exact_matches) + len(fuzzy_matches)
    
    if total_shipments == 0:
        print(f"ℹ️  No shipments to report for {customer} {po}")
        return None
    
    # Create output directory
    report_dir = Path(output_dir) / customer
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate report content
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_content = f"""# Order-Shipment Reconciliation Report

**Customer:** {customer}  
**PO Number:** {po}  
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  
**Matching Columns:** {', '.join(join_cols)}

## Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total Shipment Lines** | {total_shipments:,} | 100.0% |
| **Total Matched** | {total_matched:,} | {(total_matched/total_shipments*100):,.1f}% |
| **Exact Matches** | {len(exact_matches):,} | {(len(exact_matches)/total_shipments*100):,.1f}% |
| **Fuzzy Matches** | {len(fuzzy_matches):,} | {(len(fuzzy_matches)/total_shipments*100):,.1f}% |
| **No Matches** | {len(unmatched):,} | {(len(unmatched)/total_shipments*100):,.1f}% |

"""

    # Fuzzy matches section
    if len(fuzzy_matches) > 0:
        report_content += """
### Fuzzy Matches

| Shipment ID | Confidence | PO | Method | Style | Color | Reason |
|-------------|------------|----|---------| ------|-------|---------|
"""
        for _, row in fuzzy_matches.iterrows():
            ship_id = row.iloc[0]  # First column is shipment ID
            confidence = f"{row['confidence']:.1%}" if 'confidence' in row else "N/A"
            po_val = row.get('PO NUMBER', 'N/A')
            method = row.get('PLANNED DELIVERY METHOD', 'N/A')
            style = row.get('CUSTOMER STYLE', 'N/A')
            color = row.get('CUSTOMER COLOUR DESCRIPTION', 'N/A')
            
            report_content += f"| {ship_id} | {confidence} | {po_val} | {method} | {style} | {color} | Fuzzy string matching |\n"
    else:
        report_content += "\n### Fuzzy Matches\n\nNo fuzzy matches found.\n"

    # No matches section
    if len(unmatched) > 0:
        report_content += f"""
### No Matches ({len(unmatched):,} shipments)

The following shipments could not be matched to any orders. Analysis shows which columns had partial matches:

| Shipment ID | PO | Method | Style | Color | Partial Matches |
|-------------|----|---------| ------|-------|-----------------|
"""
        for pm in partial_matches:
            ship_data = pm["ship_data"]
            ship_id = pm["ship_id"]
            po_val = ship_data.get('PO NUMBER', 'N/A')
            method = ship_data.get('PLANNED DELIVERY METHOD', 'N/A')
            style = ship_data.get('CUSTOMER STYLE', 'N/A')
            color = ship_data.get('CUSTOMER COLOUR DESCRIPTION', 'N/A')
            
            if pm["matching_cols"]:
                partial_info = f"{len(pm['matching_cols'])}/{len(join_cols)} cols: {', '.join(pm['matching_cols'])}"
            else:
                partial_info = "No column matches"
            
            report_content += f"| {ship_id} | {po_val} | {method} | {style} | {color} | {partial_info} |\n"
    else:
        report_content += "\n### No Matches\n\nAll shipments were successfully matched!\n"

    # Analysis section
    report_content += f"""
## Analysis

### Matching Performance
- **Exact Match Rate:** {(len(exact_matches)/total_shipments*100):,.1f}%
- **Total Match Rate:** {(total_matched/total_shipments*100):,.1f}%

"""

    if len(unmatched) > 0:
        # Analyze common patterns in unmatched
        no_partial = len([pm for pm in partial_matches if len(pm["matching_cols"]) == 0])
        some_partial = len(partial_matches) - no_partial
        
        report_content += f"""### Unmatched Analysis
- **Complete mismatches:** {no_partial} shipments (no columns match)
- **Partial matches:** {some_partial} shipments (some columns match)

"""
        
        if some_partial > 0:
            # Show breakdown by number of matching columns
            partial_breakdown = {}
            for pm in partial_matches:
                match_count = len(pm["matching_cols"])
                if match_count > 0:
                    partial_breakdown[match_count] = partial_breakdown.get(match_count, 0) + 1
            
            report_content += "**Partial Match Breakdown:**\n"
            for match_count in sorted(partial_breakdown.keys(), reverse=True):
                count = partial_breakdown[match_count]
                report_content += f"- {match_count}/{len(join_cols)} columns match: {count} shipments\n"

    # Add detailed matching data section
    report_content += "\n## Detailed Matching Data\n\n"
    
    if total_shipments > 0:
        # Create table showing matching columns with order vs shipment values
        all_matches = []
        
        # Process exact matches
        for _, row in exact_matches.iterrows():
            match_data = {"Match_Type": "Exact", "Match_Percentage": "100.0%"}
            for col in join_cols:
                # Look for _o and _s suffixed columns
                order_col = f"{col}_o"
                ship_col = f"{col}_s"
                
                if order_col in row.index and ship_col in row.index:
                    match_data[f"Order_{col}"] = str(row[order_col]) if pd.notna(row[order_col]) else ""
                    match_data[f"Ship_{col}"] = str(row[ship_col]) if pd.notna(row[ship_col]) else ""
                elif col in row.index:
                    # Fallback if no suffixes - use the merged value for both
                    value = str(row[col]) if pd.notna(row[col]) else ""
                    match_data[f"Order_{col}"] = value
                    match_data[f"Ship_{col}"] = value
            all_matches.append(match_data)
        
        # Process fuzzy matches  
        for _, row in fuzzy_matches.iterrows():
            match_data = {"Match_Type": "Fuzzy"}
            if "_match_score" in row.index:
                match_data["Match_Percentage"] = f"{row['_match_score']:.1f}%"
            else:
                match_data["Match_Percentage"] = "N/A"
            for col in join_cols:
                # For fuzzy matches, look for _order_ prefixed columns
                order_col = f"_order_{col}"
                if order_col in row.index and col in row.index:
                    match_data[f"Order_{col}"] = str(row[order_col]) if pd.notna(row[order_col]) else ""
                    match_data[f"Ship_{col}"] = str(row[col]) if pd.notna(row[col]) else ""
                elif col in row.index:
                    match_data[f"Ship_{col}"] = str(row[col]) if pd.notna(row[col]) else ""
                    match_data[f"Order_{col}"] = ""
            all_matches.append(match_data)
        
        # Process unmatched (show first few for reference)
        unmatched_count = 0
        for _, row in unmatched.iterrows():
            if unmatched_count >= 5:  # Limit to first 5 unmatched for brevity
                break
            match_data = {"Match_Type": "No Match", "Match_Percentage": "0.0%"}
            for col in join_cols:
                if col in row.index:
                    match_data[f"Ship_{col}"] = str(row[col]) if pd.notna(row[col]) else ""
                    match_data[f"Order_{col}"] = "No Match"
            all_matches.append(match_data)
            unmatched_count += 1
        
        if len(unmatched) > 5:
            all_matches.append({"Match_Type": f"... and {len(unmatched)-5} more unmatched", "Match_Percentage": "", **{f"Order_{col}": "" for col in join_cols}, **{f"Ship_{col}": "" for col in join_cols}})
        
        # Generate markdown table with Order/Ship pairs
        if all_matches:
            # Table header - show Order/Ship pairs for each join column
            headers = []
            for col in join_cols:
                headers.extend([f"Order_{col}", f"Ship_{col}"])
            headers.extend(["Match_Type", "Match_Percentage"])
            
            report_content += "| " + " | ".join(headers) + " |\n"
            report_content += "|" + "|".join([" --- " for _ in headers]) + "|\n"
            
            # Table rows
            for match in all_matches:
                row_values = []
                for header in headers:
                    value = match.get(header, "")
                    # Escape pipe characters and limit length
                    value = str(value).replace("|", "\\|")[:30]
                    if len(str(match.get(header, ""))) > 30:
                        value += "..."
                    row_values.append(value)
                report_content += "| " + " | ".join(row_values) + " |\n"
    else:
        report_content += "No shipments to display.\n"

    report_content += f"""
---
*Report generated by Order-Shipment Reconciliation System*  
*Timestamp: {timestamp}*
"""

    # Write report - use simplified naming for date-based reports
    if po.startswith("shipped_"):
        # Extract date from "shipped_YYYY-MM-DD" format
        date_part = po.replace("shipped_", "").replace("-", "_")
        report_file = report_dir / f"{customer}_{date_part}.md"
    else:
        # Traditional naming for PO-based reports
        report_file = report_dir / f"{po}_{timestamp}_reconciliation_report.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"[REPORT] Detailed report saved: {report_file}")
    return report_file

def generate_enhanced_csv_report(customer, po, results_data, output_dir="../reports"):
    """Generate CSV report with matching columns side-by-side, match type, and percentage"""
    
    exact_matches = results_data["exact_matches"]
    fuzzy_matches = results_data["fuzzy_matches"]
    unmatched = results_data["unmatched"]
    orders = results_data["orders"]
    join_cols = results_data["join_cols"]
    
    # Create output directory
    report_dir = Path(output_dir) / customer
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare enhanced CSV data
    enhanced_rows = []
    
    # Process exact matches
    for _, row in exact_matches.iterrows():
        enhanced_row = {}
        
        # Add shipment data (columns ending with _s)
        for col in row.index:
            if col.endswith('_s'):
                base_col = col[:-2]  # Remove _s suffix
                enhanced_row[f"Ship_{base_col}"] = row[col]
            elif not col.endswith('_o') and not col.startswith('_') and col not in ['method', 'confidence']:
                # Handle non-suffixed columns (original shipment columns)
                enhanced_row[f"Ship_{col}"] = row[col]
        
        # Add corresponding order data (columns ending with _o)
        for col in row.index:
            if col.endswith('_o'):
                base_col = col[:-2]  # Remove _o suffix
                enhanced_row[f"Order_{base_col}"] = row[col]
        
        # Add match information
        enhanced_row["Match_Type"] = "Exact"
        enhanced_row["Match_Percentage"] = 100.0
        
        enhanced_rows.append(enhanced_row)
    
    # Process fuzzy matches
    for _, row in fuzzy_matches.iterrows():
        enhanced_row = {}
        
        # Add shipment data
        for col in row.index:
            if not col.startswith('_order_') and not col.startswith('_match_'):
                enhanced_row[f"Ship_{col}"] = row[col]
        
        # Add corresponding order data
        for col in join_cols:
            order_col = f"_order_{col}"
            if order_col in row.index:
                enhanced_row[f"Order_{col}"] = row[order_col]
        
        # Add match information
        enhanced_row["Match_Type"] = "Fuzzy"
        # Extract fuzzy match percentage if available
        if "_match_score" in row.index:
            enhanced_row["Match_Percentage"] = round(row["_match_score"], 1)
        else:
            enhanced_row["Match_Percentage"] = "N/A"
        
        enhanced_rows.append(enhanced_row)
    
    # Process unmatched
    for _, row in unmatched.iterrows():
        enhanced_row = {}
        
        # Add shipment data
        for col in row.index:
            enhanced_row[f"Ship_{col}"] = row[col]
        
        # Add empty order columns
        for col in join_cols:
            enhanced_row[f"Order_{col}"] = ""
        
        # Add match information
        enhanced_row["Match_Type"] = "No Match"
        enhanced_row["Match_Percentage"] = 0.0
        
        enhanced_rows.append(enhanced_row)
    
    # Create DataFrame and save
    if enhanced_rows:
        enhanced_df = pd.DataFrame(enhanced_rows)
        
        # Reorder columns to group matching pairs together
        ordered_cols = []
        
        # Add matching column pairs first (based on join_cols)
        for col in join_cols:
            ship_col = f"Ship_{col}"
            order_col = f"Order_{col}"
            if ship_col in enhanced_df.columns:
                ordered_cols.append(ship_col)
            if order_col in enhanced_df.columns:
                ordered_cols.append(order_col)
        
        # Add remaining shipment columns
        for col in enhanced_df.columns:
            if col.startswith("Ship_") and col not in ordered_cols:
                ordered_cols.append(col)
        
        # Add remaining order columns  
        for col in enhanced_df.columns:
            if col.startswith("Order_") and col not in ordered_cols:
                ordered_cols.append(col)
        
        # Add match information columns
        ordered_cols.extend(["Match_Type", "Match_Percentage"])
        
        # Add any remaining columns
        for col in enhanced_df.columns:
            if col not in ordered_cols:
                ordered_cols.append(col)
        
        enhanced_df = enhanced_df.reindex(columns=ordered_cols)
        
        # Generate filename with new naming convention for date-based reports
        if po.startswith("shipped_"):
            date_part = po.replace("shipped_", "").replace("-", "_")
            csv_file = report_dir / f"{customer}_{date_part}_enhanced.csv"
        else:
            csv_file = report_dir / f"{po}_enhanced.csv"
        
        enhanced_df.to_csv(csv_file, index=False)
        print(f"[CSV] Enhanced CSV saved: {csv_file}")
        return csv_file
    
    return None

def print_summary(customer, po, results_data):
    """Print concise summary to console"""
    exact_matches = results_data["exact_matches"]
    fuzzy_matches = results_data["fuzzy_matches"]
    unmatched = results_data["unmatched"]
    
    total_shipments = len(exact_matches) + len(fuzzy_matches) + len(unmatched)
    total_matched = len(exact_matches) + len(fuzzy_matches)
    
    if total_shipments == 0:
        print(f"📈 {customer} {po}: No shipments to process")
        return
    
    print(f"\n[SUMMARY] {customer} {po} Summary:")
    print(f"   Total Shipments: {total_shipments:,}")
    print(f"   Exact Matches:   {len(exact_matches):,} ({len(exact_matches)/total_shipments*100:,.1f}%)")
    print(f"   Fuzzy Matches:   {len(fuzzy_matches):,} ({len(fuzzy_matches)/total_shipments*100:,.1f}%)")
    print(f"   No Matches:      {len(unmatched):,} ({len(unmatched)/total_shipments*100:,.1f}%)")
    print(f"   Total Match Rate: {total_matched/total_shipments*100:,.1f}%")
