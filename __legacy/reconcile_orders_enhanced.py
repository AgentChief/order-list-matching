"""
reconcile_orders_enhanced.py

An enhanced version of our order-shipment reconciliation script that:
- Uses YAML configuration files for customer-specific rules
- Supports flexible matching strategies (exact, fuzzy)
- Provides detailed variance reporting
- Outputs comprehensive match results

Usage:
    python reconcile_orders_enhanced.py --customer "GREYSON" --po "4755" 
    python reconcile_orders_enhanced.py --customer "JOHNNIE O" --po "ALL" --date-from "2025-01-01" --date-to "2025-07-31"
"""
import argparse
import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
from utils.db_helper import run_query
from typing import Dict, List, Set, Tuple, Optional, Any, Union

# Helper function to normalize string values
def norm(x):
    """Normalize string values for consistent matching."""
    return str(x).upper().strip() if pd.notnull(x) else ""

def load_customer_config(customer_name: str) -> Dict:
    """
    Load customer configuration from YAML files.
    
    Args:
        customer_name: Name of the customer to load configuration for
        
    Returns:
        Dictionary containing customer configuration
    """
    # Try to load from canonical_customers.yaml first (more detailed)
    try:
        with open('canonical_customers.yaml', 'r') as f:
            all_customers = yaml.safe_load(f)
            
        for customer in all_customers.get('customers', []):
            aliases = customer.get('aliases', [])
            if customer_name.upper() in [a.upper() for a in aliases]:
                return customer
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Warning: Could not load canonical_customers.yaml: {e}")
    
    # Try customer_rules.yaml as fallback
    try:
        with open('customer_rules.yaml', 'r') as f:
            fallback_customers = yaml.safe_load(f)
            
        for customer in fallback_customers.get('customers', []):
            aliases = customer.get('aliases', [])
            if customer_name.upper() in [a.upper() for a in aliases]:
                return customer
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Warning: Could not load customer_rules.yaml: {e}")
    
    # Return empty dict if no configuration found
    print(f"Warning: No configuration found for customer '{customer_name}'")
    return {}

def get_canonical_name(customer_name: str) -> str:
    """Get the canonical name for a customer based on their aliases."""
    config = load_customer_config(customer_name)
    return config.get('canonical', customer_name)

def get_customer_aliases(canonical_name: str) -> List[str]:
    """Get all aliases for a canonical customer name."""
    config = load_customer_config(canonical_name)
    return config.get('aliases', [canonical_name])

def build_order_query(customer_name: str, po_number: Optional[str] = None,
                      date_from: Optional[str] = None, 
                      date_to: Optional[str] = None) -> str:
    """
    Build a SQL query for fetching orders based on customer and filters.
    
    Args:
        customer_name: Customer name to filter by
        po_number: Optional PO number to filter by
        date_from: Optional start date (YYYY-MM-DD)
        date_to: Optional end date (YYYY-MM-DD)
        
    Returns:
        SQL query string
    """
    canonical_name = get_canonical_name(customer_name)
    aliases = get_customer_aliases(canonical_name)
    
    # Build the customer filter part of the WHERE clause
    alias_list = ", ".join(f"'{alias}'" for alias in aliases)
    where_clauses = [f"[CUSTOMER NAME] IN ({alias_list})"]
    
    # Add PO filter if provided
    if po_number and po_number.upper() != 'ALL':
        where_clauses.append(f"[PO NUMBER] = '{po_number}'")
    
    # Add date filters if provided
    if date_from:
        where_clauses.append(f"[ORDER DATE] >= '{date_from}'")
    if date_to:
        where_clauses.append(f"[ORDER DATE] <= '{date_to}'")
    
    # Combine all WHERE clauses
    where_clause = " AND ".join(where_clauses)
    
    # Build the final query
    query = f"""
    SELECT * FROM ORDERS_UNIFIED
    WHERE {where_clause}
    """
    return query

def build_shipment_query(customer_name: str, po_number: Optional[str] = None,
                        date_from: Optional[str] = None, 
                        date_to: Optional[str] = None) -> str:
    """
    Build a SQL query for fetching shipments based on customer and filters.
    
    Args:
        customer_name: Customer name to filter by
        po_number: Optional PO number to filter by
        date_from: Optional start date (YYYY-MM-DD)
        date_to: Optional end date (YYYY-MM-DD)
        
    Returns:
        SQL query string
    """
    canonical_name = get_canonical_name(customer_name)
    aliases = get_customer_aliases(canonical_name)
    
    # Build the customer filter part of the WHERE clause
    alias_list = ", ".join(f"'{alias}'" for alias in aliases)
    where_clauses = [f"[Customer] IN ({alias_list})"]
    
    # Add PO filter if provided
    if po_number and po_number.upper() != 'ALL':
        where_clauses.append(f"[Customer_PO] = '{po_number}'")
    
    # Add date filters if provided
    if date_from:
        where_clauses.append(f"[Shipped_Date] >= '{date_from}'")
    if date_to:
        where_clauses.append(f"[Shipped_Date] <= '{date_to}'")
    
    # Combine all WHERE clauses
    where_clause = " AND ".join(where_clauses)
    
    # Build the final query
    query = f"""
    SELECT * FROM FM_orders_shipped
    WHERE {where_clause}
    """
    return query

def aggregate_shipments(shipments: pd.DataFrame, customer_config: Dict) -> pd.DataFrame:
    """
    Aggregate shipments based on customer configuration.
    
    Args:
        shipments: DataFrame containing shipment data
        customer_config: Dictionary containing customer configuration
        
    Returns:
        Aggregated shipments DataFrame
    """
    # Get column mapping from config
    col_mapping = customer_config.get('column_mapping', {})
    
    # Default group columns if not in config
    default_group_cols = ['Customer_PO', 'Style', 'Color', 'Shipping_Method', 'Shipped_Date']
    
    # Get shipment key config if available
    shipment_key_config = customer_config.get('shipment_key_config', {})
    unique_keys = shipment_key_config.get('unique_keys', [])
    
    # Build group columns based on unique keys from config
    if unique_keys:
        # Map order column names to shipment column names using column_mapping
        group_cols = []
        for key in unique_keys:
            # Use the key directly if it's a shipment column
            if key in shipments.columns:
                group_cols.append(key)
            else:
                # Otherwise, try to find a mapping
                for order_col, ship_col in col_mapping.items():
                    if order_col == key and ship_col in shipments.columns:
                        group_cols.append(ship_col)
                        break
    else:
        # Use default group columns that exist in the shipments DataFrame
        group_cols = [c for c in default_group_cols if c in shipments.columns]
    
    # Add any additional columns from extra_checks if they exist
    extra_checks = shipment_key_config.get('extra_checks', [])
    for check in extra_checks:
        if check in shipments.columns and check not in group_cols:
            group_cols.append(check)
    
    print(f"Aggregating shipments by: {group_cols}")
    
    # Perform the aggregation
    present_cols = [c for c in group_cols if c in shipments.columns]
    if 'Qty' in shipments.columns:
        agg = shipments.groupby(present_cols, dropna=False, as_index=False)['Qty'].sum()
    else:
        agg = shipments.groupby(present_cols, dropna=False, as_index=False).size().rename(columns={'size': 'Count'})
    
    return agg

def perform_style_matching(orders: pd.DataFrame, shipments: pd.DataFrame, 
                          customer_config: Dict) -> Tuple[Set, Set, Set]:
    """
    Perform style-level matching between orders and shipments.
    
    Args:
        orders: DataFrame containing order data
        shipments: DataFrame containing shipment data
        customer_config: Dictionary containing customer configuration
        
    Returns:
        Tuple of (matched_styles, unmatched_styles, order_styles)
    """
    # Get column mapping from config
    col_mapping = customer_config.get('column_mapping', {})
    
    # Determine style field names
    order_style_field = next((k for k, v in col_mapping.items() if v == 'Style'), 'CUSTOMER STYLE')
    shipment_style_field = 'Style'
    
    # Extract styles
    order_styles = set()
    shipment_styles = set()
    
    if order_style_field in orders.columns:
        order_styles = set(orders[order_style_field].dropna().astype(str).str.upper())
    
    if shipment_style_field in shipments.columns:
        shipment_styles = set(shipments[shipment_style_field].dropna().astype(str).str.upper())
    
    # Compute matches
    matched_styles = shipment_styles & order_styles
    unmatched_styles = shipment_styles - order_styles
    
    return matched_styles, unmatched_styles, order_styles

def perform_style_color_matching(orders: pd.DataFrame, shipments: pd.DataFrame, 
                               customer_config: Dict) -> Tuple[Set, Set, Set]:
    """
    Perform style+color level matching between orders and shipments.
    
    Args:
        orders: DataFrame containing order data
        shipments: DataFrame containing shipment data
        customer_config: Dictionary containing customer configuration
        
    Returns:
        Tuple of (matched_style_color, unmatched_style_color, order_style_color)
    """
    # Get column mapping from config
    col_mapping = customer_config.get('column_mapping', {})
    
    # Determine field names
    order_style_field = next((k for k, v in col_mapping.items() if v == 'Style'), 'CUSTOMER STYLE')
    order_color_field = next((k for k, v in col_mapping.items() 
                             if v == 'Color' and k in orders.columns), 'CUSTOMER COLOUR DESCRIPTION')
    
    shipment_style_field = 'Style'
    shipment_color_field = 'Color'
    
    # Extract style+color pairs
    order_style_color = set()
    shipment_style_color = set()
    
    if order_style_field in orders.columns and order_color_field in orders.columns:
        order_style_color = set(
            (norm(row[order_style_field]), norm(row[order_color_field]))
            for _, row in orders.iterrows()
        )
    
    if shipment_style_field in shipments.columns and shipment_color_field in shipments.columns:
        shipment_style_color = set(
            (norm(row[shipment_style_field]), norm(row[shipment_color_field]))
            for _, row in shipments.iterrows()
        )
    
    # Compute matches
    matched_style_color = shipment_style_color & order_style_color
    unmatched_style_color = shipment_style_color - order_style_color
    
    return matched_style_color, unmatched_style_color, order_style_color

def perform_fuzzy_matching(unmatched_style_color: Set, order_style_color: Set, 
                          threshold: int = 90) -> List[Dict]:
    """
    Perform fuzzy matching for unmatched style+color pairs.
    
    Args:
        unmatched_style_color: Set of unmatched (style, color) tuples
        order_style_color: Set of order (style, color) tuples
        threshold: Confidence threshold for fuzzy matching (0-100)
        
    Returns:
        List of dictionaries with fuzzy match results
    """
    try:
        from rapidfuzz import process, fuzz
    except ImportError:
        print("Warning: rapidfuzz not installed, skipping fuzzy matching.")
        return []
    
    fuzzy_matches = []
    order_style_color_list = list(order_style_color)
    
    for style, color in unmatched_style_color:
        # Fuzzy match on style and color separately, then combine scores
        best_match = None
        best_score = 0
        
        for o_style, o_color in order_style_color_list:
            style_score = fuzz.ratio(style, o_style)
            color_score = fuzz.ratio(color, o_color)
            avg_score = (style_score + color_score) / 2
            
            if avg_score > best_score:
                best_score = avg_score
                best_match = (o_style, o_color)
        
        match_result = {
            'shipment_style': style,
            'shipment_color': color,
            'match_style': best_match[0] if best_match else None,
            'match_color': best_match[1] if best_match else None,
            'confidence': best_score,
            'match_type': 'fuzzy' if best_score >= threshold else 'none'
        }
        
        fuzzy_matches.append(match_result)
    
    return fuzzy_matches

def build_output_dataframe(shipments: pd.DataFrame, matched_style_color: Set, 
                          fuzzy_matches: List[Dict], 
                          orders: Optional[pd.DataFrame] = None,
                          customer_config: Optional[Dict] = None) -> pd.DataFrame:
    """
    Build output DataFrame with match results.
    
    Args:
        shipments: Aggregated shipments DataFrame
        matched_style_color: Set of exactly matched (style, color) tuples
        fuzzy_matches: List of fuzzy match results
        orders: Optional orders DataFrame for additional matching
        customer_config: Optional customer configuration for field mapping
        
    Returns:
        DataFrame with match results
    """
    match_results = []
    
    # Create a lookup for fuzzy matches
    fuzzy_lookup = {
        (m['shipment_style'], m['shipment_color']): m 
        for m in fuzzy_matches
    }
    
    # Get column mapping if available
    col_mapping = {}
    if customer_config is not None:
        col_mapping = customer_config.get('column_mapping', {})
    
    # Determine shipping method fields
    order_shipping_field = next((k for k, v in col_mapping.items() if v == 'Shipping_Method'), 
                              'PLANNED DELIVERY METHOD')
    
    for idx, row in shipments.iterrows():
        style = norm(row['Style']) if 'Style' in row else ''
        color = norm(row['Color']) if 'Color' in row else ''
        shipping_method = row['Shipping_Method'] if 'Shipping_Method' in row else ''
        
        # Check for exact match
        exact_matched = (style, color) in matched_style_color
        
        # Check for fuzzy match if not exact
        fuzzy_match = None
        if not exact_matched and (style, color) in fuzzy_lookup:
            fuzzy_match = fuzzy_lookup[(style, color)]
        
        # Determine match type
        if exact_matched:
            match_type = 'exact'
            matched = True
            confidence = 100
            match_style = style
            match_color = color
        elif fuzzy_match and fuzzy_match['match_type'] == 'fuzzy':
            match_type = 'fuzzy'
            matched = True
            confidence = fuzzy_match['confidence']
            match_style = fuzzy_match['match_style']
            match_color = fuzzy_match['match_color']
        else:
            match_type = 'none'
            matched = False
            confidence = fuzzy_match['confidence'] if fuzzy_match else 0
            match_style = fuzzy_match['match_style'] if fuzzy_match else None
            match_color = fuzzy_match['match_color'] if fuzzy_match else None
        
        # Check shipping method match if we have orders data
        shipping_method_match = False
        if orders is not None and matched:
            # Find matching order row
            matching_orders = None
            if match_type == 'exact':
                # For exact matches, find by style and color
                if 'CUSTOMER STYLE' in orders.columns and 'CUSTOMER COLOUR DESCRIPTION' in orders.columns:
                    matching_orders = orders[
                        (orders['CUSTOMER STYLE'].astype(str).str.upper().str.strip() == style) &
                        (orders['CUSTOMER COLOUR DESCRIPTION'].astype(str).str.upper().str.strip() == color)
                    ]
            elif match_type == 'fuzzy':
                # For fuzzy matches, find by matched style and color
                if 'CUSTOMER STYLE' in orders.columns and 'CUSTOMER COLOUR DESCRIPTION' in orders.columns:
                    matching_orders = orders[
                        (orders['CUSTOMER STYLE'].astype(str).str.upper().str.strip() == match_style) &
                        (orders['CUSTOMER COLOUR DESCRIPTION'].astype(str).str.upper().str.strip() == match_color)
                    ]
            
            # Check if shipping method matches
            if matching_orders is not None and not matching_orders.empty and order_shipping_field in matching_orders.columns:
                # Get unique shipping methods from matching orders
                order_shipping_methods = set(
                    str(sm).upper().strip() 
                    for sm in matching_orders[order_shipping_field].dropna()
                )
                
                # Check if shipment shipping method is in order shipping methods
                shipping_method_normalized = str(shipping_method).upper().strip()
                shipping_method_match = shipping_method_normalized in order_shipping_methods
                
                # Alternative check for common shipping method variants
                if not shipping_method_match:
                    # Map AIR, SEA, SEA-FB to their common variants
                    method_variants = {
                        'AIR': {'AIR', 'AIR FREIGHT', 'BY AIR', 'AIRFREIGHT'},
                        'SEA': {'SEA', 'OCEAN', 'BY SEA', 'SEAFREIGHT', 'SEA FREIGHT'},
                        'SEA-FB': {'SEA-FB', 'SEA FB', 'SEAFB', 'SEA FREIGHT FB'}
                    }
                    
                    # Check if shipping method matches any variants
                    for base_method, variants in method_variants.items():
                        if shipping_method_normalized in variants:
                            # Check if any order shipping method is in the same variant group
                            for order_method in order_shipping_methods:
                                if order_method in variants:
                                    shipping_method_match = True
                                    break
        
        # Build result row
        result = {
            'Customer_PO': row['Customer_PO'] if 'Customer_PO' in row else np.nan,
            'Style': row['Style'] if 'Style' in row else '',
            'Color': row['Color'] if 'Color' in row else '',
            'Shipping_Method': row['Shipping_Method'] if 'Shipping_Method' in row else '',
            'Shipped_Date': row['Shipped_Date'] if 'Shipped_Date' in row else '',
            'Qty': row['Qty'] if 'Qty' in row else (row['Count'] if 'Count' in row else np.nan),
            'Matched': matched,
            'Match_Type': match_type,
            'Confidence': confidence,
            'Match_Style': match_style,
            'Match_Color': match_color,
            'Shipping_Method_Match': shipping_method_match
        }
        
        match_results.append(result)
    
    return pd.DataFrame(match_results)

def generate_summary_report(match_df: pd.DataFrame, 
                           matched_styles: Set, unmatched_styles: Set, order_styles: Set,
                           matched_style_color: Set, unmatched_style_color: Set, order_style_color: Set) -> str:
    """
    Generate a summary report of matching results.
    
    Args:
        match_df: DataFrame with match results
        matched_styles, unmatched_styles, order_styles: Style matching results
        matched_style_color, unmatched_style_color, order_style_color: Style+color matching results
        
    Returns:
        Summary report as string
    """
    match_counts = match_df['Match_Type'].value_counts()
    exact_matches = match_counts.get('exact', 0)
    fuzzy_matches = match_counts.get('fuzzy', 0)
    no_matches = match_counts.get('none', 0)
    
    report = []
    report.append("# Order-Shipment Reconciliation Summary")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Style Match Summary
    report.append("\n## Style Match Summary")
    report.append(f"- Total shipment styles: {len(matched_styles) + len(unmatched_styles)}")
    report.append(f"- Total order styles: {len(order_styles)}")
    report.append(f"- Matched styles: {len(matched_styles)} ({len(matched_styles)*100/(len(matched_styles) + len(unmatched_styles) or 1):.1f}%)")
    report.append(f"- Unmatched styles: {len(unmatched_styles)} ({len(unmatched_styles)*100/(len(matched_styles) + len(unmatched_styles) or 1):.1f}%)")
    
    # Style+Color Match Summary
    report.append("\n## Style+Color Match Summary")
    report.append(f"- Total shipment style+color pairs: {len(matched_style_color) + len(unmatched_style_color)}")
    report.append(f"- Total order style+color pairs: {len(order_style_color)}")
    report.append(f"- Matched exactly: {exact_matches} ({exact_matches*100/len(match_df) if len(match_df) > 0 else 0:.1f}%)")
    report.append(f"- Matched with fuzzy logic: {fuzzy_matches} ({fuzzy_matches*100/len(match_df) if len(match_df) > 0 else 0:.1f}%)")
    report.append(f"- No matches found: {no_matches} ({no_matches*100/len(match_df) if len(match_df) > 0 else 0:.1f}%)")
    
    # Add confidence information for fuzzy matches
    if fuzzy_matches > 0:
        fuzzy_df = match_df[match_df['Match_Type'] == 'fuzzy']
        avg_confidence = fuzzy_df['Confidence'].mean()
        report.append(f"- Average fuzzy match confidence: {avg_confidence:.1f}%")
    
    # Delivery Method Match Summary
    if 'Shipping_Method' in match_df.columns:
        # Count rows with shipping method match
        if 'Shipping_Method_Match' in match_df.columns:
            shipping_method_matches = match_df['Shipping_Method_Match'].sum()
            shipping_method_match_pct = shipping_method_matches * 100 / len(match_df) if len(match_df) > 0 else 0
        else:
            # If we don't have explicit shipping method match column, assume it matches for all matched rows
            shipping_method_matches = match_df['Matched'].sum()
            shipping_method_match_pct = shipping_method_matches * 100 / len(match_df) if len(match_df) > 0 else 0
            
        report.append("\n## Delivery Method Match Summary")
        report.append(f"- Rows with delivery method match: {shipping_method_matches} ({shipping_method_match_pct:.1f}%)")
        
        # Get unique shipping methods
        shipping_methods = match_df['Shipping_Method'].unique()
        report.append(f"- Unique shipping methods: {len(shipping_methods)}")
        report.append("- Methods: " + ", ".join(f"'{m}'" for m in sorted(shipping_methods) if pd.notna(m)))
    
    # Full Match Summary (Style + Color + Delivery Method)
    # Count rows that have style, color and delivery method all matched
    full_matches = len(match_df[(match_df['Matched'] == True) & 
                              (match_df['Shipping_Method'].notna())])
    full_match_pct = full_matches * 100 / len(match_df) if len(match_df) > 0 else 0
    
    report.append("\n## Full Match Summary (Style + Color + Delivery Method)")
    report.append(f"- Rows with full match: {full_matches} ({full_match_pct:.1f}%)")
    
    # Detailed Fuzzy Match Summary
    if fuzzy_matches > 0:
        report.append("\n## Fuzzy Matches")
        
        # Group fuzzy matches by match field (e.g., color, style)
        fuzzy_df = match_df[match_df['Match_Type'] == 'fuzzy'].copy()
        
        # Identify what was fuzzy matched - if style matches exactly but color doesn't, it's a color match
        fuzzy_df['Fuzzy_Match_Field'] = 'unknown'
        
        # Try to determine which field was fuzzy matched
        if 'Match_Style' in fuzzy_df.columns and 'Style' in fuzzy_df.columns:
            fuzzy_df.loc[fuzzy_df['Style'] == fuzzy_df['Match_Style'], 'Fuzzy_Match_Field'] = 'color'
            fuzzy_df.loc[fuzzy_df['Style'] != fuzzy_df['Match_Style'], 'Fuzzy_Match_Field'] = 'style'
            
            # Both style and color are different
            style_diff = fuzzy_df['Style'] != fuzzy_df['Match_Style']
            color_diff = fuzzy_df['Color'] != fuzzy_df['Match_Color']
            fuzzy_df.loc[style_diff & color_diff, 'Fuzzy_Match_Field'] = 'style+color'
        
        # Group by the fuzzy match field
        field_groups = fuzzy_df.groupby('Fuzzy_Match_Field')
        
        for field, group in field_groups:
            # Add a subheading for each fuzzy match field
            report.append(f"\n### {field.title()} Fuzzy Matches")
            
            # Create a table for this group
            report.append("\n| Style | Color | Shipping Method | Match Style | Match Color | Confidence | Result |")
            report.append("| ----- | ----- | --------------- | ----------- | ----------- | ---------- | ------ |")
            
            # Add each fuzzy match as a row in the table
            for _, row in group.iterrows():
                style = row['Style'] if 'Style' in row else ''
                color = row['Color'] if 'Color' in row else ''
                shipping = row['Shipping_Method'] if 'Shipping_Method' in row else ''
                match_style = row['Match_Style'] if 'Match_Style' in row else ''
                match_color = row['Match_Color'] if 'Match_Color' in row else ''
                confidence = f"{row['Confidence']:.1f}%" if 'Confidence' in row else 'N/A'
                result = "PASS" if row['Confidence'] >= 85 else "FAIL"
                
                report.append(f"| {style} | {color} | {shipping} | {match_style} | {match_color} | {confidence} | {result} |")
    
    # Quantity Summary
    total_shipped_qty = match_df['Qty'].sum()
    matched_qty = match_df[match_df['Matched'] == True]['Qty'].sum()
    unmatched_qty = match_df[match_df['Matched'] == False]['Qty'].sum()
    
    report.append("\n## Quantity Summary")
    report.append(f"- Total shipped quantity: {total_shipped_qty:.0f}")
    report.append(f"- Matched quantity: {matched_qty:.0f} ({matched_qty*100/total_shipped_qty if total_shipped_qty > 0 else 0:.1f}%)")
    report.append(f"- Unmatched quantity: {unmatched_qty:.0f} ({unmatched_qty*100/total_shipped_qty if total_shipped_qty > 0 else 0:.1f}%)")
    
    return "\n".join(report)

def reconcile_orders_shipments(customer_name: str, po_number: Optional[str] = None,
                             date_from: Optional[str] = None, date_to: Optional[str] = None,
                             fuzzy_threshold: int = 90, output_dir: str = './reports') -> Dict:
    """
    Main function to reconcile orders and shipments.
    
    Args:
        customer_name: Customer name to filter by
        po_number: Optional PO number to filter by
        date_from: Optional start date (YYYY-MM-DD)
        date_to: Optional end date (YYYY-MM-DD)
        fuzzy_threshold: Confidence threshold for fuzzy matching (0-100)
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with results and file paths
    """
    print(f"Starting order-shipment reconciliation for {customer_name}, PO: {po_number or 'ALL'}")
    
    # Load customer configuration
    customer_config = load_customer_config(customer_name)
    canonical_name = customer_config.get('canonical', customer_name)
    
    # Build and execute queries
    orders_query = build_order_query(customer_name, po_number, date_from, date_to)
    print("Fetching orders data...")
    orders = run_query(orders_query, db_key='orders')
    print(f"Orders loaded: {len(orders)} rows")
    
    shipments_query = build_shipment_query(customer_name, po_number, date_from, date_to)
    print("Fetching shipments data...")
    shipments = run_query(shipments_query, db_key='orders')
    print(f"Shipments loaded: {len(shipments)} rows")
    
    # Aggregate shipments
    agg_shipments = aggregate_shipments(shipments, customer_config)
    print(f"Aggregated shipments: {len(agg_shipments)} rows")
    
    # Perform style matching
    matched_styles, unmatched_styles, order_styles = perform_style_matching(
        orders, agg_shipments, customer_config
    )
    
    print("\n--- STYLE MATCH SUMMARY ---")
    print(f"Total shipment styles: {len(matched_styles) + len(unmatched_styles)}")
    print(f"Total order styles: {len(order_styles)}")
    print(f"Matched styles: {len(matched_styles)}")
    print(f"Unmatched styles: {len(unmatched_styles)}")
    
    # Perform style+color matching
    matched_style_color, unmatched_style_color, order_style_color = perform_style_color_matching(
        orders, agg_shipments, customer_config
    )
    
    print("\n--- STYLE + COLOR MATCH SUMMARY ---")
    print(f"Total shipment style+color pairs: {len(matched_style_color) + len(unmatched_style_color)}")
    print(f"Total order style+color pairs: {len(order_style_color)}")
    print(f"Matched style+color pairs: {len(matched_style_color)}")
    print(f"Unmatched style+color pairs: {len(unmatched_style_color)}")
    
    # Perform fuzzy matching for unmatched style+color pairs
    fuzzy_matches = []
    if len(unmatched_style_color) > 0:
        print("\n--- FUZZY MATCH ATTEMPT FOR UNMATCHED SHIPMENT STYLE+COLOR ---")
        fuzzy_matches = perform_fuzzy_matching(
            unmatched_style_color, order_style_color, threshold=fuzzy_threshold
        )
        
        high_confidence_matches = [m for m in fuzzy_matches if m['match_type'] == 'fuzzy']
        print(f"Found {len(high_confidence_matches)} high-confidence fuzzy matches (≥{fuzzy_threshold}%)")
        
        for match in high_confidence_matches:
            print(f"Fuzzy match found for shipment (Style: '{match['shipment_style']}', Color: '{match['shipment_color']}'):")
            print(f"  → Closest order (Style: '{match['match_style']}', Color: '{match['match_color']}') with confidence {match['confidence']:.1f}%")
    
    # Build output DataFrame
    match_df = build_output_dataframe(agg_shipments, matched_style_color, fuzzy_matches, orders, customer_config)
    print("\n--- MATCH OUTPUT DATAFRAME ---")
    print(f"DataFrame shape: {match_df.shape}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    po_str = po_number if po_number and po_number.upper() != 'ALL' else 'ALL_POs'
    base_filename = f"{canonical_name.replace(' ', '_')}_{po_str}_{timestamp}"
    
    csv_path = os.path.join(output_dir, f"{base_filename}_matches.csv")
    report_path = os.path.join(output_dir, f"{base_filename}_summary.md")
    
    # Save CSV output
    match_df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"Match results saved to {csv_path}")
    
    # Generate and save summary report
    summary_report = generate_summary_report(
        match_df, matched_styles, unmatched_styles, order_styles,
        matched_style_color, unmatched_style_color, order_style_color
    )
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(summary_report)
    
    print(f"Summary report saved to {report_path}")
    
    # Return results
    return {
        'match_df': match_df,
        'csv_path': csv_path,
        'report_path': report_path,
        'matched_styles': matched_styles,
        'unmatched_styles': unmatched_styles,
        'matched_style_color': matched_style_color,
        'unmatched_style_color': unmatched_style_color,
        'fuzzy_matches': fuzzy_matches
    }

def main():
    """Main function to parse arguments and run reconciliation."""
    parser = argparse.ArgumentParser(description='Reconcile orders and shipments.')
    parser.add_argument('--customer', required=True, help='Customer name')
    parser.add_argument('--po', default=None, help='PO number (optional, use "ALL" for all POs)')
    parser.add_argument('--date-from', default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date-to', default=None, help='End date (YYYY-MM-DD)')
    parser.add_argument('--fuzzy-threshold', type=int, default=90, 
                        help='Confidence threshold for fuzzy matching (0-100)')
    parser.add_argument('--output-dir', default='./reports', 
                        help='Directory to save output files')
    
    args = parser.parse_args()
    
    # Run reconciliation
    reconcile_orders_shipments(
        customer_name=args.customer,
        po_number=args.po,
        date_from=args.date_from,
        date_to=args.date_to,
        fuzzy_threshold=args.fuzzy_threshold,
        output_dir=args.output_dir
    )

# For direct testing, run reconciliation for Greyson PO 4755
if __name__ == "__main__":
    # If no arguments provided, run with default test case
    import sys
    if len(sys.argv) == 1:
        print("No arguments provided. Running test case for Greyson PO 4755...")
        reconcile_orders_shipments(
            customer_name="GREYSON",
            po_number="4755",
            fuzzy_threshold=90,
            output_dir='./reports'
        )
    else:
        main()
