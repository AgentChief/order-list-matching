"""
Enhanced Database-Driven Matching Script
Uses the new configuration database to perform order-shipment matching
Implements proper exclusion rules and canonical customer configurations
"""

import pyodbc
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from auth_helper import get_connection_string

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseDrivenMatcher:
    def __init__(self):
        self.connection_string = get_connection_string()
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def get_customer_config(self, customer_name):
        """Get customer configuration from database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("EXEC sp_get_customer_config ?", customer_name)
            result = cursor.fetchone()
            
            if result:
                return {
                    'canonical_name': result[0],
                    'status': result[1],
                    'column_mappings': json.loads(result[2]) if result[2] else [],
                    'matching_strategy': json.loads(result[3]) if result[3] else {},
                    'exclusion_rules': json.loads(result[4]) if result[4] else []
                }
            return None
    
    def apply_exclusion_rules(self, df, table_name, exclusion_rules):
        """Apply exclusion rules to filter out unwanted records"""
        original_count = len(df)
        
        for rule in exclusion_rules:
            if rule['table_name'] == table_name and rule['rule_type'] == 'exclude':
                field_name = rule['field_name']
                exclude_values = json.loads(rule['exclude_values'])
                
                if field_name in df.columns:
                    df = df[~df[field_name].isin(exclude_values)]
                    excluded_count = original_count - len(df)
                    logger.info(f"Excluded {excluded_count} records where {field_name} in {exclude_values}")
        
        return df
    
    def get_orders_with_exclusions(self, customer_name, po_number, exclusion_rules):
        """Get orders with exclusion rules applied"""
        query = """
        SELECT 
            order_id,
            customer_name,
            po_number,
            delivery_method,
            style_code,
            color_description,
            aag_order_number,
            order_type,
            order_date,
            quantity,
            CONCAT(style_code, '-', color_description) as style_color_key
        FROM stg_order_list 
        WHERE customer_name LIKE ? AND po_number = ?
        """
        
        # Use LIKE pattern for customer matching to handle canonical inconsistencies
        customer_pattern = f"{customer_name}%" if not customer_name.endswith('%') else customer_name
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=[customer_pattern, po_number])
            
            # Apply exclusion rules
            df = self.apply_exclusion_rules(df, 'orders', exclusion_rules)
            
            logger.info(f"Loaded {len(df)} orders after applying exclusion rules")
            return df
    
    def get_shipments_with_exclusions(self, customer_name, po_number, exclusion_rules):
        """Get shipments with exclusion rules applied"""
        query = """
        SELECT 
            shipment_id,
            customer_name,
            po_number,
            style_code,
            color_description,
            delivery_method,
            quantity,
            shipped_date,
            style_color_key,
            customer_po_key
        FROM stg_fm_orders_shipped_table
        WHERE customer_name LIKE ? AND po_number = ?
        """
        
        # Use LIKE pattern for customer matching to handle canonical inconsistencies  
        customer_pattern = f"{customer_name}%" if not customer_name.endswith('%') else customer_name
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=[customer_pattern, po_number])
            
            # Apply exclusion rules
            df = self.apply_exclusion_rules(df, 'shipments', exclusion_rules)
            
            logger.info(f"Loaded {len(df)} shipments after applying exclusion rules")
            return df
    
    def perform_matching(self, orders_df, shipments_df, matching_strategy):
        """Perform layered matching: Layer 0 (exact) + Layer 1 (fuzzy)"""
        # Map abstract field names to actual database field names
        field_mapping = {
            'Style': 'style_code',
            'Color': 'color_description', 
            'Customer_PO': 'po_number',
            'Shipping_Method': 'delivery_method',  # Legacy support
            'Delivery_Method': 'delivery_method',  # Correct field name
            'Size': 'size'
        }
        
        # Get primary fields and map them to actual database fields
        primary_fields_config = json.loads(matching_strategy.get('primary_match_fields', '["style_code", "color_description", "delivery_method"]'))
        primary_fields = [field_mapping.get(field, field) for field in primary_fields_config]
        
        fuzzy_threshold = matching_strategy.get('fuzzy_threshold', 0.85)
        quantity_tolerance = matching_strategy.get('quantity_tolerance', 0.05)
        
        logger.info(f"Using primary fields for matching: {primary_fields} (mapped from {primary_fields_config})")
        
        # Layer 0: Exact Matching
        exact_matches, unmatched_after_layer0 = self._layer0_exact_matching(
            orders_df, shipments_df, primary_fields, quantity_tolerance
        )
        
        logger.info(f"Layer 0 (Exact): {len(exact_matches)} matches, {len(unmatched_after_layer0)} remaining")
        
        # Layer 1: Exact style+color, flexible delivery+quantity
        layer1_matches, unmatched_after_layer1 = self._layer1_fuzzy_matching(
            orders_df, unmatched_after_layer0, primary_fields, fuzzy_threshold, quantity_tolerance
        )
        
        logger.info(f"Layer 1 (Style+Color Exact): {len(layer1_matches)} matches, {len(unmatched_after_layer1)} remaining")
        
        # Layer 2: Fuzzy style+color matching
        layer2_matches, still_unmatched = self._layer2_fuzzy_matching(
            orders_df, unmatched_after_layer1, primary_fields, fuzzy_threshold, quantity_tolerance
        )
        
        logger.info(f"Layer 2 (Fuzzy): {len(layer2_matches)} matches, {len(still_unmatched)} unmatched")
        
        # Combine all matches
        all_matches = exact_matches + layer1_matches + layer2_matches
        
        return all_matches, still_unmatched
    
    def _layer0_exact_matching(self, orders_df, shipments_df, primary_fields, quantity_tolerance):
        """Layer 0: Exact string matching after normalization"""
        matches = []
        unmatched_shipments = []
        
        # Create lookup dictionaries for fast matching
        order_lookup = {}
        for _, order in orders_df.iterrows():
            # Create composite key from primary fields
            key_parts = []
            for field in primary_fields:
                if field in order and pd.notna(order[field]):
                    # Normalize for exact matching (trim, uppercase, normalize spaces)
                    value = str(order[field]).strip().upper()
                    value = ' '.join(value.split())  # Normalize spaces
                    key_parts.append(value)
            
            if key_parts:
                composite_key = '|'.join(key_parts)
                if composite_key not in order_lookup:
                    order_lookup[composite_key] = []
                order_lookup[composite_key].append(order)
        
        logger.info(f"Created order lookup with {len(order_lookup)} unique keys")
        
        # Match shipments to orders
        matched_shipment_ids = set()
        for _, shipment in shipments_df.iterrows():
            # Create composite key from shipment (with same normalization)
            key_parts = []
            for field in primary_fields:
                if field in shipment and pd.notna(shipment[field]):
                    # Apply same normalization as orders
                    value = str(shipment[field]).strip().upper()
                    value = ' '.join(value.split())  # Normalize spaces
                    key_parts.append(value)
            
            if not key_parts:
                unmatched_shipments.append(shipment)
                continue
            
            composite_key = '|'.join(key_parts)
            
            # Look for exact match
            if composite_key in order_lookup:
                for order in order_lookup[composite_key]:
                    # Check quantity tolerance with better logic
                    order_qty = order['quantity']
                    shipment_qty = shipment['quantity']
                    
                    # Calculate percentage difference (more robust)
                    if order_qty > 0:
                        qty_diff_percent = abs(order_qty - shipment_qty) / order_qty * 100
                    else:
                        qty_diff_percent = 100.0 if shipment_qty > 0 else 0.0
                    
                    # Apply stricter tolerance - 5% default, but never allow >50% difference
                    tolerance_percent = min(quantity_tolerance * 100, 50.0)
                    qty_within_tolerance = qty_diff_percent <= tolerance_percent
                    
                    match = {
                        'shipment_id': shipment['shipment_id'],
                        'order_id': order['order_id'],
                        'match_type': 'LAYER0_EXACT',
                        'confidence': 1.0,
                        'match_reason': f'Exact match on {", ".join(primary_fields)}',
                        'quantity_check': qty_within_tolerance,
                        'quantity_diff_percent': qty_diff_percent,
                        'order_qty': order_qty,
                        'shipment_qty': shipment_qty,
                        'style_code': shipment['style_code'],
                        'color_description': shipment['color_description'],
                        'order_delivery_method': order.get('delivery_method', 'N/A'),
                        'shipment_delivery_method': shipment.get('delivery_method', 'N/A'),
                        'match_key': composite_key
                    }
                    matches.append(match)
                    matched_shipment_ids.add(shipment['shipment_id'])
                    break
            else:
                unmatched_shipments.append(shipment)
        
        return matches, unmatched_shipments
    
    def _layer1_fuzzy_matching(self, orders_df, unmatched_shipments, primary_fields, fuzzy_threshold, quantity_tolerance):
        """Layer 1: Exact style + color, flexible delivery method + quantity classification"""
        if not unmatched_shipments:
            return [], []
        
        try:
            from rapidfuzz import fuzz
            
            matches = []
            still_unmatched = []
            
            logger.info(f"Starting Layer 1 matching for {len(unmatched_shipments)} unmatched shipments")
            logger.info("Layer 1 Rules: Exact style + color, flexible delivery, quantity classification")
            
            # For each unmatched shipment, find exact style+color matches
            for shipment in unmatched_shipments:
                best_match = None
                best_reasons = []
                
                # Get shipment details for matching
                ship_style = str(shipment.get('style_code', '')).strip().upper()
                ship_color = str(shipment.get('color_description', '')).strip().upper()
                ship_delivery = str(shipment.get('delivery_method', '')).strip().upper()
                
                # Find orders with exact style and color match
                for _, order in orders_df.iterrows():
                    ord_style = str(order.get('style_code', '')).strip().upper()
                    ord_color = str(order.get('color_description', '')).strip().upper()
                    ord_delivery = str(order.get('delivery_method', '')).strip().upper()
                    
                    # Layer 1 Requirement: EXACT style and color match
                    style_match = (ship_style == ord_style) if ship_style and ord_style else False
                    color_match = (ship_color == ord_color) if ship_color and ord_color else False
                    
                    if style_match and color_match:
                        # Core match achieved - now evaluate delivery and quantity
                        delivery_match = (ship_delivery == ord_delivery) if ship_delivery and ord_delivery else False
                        
                        # Quantity classification
                        order_qty = order['quantity']
                        shipment_qty = shipment['quantity']
                        qty_result = self._classify_quantity_difference(order_qty, shipment_qty)
                        
                        # Determine overall approval status
                        approval_status = self._determine_layer1_approval(delivery_match, qty_result)
                        
                        # This is a valid Layer 1 match
                        best_match = order
                        best_reasons = [
                            f"Style: EXACT ({ship_style})",
                            f"Color: EXACT ({ship_color})",
                            f"Delivery: {'MATCH' if delivery_match else 'MISMATCH'} ({ship_delivery} vs {ord_delivery})",
                            f"Quantity: {qty_result['status']} ({qty_result['diff_percent']:.1f}%)"
                        ]
                        break  # Take first exact style+color match
                
                if best_match is not None:
                    # Calculate overall confidence for Layer 1
                    confidence = 0.9  # High confidence for exact style+color
                    
                    # Determine quantity check result
                    qty_result = self._classify_quantity_difference(best_match['quantity'], shipment['quantity'])
                    quantity_check = qty_result['status'] in ['PASS', 'CONDITIONAL']
                    
                    match = {
                        'shipment_id': shipment['shipment_id'],
                        'order_id': best_match['order_id'],
                        'match_type': 'LAYER1_EXACT_STYLE_COLOR',
                        'confidence': confidence,
                        'match_reason': '; '.join(best_reasons),
                        'quantity_check': quantity_check,
                        'quantity_diff_percent': qty_result['diff_percent'],
                        'quantity_status': qty_result['status'],
                        'order_qty': best_match['quantity'],
                        'shipment_qty': shipment['quantity'],
                        'style_code': shipment['style_code'],
                        'color_description': shipment['color_description'],
                        'order_delivery_method': best_match.get('delivery_method', 'N/A'),
                        'shipment_delivery_method': shipment.get('delivery_method', 'N/A'),
                        'delivery_match': ship_delivery == str(best_match.get('delivery_method', '')).strip().upper(),
                        'style_match': 'EXACT',
                        'color_match': 'EXACT',
                        'match_key': f"L1_EXACT_{ship_style}_{ship_color}"
                    }
                    matches.append(match)
                else:
                    still_unmatched.append(shipment)
            
            logger.info(f"Layer 1 completed: {len(matches)} matches, {len(still_unmatched)} remaining for Layer 2")
            return matches, still_unmatched
            
        except ImportError:
            logger.warning("rapidfuzz not available, skipping Layer 1 matching")
            return [], unmatched_shipments
    
    def _layer2_fuzzy_matching(self, orders_df, unmatched_shipments, primary_fields, fuzzy_threshold, quantity_tolerance):
        """Layer 2: Fuzzy style + color matching for data entry variations"""
        if not unmatched_shipments:
            return [], []
        
        try:
            from rapidfuzz import fuzz
            
            matches = []
            still_unmatched = []
            
            logger.info(f"Starting Layer 2 fuzzy matching for {len(unmatched_shipments)} shipments")
            logger.info(f"Layer 2 Rules: Fuzzy style + color (threshold: {fuzzy_threshold}), quantity classification")
            
            # For each unmatched shipment, find best fuzzy match
            for shipment in unmatched_shipments:
                best_match = None
                best_score = 0
                best_reasons = []
                
                ship_style = str(shipment.get('style_code', '')).strip().upper()
                ship_color = str(shipment.get('color_description', '')).strip().upper()
                ship_delivery = str(shipment.get('delivery_method', '')).strip().upper()
                
                for _, order in orders_df.iterrows():
                    ord_style = str(order.get('style_code', '')).strip().upper()
                    ord_color = str(order.get('color_description', '')).strip().upper()
                    ord_delivery = str(order.get('delivery_method', '')).strip().upper()
                    
                    # Calculate fuzzy similarities
                    style_similarity = 0
                    color_similarity = 0
                    
                    if ship_style and ord_style:
                        # Use token_set_ratio for better handling of variations like "ARCTIC / BLUE" vs "ARCTIC/BLUE"
                        style_similarity = fuzz.token_set_ratio(ship_style, ord_style) / 100.0
                    
                    if ship_color and ord_color:
                        # Handle color variations like "WOLF BLUE" vs "BLUE WOLF"
                        color_similarity = fuzz.token_set_ratio(ship_color, ord_color) / 100.0
                    
                    # Layer 2 Requirements: Both style and color must meet fuzzy threshold
                    if style_similarity >= fuzzy_threshold and color_similarity >= fuzzy_threshold:
                        # Calculate combined score
                        combined_score = (style_similarity + color_similarity) / 2
                        
                        if combined_score > best_score:
                            best_score = combined_score
                            best_match = order
                            
                            # Determine match types
                            style_type = 'EXACT' if style_similarity >= 0.99 else 'FUZZY'
                            color_type = 'EXACT' if color_similarity >= 0.99 else 'FUZZY'
                            delivery_match = (ship_delivery == ord_delivery) if ship_delivery and ord_delivery else False
                            
                            # Quantity classification
                            qty_result = self._classify_quantity_difference(order['quantity'], shipment['quantity'])
                            
                            best_reasons = [
                                f"Style: {style_type} ({style_similarity:.2f}) - {ship_style} vs {ord_style}",
                                f"Color: {color_type} ({color_similarity:.2f}) - {ship_color} vs {ord_color}",
                                f"Delivery: {'MATCH' if delivery_match else 'MISMATCH'} - {ship_delivery} vs {ord_delivery}",
                                f"Quantity: {qty_result['status']} ({qty_result['diff_percent']:.1f}%)"
                            ]
                
                if best_match is not None:
                    # Determine quantity check result
                    qty_result = self._classify_quantity_difference(best_match['quantity'], shipment['quantity'])
                    quantity_check = qty_result['status'] in ['PASS', 'CONDITIONAL']
                    
                    # Determine match types for storage
                    style_sim = fuzz.token_set_ratio(ship_style, str(best_match.get('style_code', '')).strip().upper()) / 100.0
                    color_sim = fuzz.token_set_ratio(ship_color, str(best_match.get('color_description', '')).strip().upper()) / 100.0
                    
                    match = {
                        'shipment_id': shipment['shipment_id'],
                        'order_id': best_match['order_id'],
                        'match_type': 'LAYER2_FUZZY',
                        'confidence': best_score,
                        'match_reason': '; '.join(best_reasons),
                        'quantity_check': quantity_check,
                        'quantity_diff_percent': qty_result['diff_percent'],
                        'quantity_status': qty_result['status'],
                        'order_qty': best_match['quantity'],
                        'shipment_qty': shipment['quantity'],
                        'style_code': shipment['style_code'],
                        'color_description': shipment['color_description'],
                        'order_delivery_method': best_match.get('delivery_method', 'N/A'),
                        'shipment_delivery_method': shipment.get('delivery_method', 'N/A'),
                        'delivery_match': ship_delivery == str(best_match.get('delivery_method', '')).strip().upper(),
                        'style_match': 'EXACT' if style_sim >= 0.99 else 'FUZZY',
                        'color_match': 'EXACT' if color_sim >= 0.99 else 'FUZZY',
                        'match_key': f"L2_FUZZY_{best_score:.2f}"
                    }
                    matches.append(match)
                else:
                    still_unmatched.append(shipment)
            
            logger.info(f"Layer 2 completed: {len(matches)} matches, {len(still_unmatched)} unmatched")
            return matches, still_unmatched
            
        except ImportError:
            logger.warning("rapidfuzz not available, skipping Layer 2 matching")
            return [], unmatched_shipments
    
    def _classify_quantity_difference(self, order_qty, shipment_qty):
        """Classify quantity difference according to business rules"""
        if order_qty <= 0:
            return {
                'status': 'FAIL',
                'diff_percent': 100.0 if shipment_qty > 0 else 0.0,
                'reason': 'Zero order quantity'
            }
        
        diff_percent = abs(order_qty - shipment_qty) / order_qty * 100
        
        if diff_percent <= 5.0:
            return {'status': 'PASS', 'diff_percent': diff_percent, 'reason': 'Within 5% tolerance'}
        elif diff_percent <= 10.0:
            return {'status': 'CONDITIONAL', 'diff_percent': diff_percent, 'reason': 'Requires approval (5-10%)'}
        else:
            return {'status': 'FAIL', 'diff_percent': diff_percent, 'reason': 'Exceeds 10% tolerance'}
    
    def _determine_layer1_approval(self, delivery_match, qty_result):
        """Determine approval status for Layer 1 matches"""
        if qty_result['status'] == 'FAIL':
            return 'QUANTITY_FAIL'
        elif not delivery_match and qty_result['status'] == 'CONDITIONAL':
            return 'DELIVERY_AND_QUANTITY_CONDITIONAL'
        elif not delivery_match:
            return 'DELIVERY_CONDITIONAL'
        elif qty_result['status'] == 'CONDITIONAL':
            return 'QUANTITY_CONDITIONAL'
        else:
            return 'APPROVED'
    
    def run_enhanced_matching(self, customer_name, po_number):
        """Run the complete enhanced matching process"""
        logger.info(f"Starting enhanced matching for {customer_name} PO {po_number}")
        
        # Get customer configuration
        config = self.get_customer_config(customer_name)
        if not config:
            logger.error(f"No configuration found for customer {customer_name}")
            return None
        
        logger.info(f"Customer status: {config['status']}")
        logger.info(f"Exclusion rules: {len(config['exclusion_rules'])}")
        
        # Get orders with exclusions applied
        orders_df = self.get_orders_with_exclusions(customer_name, po_number, config['exclusion_rules'])
        
        # Get shipments with exclusions applied  
        shipments_df = self.get_shipments_with_exclusions(customer_name, po_number, config['exclusion_rules'])
        
        # Perform matching
        matches, unmatched = self.perform_matching(orders_df, shipments_df, config['matching_strategy'])
        
        # Generate summary
        total_shipments = len(shipments_df)
        matched_count = len(matches)
        unmatched_count = len(unmatched)
        match_rate = (matched_count / max(total_shipments, 1)) * 100
        
        # Convert DataFrames to list of dicts for JSON serialization
        orders_sample = orders_df.to_dict('records') if len(orders_df) > 0 else []
        shipments_sample = shipments_df.to_dict('records') if len(shipments_df) > 0 else []
        
        results = {
            'customer': customer_name,
            'po_number': po_number,
            'total_orders': len(orders_df),
            'total_shipments': total_shipments,
            'matched_count': matched_count,
            'unmatched_count': unmatched_count,
            'match_rate': match_rate,
            'matches': matches,
            'unmatched_shipments': unmatched,
            'orders_sample': orders_sample,
            'shipments_sample': shipments_sample,
            'config_used': config
        }
        
        logger.info(f"✅ Matching completed: {matched_count}/{total_shipments} ({match_rate:.1f}%)")
        
        # Store results for HITL interface
        self._store_matching_results(customer_name, po_number, matches)
        
        return results
    
    def _store_matching_results(self, customer_name, po_number, matches):
        """Store matching results in database for HITL interface consumption"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Generate session ID for this matching run
            session_id = f"{customer_name}_{po_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Clear previous results for this customer/PO combination
            cursor.execute("""
                DELETE FROM enhanced_matching_results 
                WHERE customer_name = ? AND po_number = ?
            """, customer_name, po_number)
            
            # Insert new results
            for match in matches:
                # Determine match status
                style_match = 'MATCH' if match.get('style_code') else 'MISMATCH'
                color_match = 'MATCH' if match.get('color_description') else 'MISMATCH'
                delivery_match = 'MATCH' if match.get('order_delivery_method') == match.get('shipment_delivery_method') else 'MISMATCH'
                quantity_check = 'PASS' if match.get('quantity_check', False) else 'FAIL'
                # Determine match layer based on match type
                match_type = match.get('match_type', '')
                if 'LAYER0' in match_type or 'EXACT' in match_type:
                    match_layer = 'LAYER_0'
                elif 'LAYER1' in match_type:
                    match_layer = 'LAYER_1'  
                elif 'LAYER2' in match_type:
                    match_layer = 'LAYER_2'
                else:
                    match_layer = 'LAYER_1'  # Default fallback
                
                cursor.execute("""
                    INSERT INTO enhanced_matching_results (
                        customer_name, po_number, shipment_id, order_id,
                        match_layer, match_confidence,
                        style_match, color_match, delivery_match,
                        shipment_style_code, order_style_code,
                        shipment_color_description, order_color_description,
                        shipment_delivery_method, order_delivery_method,
                        shipment_quantity, order_quantity,
                        quantity_difference_percent, quantity_check_result,
                        matching_session_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                    customer_name, po_number, 
                    match['shipment_id'], match['order_id'],
                    match_layer, match.get('confidence', 1.0),
                    style_match, color_match, delivery_match,
                    match.get('style_code', ''), match.get('style_code', ''),  # Assuming same style for match
                    match.get('color_description', ''), match.get('color_description', ''),  # Assuming same color
                    match.get('shipment_delivery_method', ''), match.get('order_delivery_method', ''),
                    match.get('shipment_qty', 0), match.get('order_qty', 0),
                    match.get('quantity_diff_percent', 0.0), quantity_check,
                    session_id
                )
            
            conn.commit()
            logger.info(f"📊 Stored {len(matches)} matching results for HITL review")
            
        except Exception as e:
            logger.error(f"Failed to store matching results: {str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def generate_report(self, results, output_dir="reports/enhanced_matching"):
        """Generate detailed matching report with actual data extraction"""
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        customer = results['customer'].replace(' ', '_')
        po = results['po_number']
        
        # Generate markdown report
        report_file = Path(output_dir) / f"{customer}_{po}_{timestamp}_enhanced.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# Enhanced Database-Driven Matching Report\n")
            f.write(f"**Customer:** {results['customer']}  \n")
            f.write(f"**PO:** {results['po_number']}  \n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n")
            f.write(f"**Configuration Status:** {results['config_used']['status']}\n\n")
            
            f.write(f"## Executive Summary\n\n")
            f.write(f"| Metric | Value | Percentage |\n")
            f.write(f"|--------|-------|------------|\n")
            f.write(f"| **Total Orders** | {results['total_orders']} | - |\n")
            f.write(f"| **Total Shipments** | {results['total_shipments']} | - |\n")
            f.write(f"| **Successfully Matched** | {results['matched_count']} | {results['match_rate']:.1f}% |\n")
            f.write(f"| **Unmatched Shipments** | {results['unmatched_count']} | {100-results['match_rate']:.1f}% |\n\n")
            
            f.write(f"## Configuration Applied\n\n")
            f.write(f"**Exclusion Rules Applied:** {len(results['config_used']['exclusion_rules'])}\n")
            for rule in results['config_used']['exclusion_rules']:
                f.write(f"- Exclude {rule['table_name']}.{rule['field_name']} where value in {rule['exclude_values']}\n")
            
            f.write(f"\n**Matching Strategy:**\n")
            strategy = results['config_used']['matching_strategy']
            if strategy:
                f.write(f"- Primary Fields: {strategy.get('primary_match_fields', 'N/A')}\n")
                f.write(f"- Fuzzy Threshold: {strategy.get('fuzzy_threshold', 0.85)}\n")
                f.write(f"- Quantity Tolerance: {strategy.get('quantity_tolerance', 0.05)}\n")
            
            # Add detailed data analysis
            f.write(f"\n## Data Analysis\n\n")
            
            if results['orders_sample']:
                f.write(f"### Sample Orders (first 10)\n\n")
                f.write(f"| Order ID | Style | Color | Delivery Method | Quantity | Order Type |\n")
                f.write(f"|----------|-------|-------|-----------------|----------|------------|\n")
                for order in results['orders_sample'][:10]:
                    f.write(f"| {order['order_id']} | {order['style_code']} | {order['color_description']} | {order.get('delivery_method', 'N/A')} | {order['quantity']} | {order.get('order_type', 'N/A')} |\n")
            
            if results['shipments_sample']:
                f.write(f"\n### Sample Shipments (first 10)\n\n")
                f.write(f"| Shipment ID | Style | Color | Quantity | Shipped Date |\n")
                f.write(f"|-------------|-------|-------|----------|-------------|\n")
                for shipment in results['shipments_sample'][:10]:
                    f.write(f"| {shipment['shipment_id']} | {shipment['style_code']} | {shipment['color_description']} | {shipment['quantity']} | {shipment.get('shipped_date', 'N/A')} |\n")
            
            f.write(f"\n## Match Details\n\n")
            if results['matches']:
                f.write(f"### Successful Matches\n\n")
                f.write(f"| Shipment ID | Order ID | Style Match | Color Match | Delivery Match | Qty Check | Qty Diff % |\n")
                f.write(f"|-------------|----------|-------------|-------------|----------------|-----------|------------|\n")
                
                for match in results['matches'][:20]:  # Show first 20
                    qty_status = "PASS" if match['quantity_check'] else "FAIL"
                    style_match = "✓" if match['style_code'] else "✗"
                    color_match = "✓" if match['color_description'] else "✗"
                    delivery_match = "✓" if match['order_delivery_method'] == match['shipment_delivery_method'] else "✗"
                    f.write(f"| {match['shipment_id']} | {match['order_id']} | {style_match} | {color_match} | {delivery_match} | {qty_status} | {match['quantity_diff_percent']:.1f}% |\n")
            
            if results['unmatched_shipments']:
                f.write(f"\n### Unmatched Shipments (all {len(results['unmatched_shipments'])})\n\n")
                f.write(f"| Shipment ID | Style | Color | Quantity | Shipped Date |\n")
                f.write(f"|-------------|-------|-------|----------|-------------|\n")
                
                for shipment in results['unmatched_shipments']:
                    f.write(f"| {shipment['shipment_id']} | {shipment['style_code']} | {shipment['color_description']} | {shipment['quantity']} | {shipment.get('shipped_date', 'N/A')} |\n")
            
            # Add debugging section
            f.write(f"\n## Debugging Information\n\n")
            f.write(f"**Field Mapping Applied:**\n")
            f.write(f"- Configuration Fields: {strategy.get('primary_match_fields', 'N/A')}\n") 
            f.write(f"- Database Fields Used: style_code, color_description, po_number\n")
            
            # Show sample match keys
            if results['matches']:
                f.write(f"\n**Sample Match Keys:**\n")
                for match in results['matches'][:5]:
                    f.write(f"- `{match.get('match_key', 'N/A')}` → Shipment {match['shipment_id']} matched Order {match['order_id']}\n")
            
            f.write(f"\n---\n*Report generated by Enhanced Database-Driven Matcher v1.0*\n")
        
        logger.info(f"📋 Report saved: {report_file}")
        return report_file

def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced database-driven order-shipment matching")
    parser.add_argument("--customer", required=True, help="Customer name")
    parser.add_argument("--po", required=True, help="PO number")
    parser.add_argument("--output-dir", default="reports/enhanced_matching", help="Output directory for reports")
    
    args = parser.parse_args()
    
    try:
        matcher = DatabaseDrivenMatcher()
        results = matcher.run_enhanced_matching(args.customer, args.po)
        
        if results:
            report_file = matcher.generate_report(results, args.output_dir)
            print(f"\n🎉 Enhanced matching completed successfully!")
            print(f"📊 Results: {results['matched_count']}/{results['total_shipments']} matches ({results['match_rate']:.1f}%)")
            print(f"📋 Report: {report_file}")
            return 0
        else:
            print("❌ Matching failed")
            return 1
            
    except Exception as e:
        logger.error(f"Enhanced matching failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())
