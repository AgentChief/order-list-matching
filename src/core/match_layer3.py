#!/usr/bin/env python3
"""
Layer 3 Matching: Quantity Variance Resolution
Links additional ACTIVE orders to shipments to resolve >10% quantity variances
"""
import pandas as pd
import pyodbc
from typing import Dict, List, Tuple
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from auth_helper import get_connection_string

class Layer3Matcher:
    """
    Layer 3 matching focuses on resolving quantity variances by linking
    additional ACTIVE orders to existing matched shipments.
    
    Target: Shipments with quantity_check_result = 'FAIL' (>10% variance)
    Solution: Find unmatched ACTIVE orders with same style/color to close gaps
    """
    
    def __init__(self):
        self.connection_string = get_connection_string()
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def get_quantity_failures(self, customer: str, po_number: str) -> pd.DataFrame:
        """
        Get all shipments with quantity failures (>10% variance)
        """
        with self.get_connection() as conn:
            query = """
            SELECT 
                emr.shipment_id,
                emr.order_id as current_order_id,
                emr.shipment_style_code,
                emr.shipment_color_description,
                emr.shipment_quantity,
                emr.order_quantity as current_order_quantity,
                emr.quantity_difference_percent,
                emr.shipment_delivery_method,
                emr.order_delivery_method
            FROM enhanced_matching_results emr
            WHERE emr.customer_name = ?
            AND emr.po_number = ?
            AND emr.quantity_check_result = 'FAIL'
            ORDER BY ABS(emr.quantity_difference_percent) DESC
            """
            return pd.read_sql(query, conn, params=[customer, po_number])
    
    def get_unmatched_orders(self, customer: str, po_number: str) -> pd.DataFrame:
        """
        Get all ACTIVE orders that haven't been matched to any shipment
        """
        with self.get_connection() as conn:
            query = """
            SELECT 
                o.order_id,
                o.style_code,
                o.color_description,
                o.quantity,
                o.delivery_method
            FROM stg_order_list o
            LEFT JOIN enhanced_matching_results emr ON o.order_id = emr.order_id
            WHERE o.customer_name LIKE ?
            AND o.po_number = ?
            AND o.order_type = 'ACTIVE'
            AND emr.order_id IS NULL
            ORDER BY o.style_code, o.color_description
            """
            return pd.read_sql(query, conn, params=[f'{customer}%', po_number])
    
    def find_layer3_matches(self, customer: str, po_number: str) -> List[Dict]:
        """
        Find Layer 3 matches: unmatched ACTIVE orders that could resolve quantity gaps
        """
        failures = self.get_quantity_failures(customer, po_number)
        unmatched = self.get_unmatched_orders(customer, po_number)
        
        layer3_matches = []
        
        for _, failure in failures.iterrows():
            # Calculate quantity gap
            qty_gap = failure['shipment_quantity'] - failure['current_order_quantity']
            
            # Find unmatched orders with same style/color
            candidates = unmatched[
                (unmatched['style_code'] == failure['shipment_style_code']) &
                (unmatched['color_description'] == failure['shipment_color_description'])
            ].copy()
            
            if len(candidates) == 0:
                continue
                
            # Sort candidates by how well they fill the gap
            candidates['gap_fit_score'] = candidates['quantity'].apply(
                lambda x: 100 - abs((x - qty_gap) / qty_gap * 100) if qty_gap > 0 else 0
            )
            candidates = candidates.sort_values('gap_fit_score', ascending=False)
            
            # Evaluate delivery method compatibility
            for _, candidate in candidates.iterrows():
                delivery_match = "MATCH" if candidate['delivery_method'] == failure['shipment_delivery_method'] else "MISMATCH"
                
                # Calculate variance after linking
                new_total_qty = failure['current_order_quantity'] + candidate['quantity']
                variance_after = abs((failure['shipment_quantity'] - new_total_qty) / failure['shipment_quantity'] * 100)
                
                # Determine match quality based on gap filling and final variance
                if qty_gap <= 20:  # Small gaps - be careful about overshooting
                    if candidate['quantity'] <= qty_gap * 1.5 and variance_after <= 15:
                        match_quality = "EXCELLENT"
                    elif candidate['quantity'] <= qty_gap * 2.0 and variance_after <= 25:
                        match_quality = "GOOD"
                    else:
                        match_quality = "PARTIAL"
                elif candidate['quantity'] >= qty_gap * 0.9 and variance_after <= 10:  # Large gaps - prioritize closure
                    match_quality = "EXCELLENT"
                elif candidate['quantity'] >= qty_gap * 0.7 and variance_after <= 20:
                    match_quality = "GOOD"
                else:
                    match_quality = "PARTIAL"
                
                layer3_match = {
                    'shipment_id': failure['shipment_id'],
                    'current_order_id': failure['current_order_id'],
                    'additional_order_id': candidate['order_id'],
                    'style_code': failure['shipment_style_code'],
                    'color_description': failure['shipment_color_description'],
                    'shipment_quantity': failure['shipment_quantity'],
                    'current_order_quantity': failure['current_order_quantity'],
                    'additional_order_quantity': candidate['quantity'],
                    'total_order_quantity': failure['current_order_quantity'] + candidate['quantity'],
                    'quantity_gap': qty_gap,
                    'gap_after_linking': failure['shipment_quantity'] - (failure['current_order_quantity'] + candidate['quantity']),
                    'variance_before': failure['quantity_difference_percent'],
                    'variance_after': ((failure['shipment_quantity'] - (failure['current_order_quantity'] + candidate['quantity'])) / failure['shipment_quantity'] * 100),
                    'shipment_delivery': failure['shipment_delivery_method'],
                    'current_order_delivery': failure['order_delivery_method'],
                    'additional_order_delivery': candidate['delivery_method'],
                    'delivery_match': delivery_match,
                    'match_quality': match_quality,
                    'gap_fit_score': candidate['gap_fit_score']
                }
                
                layer3_matches.append(layer3_match)
        
        return layer3_matches
    
    def apply_layer3_matches(self, matches: List[Dict], customer: str, po_number: str, 
                           session_id: str = "LAYER3_AUTO") -> int:
        """
        Apply Layer 3 matches by creating additional entries in enhanced_matching_results
        """
        applied_count = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for match in matches:
                # Only apply EXCELLENT and GOOD matches automatically
                if match['match_quality'] not in ['EXCELLENT', 'GOOD']:
                    continue
                
                # Insert additional match record
                insert_query = """
                INSERT INTO enhanced_matching_results (
                    customer_name, po_number, shipment_id, order_id, match_layer,
                    match_confidence, style_match, color_match, delivery_match,
                    shipment_style_code, order_style_code,
                    shipment_color_description, order_color_description,
                    shipment_delivery_method, order_delivery_method,
                    shipment_quantity, order_quantity, quantity_difference_percent,
                    quantity_check_result, matching_session_id, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Calculate new quantity check result
                new_variance = abs(match['variance_after'])
                quantity_check = "PASS" if new_variance <= 10 else "FAIL"
                
                cursor.execute(insert_query, [
                    customer,
                    po_number,
                    match['shipment_id'],
                    match['additional_order_id'],
                    'LAYER_3',
                    match['gap_fit_score'] / 100.0,  # Convert to 0-1 confidence
                    'MATCH',  # Style always matches in Layer 3
                    'MATCH',  # Color always matches in Layer 3
                    match['delivery_match'],
                    match['style_code'],
                    match['style_code'],
                    match['color_description'],
                    match['color_description'],
                    match['shipment_delivery'],
                    match['additional_order_delivery'],
                    match['shipment_quantity'],
                    match['additional_order_quantity'],
                    match['variance_after'],
                    quantity_check,
                    session_id,
                    'LAYER3_MATCHER'
                ])
                
                applied_count += 1
            
            conn.commit()
        
        return applied_count
    
    def run_layer3_matching(self, customer: str, po_number: str, 
                           auto_apply: bool = False) -> Dict:
        """
        Run complete Layer 3 matching process
        """
        print(f"\n🔧 LAYER 3 MATCHING: {customer} PO {po_number}")
        print("=" * 60)
        
        # Find potential matches
        matches = self.find_layer3_matches(customer, po_number)
        
        if not matches:
            print("❌ No Layer 3 matches found")
            return {"matches_found": 0, "matches_applied": 0}
        
        # Group by shipment for reporting
        shipment_groups = {}
        for match in matches:
            sid = match['shipment_id']
            if sid not in shipment_groups:
                shipment_groups[sid] = []
            shipment_groups[sid].append(match)
        
        print(f"🎯 Found {len(matches)} potential Layer 3 matches for {len(shipment_groups)} shipments:")
        
        excellent_matches = 0
        good_matches = 0
        
        for shipment_id, group in shipment_groups.items():
            print(f"\n📦 Shipment {shipment_id}:")
            
            for match in group:
                quality_icon = {"EXCELLENT": "🟢", "GOOD": "🟡", "PARTIAL": "🟠"}[match['match_quality']]
                
                print(f"  {quality_icon} {match['match_quality']}: {match['style_code']} {match['color_description']}")
                print(f"    Gap: {match['quantity_gap']} units → Additional order: {match['additional_order_quantity']} units")
                print(f"    Variance: {match['variance_before']:.1f}% → {match['variance_after']:.1f}%")
                print(f"    Delivery: {match['delivery_match']} ({match['additional_order_delivery']})")
                
                if match['match_quality'] == 'EXCELLENT':
                    excellent_matches += 1
                elif match['match_quality'] == 'GOOD':
                    good_matches += 1
        
        applied_count = 0
        if auto_apply and (excellent_matches > 0 or good_matches > 0):
            print(f"\n🚀 Auto-applying {excellent_matches + good_matches} high-quality matches...")
            applied_count = self.apply_layer3_matches(matches, customer, po_number)
            print(f"✅ Applied {applied_count} Layer 3 matches")
        
        return {
            "matches_found": len(matches),
            "excellent_matches": excellent_matches,
            "good_matches": good_matches,
            "partial_matches": len(matches) - excellent_matches - good_matches,
            "matches_applied": applied_count
        }

def main():
    """CLI interface for Layer 3 matching"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Layer 3 Matching: Quantity Variance Resolution')
    parser.add_argument('--customer', required=True, help='Customer name')
    parser.add_argument('--po', required=True, help='PO number')
    parser.add_argument('--auto-apply', action='store_true', help='Automatically apply high-quality matches')
    
    args = parser.parse_args()
    
    matcher = Layer3Matcher()
    result = matcher.run_layer3_matching(args.customer, args.po, args.auto_apply)
    
    print(f"\n📊 LAYER 3 MATCHING SUMMARY:")
    print(f"   Potential matches found: {result['matches_found']}")
    print(f"   Excellent matches: {result['excellent_matches']}")
    print(f"   Good matches: {result['good_matches']}")
    print(f"   Partial matches: {result['partial_matches']}")
    print(f"   Matches applied: {result['matches_applied']}")

if __name__ == "__main__":
    main()
