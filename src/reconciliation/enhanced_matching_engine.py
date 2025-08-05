"""
Enhanced Matching Engine for TASK013
Implements improved 4-layer matching with movement table integration
"""

import pandas as pd
import pyodbc
import json
import logging
from datetime import datetime
from pathlib import Path
import sys
import uuid
from typing import Dict, List, Tuple, Optional, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from auth_helper import get_connection_string

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedMatchingEngine:
    """
    Enhanced matching engine with 4-layer approach and movement table integration
    
    Layers:
    - Layer 0: Perfect exact matches (style + color + delivery)
    - Layer 1: Exact style + color, flexible delivery
    - Layer 2: Fuzzy style + color matching
    - Layer 3: Quantity resolution and split shipment detection
    """
    
    def __init__(self):
        self.connection_string = get_connection_string()
        self.session_id = f"ENHANCED_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.batch_id = None
        
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.connection_string)
    
    def start_matching_session(self, customer_name: str, po_number: str = None, description: str = None):
        """Start a new matching session and create batch record"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            batch_name = f"ENHANCED_MATCHING_{customer_name}"
            if po_number:
                batch_name += f"_PO_{po_number}"
            
            cursor.execute("""
                INSERT INTO reconciliation_batch (name, description, start_time, status, created_by)
                OUTPUT INSERTED.id
                VALUES (?, ?, GETDATE(), 'RUNNING', ?)
            """, batch_name, description or f"Enhanced matching for {customer_name}", 'ENHANCED_MATCHING_ENGINE')
            
            self.batch_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"Started matching session {self.session_id} with batch_id {self.batch_id}")
            return self.batch_id
    
    def end_matching_session(self, status: str = 'COMPLETED', matched_count: int = 0, unmatched_count: int = 0):
        """End matching session and update batch record"""
        if not self.batch_id:
            return
            
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE reconciliation_batch 
                SET end_time = GETDATE(), status = ?, matched_count = ?, unmatched_count = ?, updated_at = GETDATE()
                WHERE id = ?
            """, status, matched_count, unmatched_count, self.batch_id)
            conn.commit()
            
            logger.info(f"Ended matching session {self.session_id}: {status} - {matched_count} matched, {unmatched_count} unmatched")
    
    def get_orders_for_matching(self, customer_name: str, po_number: str = None) -> pd.DataFrame:
        """Get orders for matching with enhanced canonicalization"""
        query = """
        SELECT 
            fol.id as order_id,
            fol.customer_name,
            fol.po_number,
            fol.style_code,
            fol.color_description,
            fol.size_code,
            fol.quantity as order_quantity,
            fol.delivery_method,
            fol.order_type,
            fol.order_date,
            fol.unit_price,
            -- Canonical fields with improved normalization
            UPPER(LTRIM(RTRIM(fol.style_code))) as canonical_style,
            UPPER(LTRIM(RTRIM(REPLACE(REPLACE(fol.color_description, '/', ' '), '-', ' ')))) as canonical_color,
            UPPER(LTRIM(RTRIM(fol.delivery_method))) as canonical_delivery,
            -- Style-color composite key
            CONCAT(UPPER(LTRIM(RTRIM(fol.style_code))), '|', 
                   UPPER(LTRIM(RTRIM(REPLACE(REPLACE(fol.color_description, '/', ' '), '-', ' '))))) as style_color_key
        FROM FACT_ORDER_LIST fol
        WHERE fol.customer_name LIKE ?
        """
        
        params = [f"{customer_name}%"]
        if po_number:
            query += " AND fol.po_number = ?"
            params.append(po_number)
        
        query += " ORDER BY fol.order_date DESC, fol.id"
        
        with self.get_connection() as conn:
            orders_df = pd.read_sql(query, conn, params=params)
            
        logger.info(f"Loaded {len(orders_df)} orders for matching")
        return orders_df
    
    def get_shipments_for_matching(self, customer_name: str, po_number: str = None) -> pd.DataFrame:
        """Get shipments for matching with enhanced canonicalization"""
        query = """
        SELECT 
            fmos.shipment_id,
            fmos.Customer as customer_name,
            fmos.Customer_PO as po_number,
            fmos.Style as style_code,
            fmos.Color as color_description,
            fmos.Size as size_code,
            fmos.Quantity as shipment_quantity,
            fmos.Shipping_Method as delivery_method,
            fmos.Shipped_Date,
            fmos.Tracking_Number,
            -- Canonical fields with improved normalization
            UPPER(LTRIM(RTRIM(fmos.Style))) as canonical_style,
            UPPER(LTRIM(RTRIM(REPLACE(REPLACE(fmos.Color, '/', ' '), '-', ' ')))) as canonical_color,
            UPPER(LTRIM(RTRIM(fmos.Shipping_Method))) as canonical_delivery,
            -- Style-color composite key
            CONCAT(UPPER(LTRIM(RTRIM(fmos.Style))), '|', 
                   UPPER(LTRIM(RTRIM(REPLACE(REPLACE(fmos.Color, '/', ' '), '-', ' '))))) as style_color_key
        FROM FM_orders_shipped fmos
        WHERE fmos.Customer LIKE ?
        """
        
        params = [f"{customer_name}%"]
        if po_number:
            query += " AND fmos.Customer_PO = ?"
            params.append(po_number)
        
        query += " ORDER BY fmos.Shipped_Date DESC, fmos.shipment_id"
        
        with self.get_connection() as conn:
            shipments_df = pd.read_sql(query, conn, params=params)
            
        logger.info(f"Loaded {len(shipments_df)} shipments for matching")
        return shipments_df
    
    def layer0_perfect_matching(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> Tuple[List[Dict], pd.DataFrame]:
        """
        Layer 0: Perfect exact matching on style + color + delivery
        Highest confidence, auto-approved matches
        """
        matches = []
        unmatched_indices = []
        
        logger.info("Starting Layer 0: Perfect exact matching")
        
        # Create lookup dictionary for orders
        order_lookup = {}
        for idx, order in orders_df.iterrows():
            # Create composite key: style + color + delivery
            key = f"{order['canonical_style']}|{order['canonical_color']}|{order['canonical_delivery']}"
            if key not in order_lookup:
                order_lookup[key] = []
            order_lookup[key].append(order.to_dict())
        
        # Match shipments to orders
        for idx, shipment in shipments_df.iterrows():
            key = f"{shipment['canonical_style']}|{shipment['canonical_color']}|{shipment['canonical_delivery']}"
            
            if key in order_lookup:
                # Find best quantity match within this perfect match group
                best_match = None
                best_qty_score = -1
                
                for order in order_lookup[key]:
                    qty_score = self._calculate_quantity_score(order['order_quantity'], shipment['shipment_quantity'])
                    if qty_score > best_qty_score:
                        best_qty_score = qty_score
                        best_match = order
                
                if best_match:
                    match = {
                        'shipment_id': shipment['shipment_id'],
                        'order_id': best_match['order_id'],
                        'match_layer': 'LAYER_0',
                        'match_type': 'PERFECT_EXACT',
                        'confidence': 1.0,
                        'style_match': 'EXACT',
                        'color_match': 'EXACT',
                        'delivery_match': 'EXACT',
                        'quantity_score': best_qty_score,
                        'order_quantity': best_match['order_quantity'],
                        'shipment_quantity': shipment['shipment_quantity'],
                        'quantity_variance': shipment['shipment_quantity'] - best_match['order_quantity'],
                        'quantity_variance_percent': ((shipment['shipment_quantity'] - best_match['order_quantity']) / best_match['order_quantity'] * 100) if best_match['order_quantity'] > 0 else 0,
                        'match_reason': 'Perfect match: style + color + delivery + quantity tolerance',
                        'customer_name': shipment['customer_name'],
                        'po_number': shipment['po_number'],
                        'style_code': shipment['style_code'],
                        'color_description': shipment['color_description']
                    }
                    matches.append(match)
                    continue
                    
            unmatched_indices.append(idx)
        
        unmatched_shipments = shipments_df.iloc[unmatched_indices].copy() if unmatched_indices else pd.DataFrame()
        
        logger.info(f"Layer 0 completed: {len(matches)} perfect matches, {len(unmatched_shipments)} remaining")
        return matches, unmatched_shipments
    
    def layer1_style_color_exact(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> Tuple[List[Dict], pd.DataFrame]:
        """
        Layer 1: Exact style + color, flexible delivery method
        High confidence matches requiring style and color to be identical
        """
        matches = []
        unmatched_indices = []
        
        logger.info("Starting Layer 1: Exact style + color, flexible delivery")
        
        # Create lookup dictionary for orders (style + color only)
        order_lookup = {}
        for idx, order in orders_df.iterrows():
            key = f"{order['canonical_style']}|{order['canonical_color']}"
            if key not in order_lookup:
                order_lookup[key] = []
            order_lookup[key].append(order.to_dict())
        
        # Match shipments to orders
        for idx, shipment in shipments_df.iterrows():
            key = f"{shipment['canonical_style']}|{shipment['canonical_color']}"
            
            if key in order_lookup:
                # Find best match considering delivery similarity and quantity
                best_match = None
                best_score = -1
                
                for order in order_lookup[key]:
                    # Calculate delivery similarity
                    delivery_similarity = self._calculate_delivery_similarity(
                        order['canonical_delivery'], 
                        shipment['canonical_delivery']
                    )
                    
                    # Calculate quantity score
                    qty_score = self._calculate_quantity_score(order['order_quantity'], shipment['shipment_quantity'])
                    
                    # Combined score (70% quantity, 30% delivery)
                    combined_score = (qty_score * 0.7) + (delivery_similarity * 0.3)
                    
                    if combined_score > best_score:
                        best_score = combined_score
                        best_match = order
                        best_match['delivery_similarity'] = delivery_similarity
                
                if best_match and best_score >= 0.6:  # Minimum threshold for Layer 1
                    match = {
                        'shipment_id': shipment['shipment_id'],
                        'order_id': best_match['order_id'],
                        'match_layer': 'LAYER_1',
                        'match_type': 'STYLE_COLOR_EXACT',
                        'confidence': min(0.95, 0.85 + (best_score * 0.1)),  # 0.85-0.95 range
                        'style_match': 'EXACT',
                        'color_match': 'EXACT',
                        'delivery_match': 'EXACT' if best_match['delivery_similarity'] >= 0.9 else 'SIMILAR',
                        'delivery_similarity': best_match['delivery_similarity'],
                        'quantity_score': self._calculate_quantity_score(best_match['order_quantity'], shipment['shipment_quantity']),
                        'order_quantity': best_match['order_quantity'],
                        'shipment_quantity': shipment['shipment_quantity'],
                        'quantity_variance': shipment['shipment_quantity'] - best_match['order_quantity'],
                        'quantity_variance_percent': ((shipment['shipment_quantity'] - best_match['order_quantity']) / best_match['order_quantity'] * 100) if best_match['order_quantity'] > 0 else 0,
                        'match_reason': f'Exact style+color match, delivery similarity: {best_match["delivery_similarity"]:.2f}',
                        'customer_name': shipment['customer_name'],
                        'po_number': shipment['po_number'],
                        'style_code': shipment['style_code'],
                        'color_description': shipment['color_description']
                    }
                    matches.append(match)
                    continue
                    
            unmatched_indices.append(idx)
        
        unmatched_shipments = shipments_df.iloc[unmatched_indices].copy() if unmatched_indices else pd.DataFrame()
        
        logger.info(f"Layer 1 completed: {len(matches)} style+color matches, {len(unmatched_shipments)} remaining")
        return matches, unmatched_shipments
    
    def layer2_fuzzy_matching(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> Tuple[List[Dict], pd.DataFrame]:
        """
        Layer 2: Fuzzy style + color matching for data entry variations
        Medium confidence matches using fuzzy string matching
        """
        try:
            from rapidfuzz import fuzz
        except ImportError:
            logger.warning("rapidfuzz not available, skipping Layer 2 fuzzy matching")
            return [], shipments_df
        
        matches = []
        unmatched_indices = []
        
        logger.info("Starting Layer 2: Fuzzy style + color matching")
        
        fuzzy_threshold = 0.8  # 80% similarity required
        
        for idx, shipment in shipments_df.iterrows():
            best_match = None
            best_score = -1
            best_style_sim = 0
            best_color_sim = 0
            
            for _, order in orders_df.iterrows():
                # Calculate fuzzy similarities
                style_similarity = fuzz.token_set_ratio(
                    shipment['canonical_style'], 
                    order['canonical_style']
                ) / 100.0
                
                color_similarity = fuzz.token_set_ratio(
                    shipment['canonical_color'], 
                    order['canonical_color']
                ) / 100.0
                
                # Both style and color must meet threshold
                if style_similarity >= fuzzy_threshold and color_similarity >= fuzzy_threshold:
                    # Calculate delivery similarity
                    delivery_similarity = self._calculate_delivery_similarity(
                        order['canonical_delivery'], 
                        shipment['canonical_delivery']
                    )
                    
                    # Calculate quantity score
                    qty_score = self._calculate_quantity_score(order['order_quantity'], shipment['shipment_quantity'])
                    
                    # Combined score (40% style, 30% color, 20% quantity, 10% delivery)
                    combined_score = (style_similarity * 0.4) + (color_similarity * 0.3) + (qty_score * 0.2) + (delivery_similarity * 0.1)
                    
                    if combined_score > best_score:
                        best_score = combined_score
                        best_match = order.to_dict()
                        best_style_sim = style_similarity
                        best_color_sim = color_similarity
                        best_match['delivery_similarity'] = delivery_similarity
            
            if best_match and best_score >= 0.7:  # Minimum threshold for Layer 2
                match = {
                    'shipment_id': shipment['shipment_id'],
                    'order_id': best_match['order_id'],
                    'match_layer': 'LAYER_2',
                    'match_type': 'FUZZY_STYLE_COLOR',
                    'confidence': min(0.85, 0.6 + (best_score * 0.25)),  # 0.6-0.85 range
                    'style_match': 'EXACT' if best_style_sim >= 0.99 else 'FUZZY',
                    'color_match': 'EXACT' if best_color_sim >= 0.99 else 'FUZZY',
                    'delivery_match': 'EXACT' if best_match['delivery_similarity'] >= 0.9 else 'SIMILAR',
                    'style_similarity': best_style_sim,
                    'color_similarity': best_color_sim,
                    'delivery_similarity': best_match['delivery_similarity'],
                    'quantity_score': self._calculate_quantity_score(best_match['order_quantity'], shipment['shipment_quantity']),
                    'order_quantity': best_match['order_quantity'],
                    'shipment_quantity': shipment['shipment_quantity'],
                    'quantity_variance': shipment['shipment_quantity'] - best_match['order_quantity'],
                    'quantity_variance_percent': ((shipment['shipment_quantity'] - best_match['order_quantity']) / best_match['order_quantity'] * 100) if best_match['order_quantity'] > 0 else 0,
                    'match_reason': f'Fuzzy match - Style: {best_style_sim:.2f}, Color: {best_color_sim:.2f}',
                    'customer_name': shipment['customer_name'],
                    'po_number': shipment['po_number'],
                    'style_code': shipment['style_code'],
                    'color_description': shipment['color_description']
                }
                matches.append(match)
                continue
                
            unmatched_indices.append(idx)
        
        unmatched_shipments = shipments_df.iloc[unmatched_indices].copy() if unmatched_indices else pd.DataFrame()
        
        logger.info(f"Layer 2 completed: {len(matches)} fuzzy matches, {len(unmatched_shipments)} remaining")
        return matches, unmatched_shipments
    
    def layer3_quantity_resolution(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame, existing_matches: List[Dict]) -> Tuple[List[Dict], pd.DataFrame]:
        """
        Layer 3: Quantity resolution and split shipment detection
        Attempts to resolve quantity discrepancies through split shipment detection
        """
        matches = []
        unmatched_indices = []
        
        logger.info("Starting Layer 3: Quantity resolution and split shipment detection")
        
        # Group existing matches by order to understand what's already matched
        order_matches = {}
        for match in existing_matches:
            order_id = match['order_id']
            if order_id not in order_matches:
                order_matches[order_id] = []
            order_matches[order_id].append(match)
        
        # Analyze remaining shipments for potential split shipment opportunities
        for idx, shipment in shipments_df.iterrows():
            best_split_match = self._find_split_shipment_opportunity(
                shipment, orders_df, order_matches
            )
            
            if best_split_match:
                match = {
                    'shipment_id': shipment['shipment_id'],
                    'order_id': best_split_match['order_id'],
                    'match_layer': 'LAYER_3',
                    'match_type': 'QUANTITY_RESOLUTION',
                    'confidence': best_split_match['confidence'],
                    'style_match': best_split_match['style_match'],
                    'color_match': best_split_match['color_match'],
                    'delivery_match': best_split_match['delivery_match'],
                    'quantity_score': best_split_match['quantity_score'],
                    'order_quantity': best_split_match['order_quantity'],
                    'shipment_quantity': shipment['shipment_quantity'],
                    'quantity_variance': shipment['shipment_quantity'] - best_split_match['remaining_quantity'],
                    'quantity_variance_percent': ((shipment['shipment_quantity'] - best_split_match['remaining_quantity']) / best_split_match['remaining_quantity'] * 100) if best_split_match['remaining_quantity'] > 0 else 0,
                    'match_reason': best_split_match['reason'],
                    'split_shipment_flag': True,
                    'remaining_order_quantity': best_split_match['remaining_quantity'],
                    'customer_name': shipment['customer_name'],
                    'po_number': shipment['po_number'],
                    'style_code': shipment['style_code'],
                    'color_description': shipment['color_description']
                }
                matches.append(match)
                
                # Update order matches to reflect this new match
                if best_split_match['order_id'] not in order_matches:
                    order_matches[best_split_match['order_id']] = []
                order_matches[best_split_match['order_id']].append(match)
                continue
                
            unmatched_indices.append(idx)
        
        unmatched_shipments = shipments_df.iloc[unmatched_indices].copy() if unmatched_indices else pd.DataFrame()
        
        logger.info(f"Layer 3 completed: {len(matches)} quantity resolution matches, {len(unmatched_shipments)} remaining")
        return matches, unmatched_shipments
    
    def _calculate_quantity_score(self, order_qty: int, shipment_qty: int) -> float:
        """Calculate quantity matching score (0.0 to 1.0)"""
        if order_qty <= 0:
            return 0.0
        
        diff_percent = abs(order_qty - shipment_qty) / order_qty * 100
        
        if diff_percent <= 5:
            return 1.0
        elif diff_percent <= 10:
            return 0.8
        elif diff_percent <= 25:
            return 0.6
        elif diff_percent <= 50:
            return 0.4
        else:
            return 0.2
    
    def _calculate_delivery_similarity(self, delivery1: str, delivery2: str) -> float:
        """Calculate delivery method similarity"""
        if not delivery1 or not delivery2:
            return 0.5
        
        if delivery1 == delivery2:
            return 1.0
        
        # Common delivery method mappings
        delivery_mappings = {
            'AIR': ['EXPRESS', 'EXPEDITED', 'OVERNIGHT'],
            'GROUND': ['STANDARD', 'REGULAR', 'NORMAL'],
            'SEA': ['OCEAN', 'BOAT', 'SHIP'],
            'TRUCK': ['GROUND', 'LTL', 'FREIGHT']
        }
        
        for key, aliases in delivery_mappings.items():
            if (delivery1 == key and delivery2 in aliases) or (delivery2 == key and delivery1 in aliases):
                return 0.9
            if delivery1 in aliases and delivery2 in aliases:
                return 0.8
        
        # Fuzzy similarity as fallback
        try:
            from rapidfuzz import fuzz
            return fuzz.token_set_ratio(delivery1, delivery2) / 100.0
        except ImportError:
            return 0.3  # Low similarity if no fuzzy matching available
    
    def _find_split_shipment_opportunity(self, shipment: pd.Series, orders_df: pd.DataFrame, order_matches: Dict) -> Optional[Dict]:
        """Find potential split shipment opportunities for unmatched shipments"""
        
        for _, order in orders_df.iterrows():
            order_id = order['order_id']
            
            # Calculate total quantity already matched to this order
            matched_qty = sum(match['shipment_quantity'] for match in order_matches.get(order_id, []))
            remaining_qty = order['order_quantity'] - matched_qty
            
            if remaining_qty <= 0:
                continue  # Order fully satisfied
            
            # Check if style and color match (exact or fuzzy)
            style_match = self._check_style_match(shipment['canonical_style'], order['canonical_style'])
            color_match = self._check_color_match(shipment['canonical_color'], order['canonical_color'])
            
            if not style_match or not color_match:
                continue
            
            # Check if shipment quantity makes sense for remaining quantity
            qty_score = self._calculate_quantity_score(remaining_qty, shipment['shipment_quantity'])
            
            if qty_score >= 0.4:  # At least 40% quantity match
                delivery_similarity = self._calculate_delivery_similarity(
                    order['canonical_delivery'], 
                    shipment['canonical_delivery']
                )
                
                confidence = min(0.75, (qty_score * 0.6) + (delivery_similarity * 0.2) + 0.2)
                
                return {
                    'order_id': order_id,
                    'confidence': confidence,
                    'style_match': 'EXACT' if style_match == 1.0 else 'FUZZY',
                    'color_match': 'EXACT' if color_match == 1.0 else 'FUZZY',
                    'delivery_match': 'EXACT' if delivery_similarity >= 0.9 else 'SIMILAR',
                    'quantity_score': qty_score,
                    'order_quantity': order['order_quantity'],
                    'remaining_quantity': remaining_qty,
                    'reason': f'Split shipment resolution - remaining qty: {remaining_qty}, shipment qty: {shipment["shipment_quantity"]}'
                }
        
        return None
    
    def _check_style_match(self, style1: str, style2: str) -> bool:
        """Check if styles match (exact or fuzzy)"""
        if style1 == style2:
            return True
        
        try:
            from rapidfuzz import fuzz
            return fuzz.token_set_ratio(style1, style2) >= 80
        except ImportError:
            return False
    
    def _check_color_match(self, color1: str, color2: str) -> bool:
        """Check if colors match (exact or fuzzy)"""
        if color1 == color2:
            return True
        
        try:
            from rapidfuzz import fuzz
            return fuzz.token_set_ratio(color1, color2) >= 80
        except ImportError:
            return False
    
    def store_matches(self, matches: List[Dict]) -> None:
        """Store matches in enhanced_matching_results table and movement table"""
        if not matches:
            return
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Clear previous results for this session
            cursor.execute("""
                DELETE FROM enhanced_matching_results 
                WHERE matching_session_id = ?
            """, self.session_id)
            
            # Insert new matches
            for match in matches:
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
                        matching_session_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                """, 
                    match['customer_name'], match['po_number'],
                    match['shipment_id'], match['order_id'],
                    match['match_layer'], match['confidence'],
                    match['style_match'], match['color_match'], match['delivery_match'],
                    match['style_code'], match['style_code'],  # Using shipment style for both
                    match['color_description'], match['color_description'],  # Using shipment color for both
                    match.get('delivery_method', ''), match.get('delivery_method', ''),
                    match['shipment_quantity'], match['order_quantity'],
                    match['quantity_variance_percent'], 
                    'PASS' if abs(match['quantity_variance_percent']) <= 10 else 'FAIL',
                    self.session_id
                )
                
                # Create movement table entries for reconciliation
                match_group_id = f"{match['customer_name']}_{match['po_number']}_{match['order_id']}_{match['shipment_id']}"
                
                cursor.execute("""
                    EXEC sp_capture_reconciliation_event 
                        @order_id = ?, @shipment_id = ?, @match_group_id = ?,
                        @reconciliation_status = ?, @reconciliation_confidence = ?,
                        @reconciliation_method = ?, @quantity_variance = ?, @batch_id = ?
                """, 
                    str(match['order_id']), match['shipment_id'], match_group_id,
                    'MATCHED', match['confidence'], match['match_layer'],
                    match['quantity_variance'], self.batch_id
                )
            
            conn.commit()
            logger.info(f"Stored {len(matches)} matches in database")
    
    def run_enhanced_matching(self, customer_name: str, po_number: str = None) -> Dict[str, Any]:
        """Run the complete enhanced 4-layer matching process"""
        logger.info(f"Starting enhanced matching for {customer_name}" + (f" PO {po_number}" if po_number else ""))
        
        # Start matching session
        self.start_matching_session(customer_name, po_number)
        
        try:
            # Load data
            orders_df = self.get_orders_for_matching(customer_name, po_number)
            shipments_df = self.get_shipments_for_matching(customer_name, po_number)
            
            if orders_df.empty or shipments_df.empty:
                logger.warning("No orders or shipments found for matching")
                self.end_matching_session('COMPLETED', 0, len(shipments_df))
                return {
                    'status': 'NO_DATA',
                    'total_orders': len(orders_df),
                    'total_shipments': len(shipments_df),
                    'matches': []
                }
            
            all_matches = []
            remaining_shipments = shipments_df.copy()
            
            # Layer 0: Perfect exact matches
            layer0_matches, remaining_shipments = self.layer0_perfect_matching(orders_df, remaining_shipments)
            all_matches.extend(layer0_matches)
            
            # Layer 1: Exact style + color, flexible delivery
            if not remaining_shipments.empty:
                layer1_matches, remaining_shipments = self.layer1_style_color_exact(orders_df, remaining_shipments)
                all_matches.extend(layer1_matches)
            
            # Layer 2: Fuzzy style + color matching
            if not remaining_shipments.empty:
                layer2_matches, remaining_shipments = self.layer2_fuzzy_matching(orders_df, remaining_shipments)
                all_matches.extend(layer2_matches)
            
            # Layer 3: Quantity resolution and split shipment detection
            if not remaining_shipments.empty:
                layer3_matches, remaining_shipments = self.layer3_quantity_resolution(orders_df, remaining_shipments, all_matches)
                all_matches.extend(layer3_matches)
            
            # Store all matches
            self.store_matches(all_matches)
            
            # Update session
            self.end_matching_session('COMPLETED', len(all_matches), len(remaining_shipments))
            
            # Generate summary
            layer_summary = {
                'LAYER_0': len([m for m in all_matches if m['match_layer'] == 'LAYER_0']),
                'LAYER_1': len([m for m in all_matches if m['match_layer'] == 'LAYER_1']),
                'LAYER_2': len([m for m in all_matches if m['match_layer'] == 'LAYER_2']),
                'LAYER_3': len([m for m in all_matches if m['match_layer'] == 'LAYER_3'])
            }
            
            match_rate = (len(all_matches) / len(shipments_df) * 100) if len(shipments_df) > 0 else 0
            
            results = {
                'status': 'SUCCESS',
                'customer_name': customer_name,
                'po_number': po_number,
                'session_id': self.session_id,
                'batch_id': self.batch_id,
                'total_orders': len(orders_df),
                'total_shipments': len(shipments_df),
                'total_matches': len(all_matches),
                'unmatched_shipments': len(remaining_shipments),
                'match_rate': match_rate,
                'layer_summary': layer_summary,
                'matches': all_matches,
                'unmatched_shipment_ids': remaining_shipments['shipment_id'].tolist() if not remaining_shipments.empty else []
            }
            
            logger.info(f"Enhanced matching completed: {len(all_matches)}/{len(shipments_df)} matches ({match_rate:.1f}%)")
            logger.info(f"Layer distribution: {layer_summary}")
            
            return results
            
        except Exception as e:
            logger.error(f"Enhanced matching failed: {str(e)}")
            self.end_matching_session('ERROR', 0, 0)
            raise

def main():
    """Main execution for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced matching engine with 4-layer approach")
    parser.add_argument("--customer", required=True, help="Customer name")
    parser.add_argument("--po", help="PO number (optional)")
    
    args = parser.parse_args()
    
    try:
        engine = EnhancedMatchingEngine()
        results = engine.run_enhanced_matching(args.customer, args.po)
        
        print(f"\n🎉 Enhanced matching completed!")
        print(f"📊 Results: {results['total_matches']}/{results['total_shipments']} matches ({results['match_rate']:.1f}%)")
        print(f"🎯 Layer Distribution:")
        for layer, count in results['layer_summary'].items():
            print(f"   {layer}: {count}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Enhanced matching failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())