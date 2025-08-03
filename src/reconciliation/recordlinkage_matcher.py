"""
Recordlinkage implementation for order-shipment matching.
"""
import recordlinkage
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path

from src.utils.db import (
    get_customer_match_config,
    save_reconciliation_result,
    save_attribute_scores,
    add_to_hitl_queue
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class RecordLinkageMatcher:
    """
    A class for matching orders to shipments using recordlinkage.
    """
    
    def __init__(self, customer_name: str, config: Optional[Dict] = None):
        """
        Initialize the matcher with customer-specific settings.
        
        Args:
            customer_name: Customer name for matching
            config: Optional configuration override
        """
        self.customer_name = customer_name
        
        # Load config from database if not provided
        self.config = config or get_customer_match_config(customer_name)
        
        # Extract thresholds from config
        self.thresholds = {
            'exact': float(self.config.get('threshold', {}).get('exact_match', '1.0')),
            'fuzzy': float(self.config.get('threshold', {}).get('fuzzy_match', '0.85')),
            'uncertain': float(self.config.get('threshold', {}).get('uncertain_match', '0.7'))
        }
        
        # Extract attribute weights from config
        self.weights = {}
        for key, value in self.config.get('attribute_weight', {}).items():
            self.weights[key] = float(value)
        
        # Initialize recordlinkage
        self.indexer = recordlinkage.Index()
        self.compare = recordlinkage.Compare()
    
    def prepare_data(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepare and clean data for matching.
        
        Args:
            orders_df: DataFrame with orders data
            shipments_df: DataFrame with shipments data
            
        Returns:
            Cleaned and prepared DataFrames
        """
        # Make copies to avoid modifying originals
        orders = orders_df.copy()
        shipments = shipments_df.copy()
        
        # Ensure required columns exist
        required_columns = ['style', 'color', 'size']
        for col in required_columns:
            if col not in orders.columns:
                orders[col] = np.nan
            if col not in shipments.columns:
                shipments[col] = np.nan
        
        # Clean and standardize data
        for col in required_columns:
            # Convert to string
            orders[col] = orders[col].astype(str).str.upper().str.strip()
            shipments[col] = shipments[col].astype(str).str.upper().str.strip()
            
            # Replace NaN values with empty string
            orders[col] = orders[col].replace('NAN', '').replace('NONE', '')
            shipments[col] = shipments[col].replace('NAN', '').replace('NONE', '')
        
        return orders, shipments
    
    def build_comparison_space(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> recordlinkage.Index:
        """
        Build the comparison space (candidate pairs) for matching.
        
        Args:
            orders_df: DataFrame with orders data
            shipments_df: DataFrame with shipments data
            
        Returns:
            Indexer with candidate pairs
        """
        logger.info(f"Building comparison space for {len(orders_df)} orders and {len(shipments_df)} shipments")
        
        # If PO numbers are available in both datasets, use them for blocking
        if 'po_number' in orders_df.columns and 'po_number' in shipments_df.columns:
            self.indexer.block('po_number')
            logger.info("Using PO number for blocking")
        
        # Add style blocking if available
        if 'style' in orders_df.columns and 'style' in shipments_df.columns:
            self.indexer.block('style')
            logger.info("Using style for blocking")
        
        # If few records or no blocking keys available, use full comparison
        if len(orders_df) * len(shipments_df) < 10000 or len(self.indexer.algorithms) == 0:
            self.indexer.full()
            logger.info("Using full comparison space")
        
        # Build pairs
        candidate_pairs = self.indexer.index(orders_df, shipments_df)
        logger.info(f"Generated {len(candidate_pairs)} candidate pairs")
        
        return candidate_pairs
    
    def configure_comparisons(self) -> None:
        """
        Configure the comparison methods for different attributes.
        """
        # Exact comparison for style codes
        if 'style' in self.weights:
            self.compare.exact('style', 'style', label='style_exact')
            self.compare.string('style', 'style', method='jarowinkler', threshold=0.85, label='style_fuzzy')
        
        # Exact and fuzzy comparison for color
        if 'color' in self.weights:
            self.compare.exact('color', 'color', label='color_exact')
            self.compare.string('color', 'color', method='jarowinkler', threshold=0.85, label='color_fuzzy')
        
        # Exact comparison for size
        if 'size' in self.weights:
            self.compare.exact('size', 'size', label='size_exact')
        
        # Optional: Quantity comparison if needed
        if 'quantity' in self.weights and 'quantity' in self.config.get('key_attributes', []):
            self.compare.exact('quantity', 'quantity', label='quantity_exact')
    
    def compute_similarity(self, candidate_pairs: recordlinkage.Index, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute similarity scores for candidate pairs.
        
        Args:
            candidate_pairs: Candidate pairs for comparison
            orders_df: DataFrame with orders data
            shipments_df: DataFrame with shipments data
            
        Returns:
            DataFrame with similarity scores
        """
        # Configure comparisons
        self.configure_comparisons()
        
        # Compute similarity scores
        logger.info("Computing similarity scores")
        features = self.compare.compute(candidate_pairs, orders_df, shipments_df)
        
        return features
    
    def apply_weights(self, features: pd.DataFrame) -> pd.DataFrame:
        """
        Apply weights to similarity scores based on configuration.
        
        Args:
            features: DataFrame with similarity scores
            
        Returns:
            DataFrame with weighted scores
        """
        # Create a copy to avoid modifying the original
        weighted = features.copy()
        
        # Apply weights
        for column in features.columns:
            attribute = column.split('_')[0]  # Extract attribute name from column name
            if attribute in self.weights:
                weighted[column] = features[column] * self.weights[attribute]
        
        # Calculate weighted sum for combined attributes
        for attribute in self.weights:
            # Find all columns for this attribute
            cols = [col for col in weighted.columns if col.startswith(f"{attribute}_")]
            if cols:
                # Take maximum score among different comparison methods for same attribute
                weighted[f"{attribute}_weighted"] = weighted[cols].max(axis=1)
        
        # Calculate overall score
        weighted_cols = [col for col in weighted.columns if col.endswith('_weighted')]
        if weighted_cols:
            weighted['overall_score'] = weighted[weighted_cols].sum(axis=1) / sum(self.weights.values())
        else:
            # Fallback if no weighted columns
            weighted['overall_score'] = weighted.mean(axis=1)
        
        return weighted
    
    def classify_matches(self, weighted_scores: pd.DataFrame) -> pd.DataFrame:
        """
        Classify matches based on weighted scores and thresholds.
        
        Args:
            weighted_scores: DataFrame with weighted similarity scores
            
        Returns:
            DataFrame with classification results
        """
        # Create a copy to avoid modifying the original
        results = weighted_scores.copy()
        
        # Classify matches
        results['match_status'] = 'unmatched'
        results.loc[results['overall_score'] >= self.thresholds['exact'], 'match_status'] = 'matched'
        results.loc[(results['overall_score'] >= self.thresholds['uncertain']) & 
                    (results['overall_score'] < self.thresholds['fuzzy']), 'match_status'] = 'uncertain'
        results.loc[(results['overall_score'] >= self.thresholds['fuzzy']) & 
                    (results['overall_score'] < self.thresholds['exact']), 'match_status'] = 'matched'
        
        # Determine match method
        results['match_method'] = 'recordlinkage'
        results.loc[results['overall_score'] >= self.thresholds['exact'], 'match_method'] = 'exact'
        results.loc[(results['overall_score'] >= self.thresholds['fuzzy']) & 
                    (results['overall_score'] < self.thresholds['exact']), 'match_method'] = 'fuzzy'
        
        return results
    
    def match(self, orders_df: pd.DataFrame, shipments_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Match orders to shipments.
        
        Args:
            orders_df: DataFrame with orders data
            shipments_df: DataFrame with shipments data
            
        Returns:
            Tuple of (matches, uncertain_matches, unmatched)
        """
        # Prepare data
        orders, shipments = self.prepare_data(orders_df, shipments_df)
        
        # Build comparison space
        candidate_pairs = self.build_comparison_space(orders, shipments)
        
        # Compute similarity
        features = self.compute_similarity(candidate_pairs, orders, shipments)
        
        # Apply weights
        weighted = self.apply_weights(features)
        
        # Classify matches
        results = self.classify_matches(weighted)
        
        # Split into matches, uncertain matches, and unmatched
        matches = results[results['match_status'] == 'matched']
        uncertain = results[results['match_status'] == 'uncertain']
        
        # Find unmatched shipments
        matched_shipment_ids = set(matches.index.get_level_values(1)) | set(uncertain.index.get_level_values(1))
        unmatched_shipments = shipments[~shipments.index.isin(matched_shipment_ids)]
        
        return matches, uncertain, unmatched_shipments
    
    def save_results(self, matches: pd.DataFrame, uncertain: pd.DataFrame, unmatched: pd.DataFrame, 
                    orders_df: pd.DataFrame, shipments_df: pd.DataFrame, po_number: str) -> Dict[str, int]:
        """
        Save matching results to the database.
        
        Args:
            matches: DataFrame with matches
            uncertain: DataFrame with uncertain matches
            unmatched: DataFrame with unmatched shipments
            orders_df: Original orders DataFrame
            shipments_df: Original shipments DataFrame
            po_number: PO number for this reconciliation
            
        Returns:
            Dictionary with counts of saved records
        """
        logger.info(f"Saving reconciliation results for {self.customer_name}, PO: {po_number}")
        
        saved_counts = {
            'matched': 0,
            'uncertain': 0,
            'unmatched': 0,
            'hitl_queue': 0
        }
        
        # Save matches
        for (order_idx, shipment_idx), row in matches.iterrows():
            order_data = orders_df.loc[order_idx].to_dict()
            shipment_data = shipments_df.loc[shipment_idx].to_dict()
            
            # Create match details
            match_details = {
                'overall_score': float(row['overall_score']),
                'order_data': {k: v for k, v in order_data.items() if k != 'id'},
                'shipment_data': {k: v for k, v in shipment_data.items() if k != 'id'}
            }
            
            # Save reconciliation result
            reconciliation_id = save_reconciliation_result(
                customer_name=self.customer_name,
                order_id=order_data.get('id'),
                shipment_id=shipment_data.get('id'),
                po_number=po_number,
                match_status='matched',
                confidence_score=float(row['overall_score']),
                match_method=row['match_method'],
                match_details=match_details
            )
            
            # Save attribute scores
            attribute_scores = []
            for column in row.index:
                if column not in ['match_status', 'match_method', 'overall_score'] and not column.endswith('_weighted'):
                    attribute_name = column.split('_')[0]
                    attribute_scores.append({
                        'attribute_name': attribute_name,
                        'order_value': str(order_data.get(attribute_name, '')),
                        'shipment_value': str(shipment_data.get(attribute_name, '')),
                        'match_score': float(row[column]),
                        'match_method': column.split('_')[1] if '_' in column else 'exact',
                        'is_key_attribute': attribute_name in self.config.get('key_attributes', []),
                        'weight': self.weights.get(attribute_name, 1.0)
                    })
            
            save_attribute_scores(reconciliation_id, attribute_scores)
            saved_counts['matched'] += 1
        
        # Save uncertain matches and add to HITL queue
        for (order_idx, shipment_idx), row in uncertain.iterrows():
            order_data = orders_df.loc[order_idx].to_dict()
            shipment_data = shipments_df.loc[shipment_idx].to_dict()
            
            # Create match details
            match_details = {
                'overall_score': float(row['overall_score']),
                'order_data': {k: v for k, v in order_data.items() if k != 'id'},
                'shipment_data': {k: v for k, v in shipment_data.items() if k != 'id'}
            }
            
            # Save reconciliation result
            reconciliation_id = save_reconciliation_result(
                customer_name=self.customer_name,
                order_id=order_data.get('id'),
                shipment_id=shipment_data.get('id'),
                po_number=po_number,
                match_status='uncertain',
                confidence_score=float(row['overall_score']),
                match_method='recordlinkage',
                match_details=match_details
            )
            
            # Save attribute scores
            attribute_scores = []
            for column in row.index:
                if column not in ['match_status', 'match_method', 'overall_score'] and not column.endswith('_weighted'):
                    attribute_name = column.split('_')[0]
                    attribute_scores.append({
                        'attribute_name': attribute_name,
                        'order_value': str(order_data.get(attribute_name, '')),
                        'shipment_value': str(shipment_data.get(attribute_name, '')),
                        'match_score': float(row[column]),
                        'match_method': column.split('_')[1] if '_' in column else 'exact',
                        'is_key_attribute': attribute_name in self.config.get('key_attributes', []),
                        'weight': self.weights.get(attribute_name, 1.0)
                    })
            
            save_attribute_scores(reconciliation_id, attribute_scores)
            
            # Calculate priority based on confidence score
            # Higher scores get higher priority
            priority = min(int(row['overall_score'] * 10), 9) + 1
            
            # Add to HITL queue
            add_to_hitl_queue(reconciliation_id, priority)
            
            saved_counts['uncertain'] += 1
            saved_counts['hitl_queue'] += 1
        
        # Save unmatched shipments
        for idx, row in unmatched.iterrows():
            shipment_data = row.to_dict()
            
            # Save reconciliation result
            reconciliation_id = save_reconciliation_result(
                customer_name=self.customer_name,
                order_id=None,
                shipment_id=shipment_data.get('id'),
                po_number=po_number,
                match_status='unmatched',
                confidence_score=0.0,
                match_method='recordlinkage',
                match_details={'shipment_data': shipment_data}
            )
            
            # Add to HITL queue with low priority
            add_to_hitl_queue(reconciliation_id, 3)
            
            saved_counts['unmatched'] += 1
            saved_counts['hitl_queue'] += 1
        
        logger.info(f"Saved {saved_counts['matched']} matches, {saved_counts['uncertain']} uncertain, "
                   f"{saved_counts['unmatched']} unmatched, {saved_counts['hitl_queue']} to HITL queue")
        
        return saved_counts


def reconcile_with_recordlinkage(
    customer_name: str,
    po_number: str,
    orders_df: pd.DataFrame,
    shipments_df: pd.DataFrame,
    config: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Reconcile orders and shipments using recordlinkage.
    
    Args:
        customer_name: Name of the customer
        po_number: PO number
        orders_df: DataFrame with orders data
        shipments_df: DataFrame with shipments data
        config: Optional configuration override
        
    Returns:
        Dictionary with reconciliation results
    """
    matcher = RecordLinkageMatcher(customer_name, config)
    
    # Perform matching
    matches, uncertain, unmatched = matcher.match(orders_df, shipments_df)
    
    # Save results to database
    saved_counts = matcher.save_results(matches, uncertain, unmatched, orders_df, shipments_df, po_number)
    
    # Prepare summary results
    results = {
        'customer_name': customer_name,
        'po_number': po_number,
        'total_orders': len(orders_df),
        'total_shipments': len(shipments_df),
        'matched_count': len(matches),
        'uncertain_count': len(uncertain),
        'unmatched_count': len(unmatched),
        'match_percentage': round(len(matches) / len(shipments_df) * 100, 2) if len(shipments_df) > 0 else 0,
        'saved_counts': saved_counts
    }
    
    logger.info(f"Reconciliation complete for {customer_name}, PO: {po_number}")
    logger.info(f"Results: {results}")
    
    return results
