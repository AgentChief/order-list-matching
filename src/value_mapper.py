"""
Value Mapping System for Order-Shipment Reconciliation

This module handles mapping between different value formats used in order systems (Excel) 
and shipment systems (FileMaker) to improve matching accuracy.
"""
import yaml
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import re
from difflib import SequenceMatcher
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ValueMapper:
    """Handles value mappings between order and shipment systems"""
    
    def __init__(self, config_path: Union[str, Path] = None):
        """Initialize mapper with configuration file"""
        if config_path is None:
            # Updated path for new repository structure
            config_path = Path(__file__).parent.parent / "config" / "value_mappings.yaml"
        
        self.config_path = Path(config_path)
        self.mappings = self._load_mappings()
        
    def _load_mappings(self) -> Dict:
        """Load mapping configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Mapping config not found at {self.config_path}, using empty mappings")
            return {"global_mappings": {}, "customer_specific_mappings": {}}
    
    def save_mappings(self):
        """Save current mappings back to YAML file"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.mappings, f, default_flow_style=False, indent=2)
    
    def get_mapped_value(self, column: str, value: str, direction: str = "order_to_shipment", 
                        customer: str = None) -> Tuple[Optional[str], float]:
        """
        Get mapped value for a given column and value
        
        Args:
            column: Column name (e.g., 'PLANNED_DELIVERY_METHOD')
            value: Value to map
            direction: 'order_to_shipment' or 'shipment_to_order'
            customer: Customer name for customer-specific mappings
            
        Returns:
            Tuple of (mapped_value, confidence) or (None, 0.0) if no mapping found
        """
        # Check customer-specific mappings first
        if customer:
            mapped_val, conf = self._check_customer_mappings(column, value, direction, customer)
            if mapped_val:
                return mapped_val, conf
        
        # Check global mappings
        return self._check_global_mappings(column, value, direction)
    
    def _check_customer_mappings(self, column: str, value: str, direction: str, 
                                customer: str) -> Tuple[Optional[str], float]:
        """Check customer-specific mappings"""
        customer_mappings = self.mappings.get("customer_specific_mappings", {}).get(customer, {})
        if column not in customer_mappings:
            return None, 0.0
            
        return self._find_mapping_in_config(customer_mappings[column], value, direction)
    
    def _check_global_mappings(self, column: str, value: str, direction: str) -> Tuple[Optional[str], float]:
        """Check global mappings"""
        global_mappings = self.mappings.get("global_mappings", {})
        if column not in global_mappings:
            return None, 0.0
            
        return self._find_mapping_in_config(global_mappings[column], value, direction)
    
    def _find_mapping_in_config(self, config: Dict, value: str, direction: str) -> Tuple[Optional[str], float]:
        """Find mapping in a specific configuration section"""
        mappings = config.get("mappings", [])
        
        # Check exact mappings first
        for mapping in mappings:
            if direction == "order_to_shipment":
                if mapping.get("order_value", "").upper() == value.upper():
                    # Return first shipment value (could be multiple)
                    shipment_vals = mapping.get("shipment_values", [])
                    if shipment_vals:
                        return shipment_vals[0], mapping.get("confidence", 1.0)
            else:  # shipment_to_order
                shipment_vals = [v.upper() for v in mapping.get("shipment_values", [])]
                if value.upper() in shipment_vals:
                    return mapping.get("order_value"), mapping.get("confidence", 1.0)
        
        # Check fuzzy rules if enabled
        fuzzy_rules = config.get("fuzzy_rules", {})
        if fuzzy_rules.get("enabled", False):
            return self._apply_fuzzy_rules(fuzzy_rules, mappings, value, direction)
        
        return None, 0.0
    
    def _apply_fuzzy_rules(self, fuzzy_rules: Dict, mappings: List[Dict], 
                          value: str, direction: str) -> Tuple[Optional[str], float]:
        """Apply fuzzy matching rules"""
        threshold = fuzzy_rules.get("threshold", 0.80)
        rules = fuzzy_rules.get("rules", [])
        
        for mapping in mappings:
            if direction == "order_to_shipment":
                target_val = mapping.get("order_value", "")
                result_vals = mapping.get("shipment_values", [])
            else:
                target_val = mapping.get("shipment_values", [""])[0]  # Use first shipment value
                result_vals = [mapping.get("order_value", "")]
            
            # Apply each fuzzy rule
            for rule in rules:
                pattern = rule.get("pattern", "")
                confidence = rule.get("confidence", 0.5)
                
                if self._matches_fuzzy_pattern(value, target_val, pattern, threshold):
                    if result_vals and result_vals[0]:
                        return result_vals[0], confidence
        
        return None, 0.0
    
    def _matches_fuzzy_pattern(self, value1: str, value2: str, pattern: str, threshold: float) -> bool:
        """Check if two values match according to a fuzzy pattern"""
        if pattern == "case_insensitive":
            return value1.upper() == value2.upper()
        
        elif pattern == "remove_spaces_dashes":
            clean1 = re.sub(r'[\s\-_]', '', value1.upper())
            clean2 = re.sub(r'[\s\-_]', '', value2.upper())
            return clean1 == clean2
        
        elif pattern == "extract_color_name":
            # Extract color name from "CODE - COLOR" format
            color1 = re.sub(r'^\d+\s*-\s*', '', value1).strip().upper()
            color2 = re.sub(r'^\d+\s*-\s*', '', value2).strip().upper()
            return color1 == color2
        
        elif pattern == "normalize_punctuation":
            norm1 = re.sub(r'[^\w\s]', ' ', value1.upper()).strip()
            norm2 = re.sub(r'[^\w\s]', ' ', value2.upper()).strip()
            return norm1 == norm2
        
        elif pattern == "remove_leading_zeros":
            clean1 = value1.lstrip('0') or '0'
            clean2 = value2.lstrip('0') or '0'
            return clean1 == clean2
        
        else:
            # Default to sequence matching
            similarity = SequenceMatcher(None, value1.upper(), value2.upper()).ratio()
            return similarity >= threshold
    
    def suggest_mappings(self, unmatched_pairs: List[Tuple[str, str, str]]) -> Dict:
        """
        Suggest new mappings based on unmatched pairs
        
        Args:
            unmatched_pairs: List of (column, order_value, shipment_value) tuples
            
        Returns:
            Dictionary with suggested mappings
        """
        suggestions = {
            "suggested_mappings": [],
            "analysis": {
                "total_pairs": len(unmatched_pairs),
                "by_column": {}
            }
        }
        
        # Group by column
        by_column = {}
        for column, order_val, shipment_val in unmatched_pairs:
            if column not in by_column:
                by_column[column] = []
            by_column[column].append((order_val, shipment_val))
        
        suggestions["analysis"]["by_column"] = {k: len(v) for k, v in by_column.items()}
        
        # Analyze each column
        for column, pairs in by_column.items():
            column_suggestions = self._analyze_column_patterns(column, pairs)
            suggestions["suggested_mappings"].extend(column_suggestions)
        
        return suggestions
    
    def _analyze_column_patterns(self, column: str, pairs: List[Tuple[str, str]]) -> List[Dict]:
        """Analyze patterns in unmatched pairs for a specific column"""
        suggestions = []
        
        # Count frequency of each value pair
        pair_counts = {}
        for order_val, shipment_val in pairs:
            key = (order_val, shipment_val)
            pair_counts[key] = pair_counts.get(key, 0) + 1
        
        # Sort by frequency (most common first)
        sorted_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)
        
        for (order_val, shipment_val), count in sorted_pairs:
            # Calculate confidence based on frequency and pattern analysis
            confidence = min(0.95, 0.5 + (count / len(pairs)) * 0.4)
            
            # Determine pattern type
            pattern_type = self._determine_pattern_type(order_val, shipment_val)
            
            # Generate rationale
            rationale = self._generate_rationale(column, order_val, shipment_val, count, pattern_type)
            
            suggestions.append({
                "column": column,
                "order_value": order_val,
                "shipment_value": shipment_val,
                "confidence": confidence,
                "rationale": rationale,
                "pattern_type": pattern_type,
                "frequency": count
            })
        
        return suggestions
    
    def _determine_pattern_type(self, order_val: str, shipment_val: str) -> str:
        """Determine the type of pattern between two values"""
        if order_val == shipment_val:
            return "exact"
        elif order_val.upper() == shipment_val.upper():
            return "case_variation"
        elif re.sub(r'[\s\-_]', '', order_val.upper()) == re.sub(r'[\s\-_]', '', shipment_val.upper()):
            return "format_variation"
        elif SequenceMatcher(None, order_val.upper(), shipment_val.upper()).ratio() > 0.8:
            return "fuzzy"
        else:
            return "business_rule"
    
    def _generate_rationale(self, column: str, order_val: str, shipment_val: str, 
                           count: int, pattern_type: str) -> str:
        """Generate human-readable rationale for a mapping suggestion"""
        base = f"'{order_val}' in orders appears {count} time(s) with '{shipment_val}' in shipments"
        
        if pattern_type == "exact":
            return f"{base} - exact match"
        elif pattern_type == "case_variation":
            return f"{base} - same value with case difference"
        elif pattern_type == "format_variation":
            return f"{base} - same value with formatting differences (spaces, dashes)"
        elif pattern_type == "fuzzy":
            return f"{base} - similar values, likely same meaning"
        else:
            return f"{base} - business rule mapping needed"
    
    def add_mapping(self, column: str, order_value: str, shipment_values: List[str], 
                   confidence: float, rationale: str, customer: str = None):
        """Add a new mapping to the configuration"""
        mapping = {
            "order_value": order_value,
            "shipment_values": shipment_values,
            "confidence": confidence,
            "rationale": rationale,
            "bidirectional": True,
            "fuzzy_match": confidence < 1.0
        }
        
        if customer:
            # Add to customer-specific mappings
            if "customer_specific_mappings" not in self.mappings:
                self.mappings["customer_specific_mappings"] = {}
            if customer not in self.mappings["customer_specific_mappings"]:
                self.mappings["customer_specific_mappings"][customer] = {}
            if column not in self.mappings["customer_specific_mappings"][customer]:
                self.mappings["customer_specific_mappings"][customer][column] = {"mappings": []}
            
            self.mappings["customer_specific_mappings"][customer][column]["mappings"].append(mapping)
        else:
            # Add to global mappings
            if "global_mappings" not in self.mappings:
                self.mappings["global_mappings"] = {}
            if column not in self.mappings["global_mappings"]:
                self.mappings["global_mappings"][column] = {"mappings": []}
            
            self.mappings["global_mappings"][column]["mappings"].append(mapping)


# Utility functions for integration with existing reconciliation system
def apply_value_mappings(df, column_mappings: Dict[str, str], customer: str = None) -> int:
    """
    Apply value mappings to a DataFrame
    
    Args:
        df: DataFrame to modify
        column_mappings: Dict mapping order columns to shipment columns
        customer: Customer name for customer-specific mappings
        
    Returns:
        Number of values that were mapped
    """
    mapper = ValueMapper()
    mapped_count = 0
    
    for order_col, shipment_col in column_mappings.items():
        if order_col in df.columns:
            for idx, value in df[order_col].items():
                if pd.isna(value):
                    continue
                    
                mapped_val, confidence = mapper.get_mapped_value(
                    order_col, str(value), "order_to_shipment", customer
                )
                
                if mapped_val and confidence >= 0.7:  # Only apply high-confidence mappings
                    df.at[idx, order_col] = mapped_val
                    mapped_count += 1
                    logger.debug(f"Mapped {order_col}: '{value}' -> '{mapped_val}' (confidence: {confidence})")
    
    return mapped_count


if __name__ == "__main__":
    # Example usage
    mapper = ValueMapper()
    
    # Test mapping
    result, conf = mapper.get_mapped_value("PLANNED_DELIVERY_METHOD", "FAST BOAT", customer="GREYSON")
    print(f"Mapping result: {result} (confidence: {conf})")
    
    # Test suggestions
    unmatched = [
        ("PLANNED_DELIVERY_METHOD", "FAST BOAT", "SEA-FB"),
        ("PLANNED_DELIVERY_METHOD", "AIR", "AIR_FREIGHT"),
        ("CUSTOMER_COLOUR_DESCRIPTION", "417 - MALTESE BLUE", "MALTESE BLUE")
    ]
    
    suggestions = mapper.suggest_mappings(unmatched)
    print(f"Suggestions: {json.dumps(suggestions, indent=2)}")
