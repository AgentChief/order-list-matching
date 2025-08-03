from rapidfuzz import fuzz, process
import pandas as pd

def match(orders, ships, cfg):
    thresh = cfg.get("fuzzy_threshold", 0.9)
    
    # If no ships to process, return empty results
    if ships.empty:
        return pd.DataFrame(), ships
    
    # Create fuzzy mappings for colors and shipping methods
    ships_processed = ships.copy()
    fuzzy_matches_made = False
    
    # 1. Fuzzy matching for colors
    color_palette = orders["CUSTOMER COLOUR DESCRIPTION"].unique()
    color_col = None
    for col in ["CUSTOMER COLOUR DESCRIPTION", "COLOR", "Color"]:
        if col in ships_processed.columns:
            color_col = col
            break
    
    if color_col is not None:
        color_mapping = {c: process.extractOne(c, color_palette, scorer=fuzz.token_set_ratio) 
                        for c in ships_processed[color_col].unique()}
        ships_processed["Color_fuzzy"] = ships_processed[color_col].map(
            lambda c: color_mapping[c][0] if color_mapping[c][1]/100 >= thresh else None)
        
        # Apply fuzzy color matches
        color_matches = ships_processed["Color_fuzzy"].notna()
        if color_matches.any():
            ships_processed.loc[color_matches, color_col] = ships_processed.loc[color_matches, "Color_fuzzy"]
            fuzzy_matches_made = True
        ships_processed = ships_processed.drop("Color_fuzzy", axis=1)
    
    # 2. Fuzzy matching for shipping methods
    shipping_palette = orders["PLANNED DELIVERY METHOD"].unique()
    shipping_col = None
    for col in ["PLANNED DELIVERY METHOD", "Shipping_Method"]:
        if col in ships_processed.columns:
            shipping_col = col
            break
    
    if shipping_col is not None:
        shipping_mapping = {s: process.extractOne(s, shipping_palette, scorer=fuzz.token_set_ratio) 
                           for s in ships_processed[shipping_col].unique()}
        ships_processed["Shipping_fuzzy"] = ships_processed[shipping_col].map(
            lambda s: shipping_mapping[s][0] if shipping_mapping[s][1]/100 >= thresh else None)
        
        # Apply fuzzy shipping matches
        shipping_matches = ships_processed["Shipping_fuzzy"].notna()
        if shipping_matches.any():
            ships_processed.loc[shipping_matches, shipping_col] = ships_processed.loc[shipping_matches, "Shipping_fuzzy"]
            fuzzy_matches_made = True
        ships_processed = ships_processed.drop("Shipping_fuzzy", axis=1)
    
    # 3. Now try to match with orders using fuzzy-corrected data
    if not fuzzy_matches_made:
        # No fuzzy corrections possible, return empty results
        return pd.DataFrame(), ships
    
    # CRITICAL FIX: Rename shipment columns to match order columns (like exact matching does)
    ships_for_join = ships_processed.rename(columns={v: k for k, v in cfg["map"].items()})
    
    # Use the same join columns as exact matching (from the map section)
    join_cols = [order_col for order_col in cfg["map"].keys() 
                 if order_col in orders.columns and order_col in ships_for_join.columns]
    
    m = orders.merge(ships_for_join, on=join_cols, how="inner", suffixes=("_o", "_s"))
    m["method"] = "fuzzy"
    
    # Calculate confidence based on fuzzy matches made
    if not m.empty:
        confidences = []
        for _, row in m.iterrows():
            scores = []
            if color_col and color_col+"_o" in m.columns and color_col+"_s" in m.columns:
                scores.append(fuzz.token_set_ratio(row[color_col+"_o"], row[color_col+"_s"]))
            if shipping_col and shipping_col+"_o" in m.columns and shipping_col+"_s" in m.columns:
                scores.append(fuzz.token_set_ratio(row[shipping_col+"_o"], row[shipping_col+"_s"]))
            confidences.append(sum(scores) / len(scores) / 100 if scores else 0.0)
        m["confidence"] = confidences
    else:
        m["confidence"] = pd.Series(dtype='float64')
    
    # Calculate what's left: ships that didn't join with orders after fuzzy corrections
    if not m.empty:
        # Get the original shipment ID column to track which ships were matched
        ship_id_col = ships.columns[0]
        matched_ship_ids = set(m[ship_id_col]) if ship_id_col in m.columns else set()
        left = ships[~ships[ship_id_col].isin(matched_ship_ids)] if ship_id_col in ships.columns else ships
    else:
        left = ships  # No matches, all ships remain
    
    return m, left
