def match(orders, ships, cfg):
    # Store original shipment data for tracking
    original_ships = ships.copy()
    original_ships['_ship_index'] = range(len(original_ships))
    
    # Rename shipment cols to align with order cols per mapping
    ships_renamed = ships.copy()
    ships_renamed = ships_renamed.rename(columns={v: k for k, v in cfg["map"].items()})
    ships_renamed['_ship_index'] = range(len(ships_renamed))
    
    # Use ALL mapped columns that exist in both tables for joining
    join_cols = [order_col for order_col in cfg["map"].keys() 
                 if order_col in orders.columns and order_col in ships_renamed.columns]
    
    # Perform inner join
    matches = orders.merge(ships_renamed, on=join_cols, how="inner", suffixes=("_o", "_s"))
    
    # CRITICAL FIX: Ensure each shipment appears only once in results
    # Take the first match for each shipment (avoid many-to-many duplicates)
    if len(matches) > 0:
        matches = matches.drop_duplicates(subset=['_ship_index'], keep='first')
    
    # Identify matched shipments by their original indices
    if len(matches) > 0 and '_ship_index' in matches.columns:
        matched_indices = set(matches['_ship_index'])
        leftover = original_ships[~original_ships['_ship_index'].isin(matched_indices)]
    else:
        # No matches found
        leftover = original_ships
    
    # Clean up tracking columns
    if '_ship_index' in matches.columns:
        matches = matches.drop(columns=['_ship_index'])
    if '_ship_index' in leftover.columns:
        leftover = leftover.drop(columns=['_ship_index'])
    
    # Add match metadata
    matches["method"], matches["confidence"] = "exact", 1.0
    
    return matches, leftover
