# Recordlinkage Implementation Plan
**Date:** July 30, 2025
**Status:** Approved for Implementation

## Real Data Insights

Our analysis of GREYSON PO 4755 revealed important insights that validate the recordlinkage + HITL approach:

1. **Split Shipments**: We found multiple shipments (38, 82, 78, 14, 41 units) that should match to a single order of 156 units
2. **Style Variants**: LSP24K59 should be canonicalized to LSP24K88
3. **Quantity Reconciliation**: Total shipped (253) exceeds ordered (156), a business issue that should be highlighted

These exact patterns are common in the business and validate our approach.

## Core Architecture

### Layer 0: Exact Matching with Aliases
- Uses `map_attribute` table for canonicalization
- Joins on canonical values
- Currently achieving 69.7% match rate (115/165)

### Layer 1: Fuzzy Matching with recordlinkage
- Uses weighted multi-attribute comparison
- Considers style, color, PO, delivery method, and quantity
- **Critical Enhancement**: Style MUST be 100% match or forced to LOW_CONF regardless of score
- **Split Shipment Detection**: Consolidates shipments by style+color+PO before comparing quantities
- Classifies as HI_CONF (≥0.85), LOW_CONF (0.60-0.85), NO_MATCH (<0.60)

### Human-in-the-Loop (HITL)
- Streamlit UI for reviewing suggested aliases
- Approve/reject/edit canonical values
- Each approval improves future Layer 0 matching

## Database Schema

```sql
-- Source of truth for canonicalization
CREATE TABLE map_attribute (
    customer_id NVARCHAR(50),
    attr_type NVARCHAR(50),       -- Expanded for custom attributes
    raw_value NVARCHAR(255),
    canon_value NVARCHAR(255),
    confidence DECIMAL(3,2),
    approved_by NVARCHAR(100),
    approved_ts DATETIME2,
    is_required BIT DEFAULT 1,     -- Match requires this attribute
    weight DECIMAL(3,2) DEFAULT 1.0, -- Customer-specific weighting
    PRIMARY KEY (customer_id, attr_type, raw_value)
);

-- Customer attribute configuration
CREATE TABLE customer_attribute_config (
    customer_id NVARCHAR(50),
    attr_type NVARCHAR(50),
    display_name NVARCHAR(100),
    order_column NVARCHAR(100),
    shipment_column NVARCHAR(100),
    is_required BIT DEFAULT 0,
    weight DECIMAL(3,2) DEFAULT 1.0,
    match_tolerance DECIMAL(3,2) DEFAULT 0.0, -- 0.0 = exact match required
    PRIMARY KEY (customer_id, attr_type)
);

-- Human review queue
CREATE TABLE alias_review_queue (
    queue_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(50),
    attr_type NVARCHAR(50),
    raw_value NVARCHAR(255),
    canon_value NVARCHAR(255),
    confidence DECIMAL(3,2),
    status NVARCHAR(20) DEFAULT 'PENDING',
    suggested_by NVARCHAR(100),
    suggested_ts DATETIME2 DEFAULT GETDATE(),
    approved_by NVARCHAR(100),
    approved_ts DATETIME2,
    notes NVARCHAR(500)
);

-- Reconciliation results
CREATE TABLE recon_results_tmp (
    shipment_id NVARCHAR(50),
    order_id NVARCHAR(50),
    match_flag NVARCHAR(20),
    score DECIMAL(3,2),
    created_ts DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT pk_recon_results PRIMARY KEY (shipment_id, order_id)
);
```

## Key Algorithm Enhancements

### 1. Style Exact Match Requirement
```python
# Force human review for any style mismatches regardless of overall score
if style_similarity < 1.0:  # Not an exact match
    match_classification = "LOW_CONF"  # Force human review
```

### 2. Split Shipment Detection
```python
# Pre-process to detect split shipments
consolidated_shipments = {}
for ship in unmatched_ships:
    key = (ship.style_norm, ship.color_norm, ship.po_norm)
    if key not in consolidated_shipments:
        consolidated_shipments[key] = []
    consolidated_shipments[key].append(ship)

# Compare consolidated quantities with orders
for key, ships in consolidated_shipments.items():
    style, color, po = key
    matching_orders = orders[
        (orders.style_norm == style) & 
        (orders.color_norm == color) & 
        (orders.po_norm == po)
    ]
    
    if not matching_orders.empty:
        total_ship_qty = sum(ship.qty for ship in ships)
        for _, order in matching_orders.iterrows():
            qty_diff = abs(total_ship_qty - order.qty) / max(total_ship_qty, order.qty)
            # Apply tolerance check...
```

### 3. Preventing Duplicate Matches
```python
# Post-process to prevent duplicate matches
matched_ship_ids = set()
matched_order_ids = set()
final_matches = []

# Process highest confidence matches first
all_candidate_matches.sort(key=lambda m: m.confidence, reverse=True)

for match in all_candidate_matches:
    ship_id = match.ship_id
    order_id = match.order_id
    
    # Only accept if neither is already matched
    if ship_id not in matched_ship_ids and order_id not in matched_order_ids:
        final_matches.append(match)
        matched_ship_ids.add(ship_id)
        matched_order_ids.add(order_id)
```

## Implementation Phases

### Phase 1: Foundation (1 week)
- [x] Repository restructure completed
- [ ] Create database schema (5 tables)
- [ ] Add recordlinkage to requirements.txt
- [ ] Create database utilities (src/utils/db.py)
- [ ] Migrate existing YAML mappings to map_attribute table

### Phase 2: Core Logic (1 week)
- [ ] Implement `src/recon.py` with recordlinkage pipeline
- [ ] Update alias canonicalization to use database
- [ ] Implement style exact match requirement
- [ ] Implement split shipment detection
- [ ] Implement duplicate match prevention
- [ ] Create simple SQL deduction script
- [ ] Test with GREYSON PO 4755 data

### Phase 3: HITL Interface (1 week)
- [ ] Implement basic Streamlit UI (approve/reject workflow)
- [ ] Add bulk operations
- [ ] Add metrics dashboard
- [ ] Test end-to-end process

## Expected Business Outcomes

1. **Immediate**: Match rate improves from 69.7% to 80%+ after first HITL cycle
2. **Short-term**: Match rate reaches 90%+ as common aliases accumulate
3. **Long-term**: System learns patterns across customers, achieves 95%+ match rate
4. **Business value**: Complete visibility into order-shipment reconciliation
5. **Process improvement**: Focuses human time on exceptions, not routine matching

## Next Steps

1. Set up database tables
2. Migrate current YAML mappings
3. Create database utility functions
4. Begin work on recon.py implementation
