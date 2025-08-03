# Repository Migration & Enhancement Analysis
**Date:** July 30, 2025
**Status:** In Progress

## PART 1: STRUCTURE CLEANUP (COMPLETED)

### Current Structure Issues
1. **Main application code trapped in subfolder:** `order-match-lm/`
2. **Duplicate configurations:** Multiple `canonical_customers.yaml`, `requirements.txt`
3. **Testing chaos:** Debug scripts mixed with core code
4. **Import path confusion:** Relative imports failing

### Core Directories to Promote
- `order-match-lm/src/` → `src/` ✅ COMPLETE
- `order-match-lm/config/` → `config/` ✅ COMPLETE
- `order-match-lm/reports/` → `reports/` ✅ COMPLETE
- `order-match-lm/docs/` → `docs/` ✅ COMPLETE
- `order-match-lm/utils/` → `utils/` ✅ COMPLETE

### Debug/Test Files to Organize
- `order-match-lm/debug_*.py` → `tests/debug/` ✅ COMPLETE
- `order-match-lm/analyze_*.py` → `tests/integration/` ✅ COMPLETE
- `order-match-lm/test_*.py` → `tests/unit/` ✅ COMPLETE

## PART 2: ARCHITECTURAL ENHANCEMENT PLAN

### 1. Enhanced Matching Framework
- **Current:** rapidfuzz + LLM for fuzzy matching
- **Target:** recordlinkage library for structured entity resolution
- **Benefits:**
  - Full pipeline (blocking, comparison, classification)
  - Multi-attribute scoring (style, color, delivery, PO)
  - Extensible for ML classifier integration
  - Auditability through feature matrices

### 2. Database Tables to Create

#### `map_attribute` - Source of Truth for Canonicalization
```sql
CREATE TABLE map_attribute (
    customer_id NVARCHAR(50),
    attr_type NVARCHAR(20),        -- 'style', 'colour', 'po', 'delivery_method'
    raw_value NVARCHAR(255),
    canon_value NVARCHAR(255),
    confidence DECIMAL(3,2),
    approved_by NVARCHAR(100),
    approved_ts DATETIME2,
    PRIMARY KEY (customer_id, attr_type, raw_value)
);
```

#### `alias_review_queue` - Human-in-the-Loop Work Queue
```sql
CREATE TABLE alias_review_queue (
    queue_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id NVARCHAR(50),
    attr_type NVARCHAR(20),
    raw_value NVARCHAR(255),
    canon_value NVARCHAR(255),
    confidence DECIMAL(3,2),
    status NVARCHAR(20) DEFAULT 'PENDING',  -- PENDING/APPROVED/REJECTED
    suggested_by NVARCHAR(100),
    suggested_ts DATETIME2 DEFAULT GETDATE(),
    approved_by NVARCHAR(100),
    approved_ts DATETIME2,
    notes NVARCHAR(500)
);
```

#### `recon_results_tmp` - Reconciliation Results
```sql
CREATE TABLE recon_results_tmp (
    shipment_id NVARCHAR(50),
    order_id NVARCHAR(50),
    match_flag NVARCHAR(20),       -- EXACT_OK/EXACT_QTY_MISMATCH/HI_CONF/LOW_CONF/NO_MATCH
    score DECIMAL(3,2),
    created_ts DATETIME2 DEFAULT GETDATE()
);
```

### 3. New Files to Create

#### Core Logic
- `src/recon.py` - Main reconciliation engine
- `src/utils/db.py` - Database utilities
- `config/db.yaml` - Database connection configuration
- `config/recon.yml` - Matching parameters and thresholds

#### UI Components
- `ui/alias_manager.py` - Streamlit-based HITL interface
- `ui/components/` - UI component library
- `ui/static/` - Static assets

#### SQL Scripts
- `sql/00_schema/01_alias_tables.sql` - Table definitions
- `sql/10_deduction/apply_deductions.sql` - Business logic

### 4. Implementation Approach

#### Layer 0: Deterministic Matching
- Use approved aliases from `map_attribute` table
- Canonicalize raw values before matching
- Join on normalized values for exact matches

#### Layer 1: Fuzzy Matching with recordlinkage
- Block by customer_id to limit candidate pairs
- Compare columns with weighted scoring:
  - Style (weight: 3)
  - Color (weight: 2)
  - PO Number (weight: 3)
  - Delivery Method (weight: 1)
  - Quantity (weight: 1)
- Classify matches by confidence:
  - ≥0.85 → HI_CONF (auto-approve, queue for review)
  - 0.60-0.85 → LOW_CONF (queue for review)
  - <0.60 → NO_MATCH

#### Human-in-the-Loop (HITL) Process
- Review suggested aliases in Streamlit UI
- Approve/reject/edit suggestions
- Approved aliases move to `map_attribute` table
- Future runs use approved aliases for deterministic matching

### 5. Migration Strategy
1. Set up database tables
2. Implement core reconciliation logic
3. Develop basic HITL interface
4. Migrate current YAML mappings to database
5. Test with existing data
6. Deploy with metrics dashboard
