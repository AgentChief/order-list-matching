# TASK013 IMPLEMENTATION PLAN - Comprehensive Schema Review & Movement Table Design

## Executive Summary

**STATUS**: READY FOR IMPLEMENTATION with critical database cleanup needed first  
**SCHEMA ASSESSMENT**: Unified and harmonized throughout data model ✅  
**MOVEMENT TABLE FEASIBILITY**: HIGH - All required components in place  
**CRITICAL ISSUE**: Database folder duplication requires immediate cleanup  

---

## 🚨 CRITICAL FINDING: Database Folder Duplication

### Current State Analysis
- **TWO database folders**: `./database/` AND `./db/` 
- **110 SQL files** in `./db/` vs **34 files** in `./database/`
- **Active development** happening in `./db/` folder
- **Legacy files** remain in `./database/` folder
- **Risk**: Confusion, outdated files, deployment issues

### Immediate Action Required: TASK014 - Database Folder Consolidation

---

## 🏗️ UNIFIED SCHEMA ANALYSIS - Current Data Model

### Source Tables (RAW DATA LAYER)
```sql
-- ✅ AVAILABLE: Orders Source
FACT_ORDER_LIST / ORDERS_UNIFIED
├── Columns: [CUSTOMER NAME], [PO NUMBER], [CUSTOMER STYLE], 
│             [CUSTOMER COLOUR DESCRIPTION], [ORDER DATE PO RECEIVED], [QUANTITY]
├── Grain: Order line level (with SIZE)
├── Primary Key: [id] / [record_uuid]
└── Status: ✅ ESTABLISHED, POPULATED

-- ✅ AVAILABLE: Shipments Source  
FM_orders_shipped
├── Columns: [Customer], [Customer_PO], [Style], [Color],
│             [Shipped_Date], [Qty], [Size]
├── Grain: Shipment line level (with SIZE)
├── Primary Key: [id]
└── Status: ✅ ESTABLISHED, POPULATED
```

### Staging Layer (NORMALIZED + AGGREGATED)
```sql
-- ✅ AVAILABLE: Aggregated Orders
stg_order_list → int_orders_extended → mart_fact_order_list
├── Harmonized column names: customer_name, po_number, style_code, color_description
├── Grain: Order header level (SIZE aggregated into quantity)
├── Business Keys: style_color_key, customer_po_key
└── Status: ✅ ESTABLISHED, FUNCTIONAL

-- ✅ AVAILABLE: Aggregated Shipments
stg_fm_orders_shipped_table → int_shipments_extended → mart_fact_orders_shipped  
├── Harmonized column names: customer_name, po_number, style_code, color_description
├── Grain: Shipment header level (SIZE aggregated, breakdown in size_breakdown)
├── Business Keys: style_color_key, customer_po_key
└── Status: ✅ ESTABLISHED, FUNCTIONAL
```

### Reconciliation Layer (MATCHING RESULTS)
```sql
-- ✅ AVAILABLE: Match Results
reconciliation_result
├── Links: order_id → ORDERS_UNIFIED.id, shipment_id → FM_orders_shipped.id
├── Match Metadata: match_status, confidence_score, match_method
├── Split Handling: is_split_shipment, split_group_id
├── MISSING: match_group UNIQUEIDENTIFIER (NEEDED FOR TASK013)
└── Status: 🔶 NEEDS match_group field for movement table
```

### Performance Layer (MATERIALIZED CACHE)
```sql
-- ✅ AVAILABLE: Summary Cache (TASK001)
shipment_summary_cache
├── Pre-computed indicators and aggregations
├── Performance: <1s queries vs 2-5s original
├── Status: ✅ DEPLOYED AND VALIDATED
└── Usage: Ready for UI integration
```

---

## 📊 MOVEMENT TABLE - ORDER OF OPERATIONS ANALYSIS

### Phase 1: Foundation Setup (Week 1)
```sql
-- Step 1.1: ✅ HAVE - Source Data Ready
FACT_ORDER_LIST    -- Orders with harmonized schema
FM_orders_shipped  -- Shipments with harmonized schema  

-- Step 1.2: ✅ HAVE - Reconciliation Infrastructure
reconciliation_result  -- Match results with confidence scores
-- 🔶 MISSING: match_group UNIQUEIDENTIFIER field for linking

-- Step 1.3: 🆕 CREATE - Movement Table Schema
fact_order_movements
├── movement_type: 'ORDER' | 'SHIP' | 'ORDER_CANCEL'
├── qty: positive for orders, negative for shipments  
├── match_group: UNIQUEIDENTIFIER linking orders to shipments
├── Grain: One row per order or shipment movement
└── Indexes: customer_date, match_group, type_date, style_color
```

### Phase 2: ETL Process Design (Week 1-2)
```sql
-- Step 2.1: 🆕 CREATE - Movement Population Procedure
sp_load_fact_movements
├── INPUT: mart_fact_order_list (ORDER movements, +qty)
├── INPUT: mart_fact_orders_shipped (SHIP movements, -qty)  
├── LINK: reconciliation_result.match_group (to be added)
├── OUTPUT: fact_order_movements populated
└── Mode: Full refresh | Incremental refresh

-- Step 2.2: ✅ HAVE - Integration Points
-- Current reconciliation workflow:
Orders + Shipments → Reconciliation → reconciliation_result
-- New addition:
reconciliation_result → sp_load_fact_movements → fact_order_movements
```

### Phase 3: Query Layer (Week 2-3)
```sql
-- Step 3.1: 🆕 CREATE - Point-in-Time Functions
fn_open_orders_at_date(@as_of_date DATE)
├── Logic: SUM(qty) WHERE movement_date <= @as_of_date
├── Filter: match_flag IN ('EXACT_OK', 'HI_CONF') 
├── Group: customer_name, style_code, color_description  
└── Result: Open order quantities for any historical date

-- Step 3.2: 🆕 CREATE - Movement Analysis Views
vw_movement_summary, vw_shipment_velocity, vw_open_orders_trend
├── Business Intelligence ready queries
├── Power BI connection points
└── Historical analysis capabilities
```

---

## 🔗 UNIFIED SCHEMA VALIDATION

### ✅ HARMONIZED FIELD MAPPING CONFIRMED
```sql
-- ORDERS (Source → Staging → Mart)
[CUSTOMER NAME] → customer_name → customer_name              ✅ Consistent
[PO NUMBER] → po_number → po_number                          ✅ Consistent  
[CUSTOMER STYLE] → style_code → style_code                   ✅ Consistent
[CUSTOMER COLOUR DESCRIPTION] → color_description → color_description ✅ Consistent
[ORDER DATE PO RECEIVED] → order_date → order_date          ✅ Consistent
[QUANTITY] → quantity → quantity                             ✅ Consistent

-- SHIPMENTS (Source → Staging → Mart)  
[Customer] → customer_name → customer_name                   ✅ Consistent
[Customer_PO] → po_number → po_number                        ✅ Consistent
[Style] → style_code → style_code                           ✅ Consistent
[Color] → color_description → color_description             ✅ Consistent
[Shipped_Date] → shipped_date → shipped_date                ✅ Consistent
[Qty] → quantity → quantity                                 ✅ Consistent

-- RECONCILIATION INTEGRATION
reconciliation_result.order_id → ORDERS_UNIFIED.id          ✅ Established
reconciliation_result.shipment_id → FM_orders_shipped.id    ✅ Established
reconciliation_result.match_status → ['matched', 'unmatched', 'uncertain'] ✅ Available
reconciliation_result.confidence_score → FLOAT              ✅ Available
reconciliation_result.split_group_id → NVARCHAR(100)        ✅ Available
```

### 🔶 SCHEMA GAPS FOR MOVEMENT TABLE
```sql
-- MISSING: match_group field in reconciliation_result
ALTER TABLE reconciliation_result ADD 
    match_group UNIQUEIDENTIFIER DEFAULT NEWID();

-- This field will link movements: same match_group = same order/shipment pair
-- Required for movement table population and point-in-time analysis
```

---

## 📋 IMPLEMENTATION READINESS CHECKLIST

### ✅ READY - Infrastructure Components
- [x] Source tables with clean, harmonized schemas
- [x] Staging layer with consistent field names  
- [x] Mart layer with business-ready views
- [x] Reconciliation system with match results
- [x] Performance cache pattern proven (TASK001)
- [x] ETL patterns and stored procedures established

### 🔶 NEEDS COMPLETION - Movement Table Specific
- [ ] Add match_group field to reconciliation_result
- [ ] Create fact_order_movements table schema
- [ ] Build sp_load_fact_movements procedure  
- [ ] Create point-in-time query functions
- [ ] Integrate with existing reconciliation workflow

### 🚨 BLOCKING ISSUES
- [ ] **CRITICAL**: Database folder consolidation (TASK014)
- [ ] **HIGH**: match_group field addition
- [ ] **MEDIUM**: Performance testing with realistic data volumes

---

## 🎯 RECOMMENDED APPROACH

### Week 1: Foundation & Cleanup
1. **Execute TASK014**: Consolidate database folders (CRITICAL BLOCKER)
2. **Add match_group field**: Extend reconciliation_result schema  
3. **Create movement table**: fact_order_movements with proper indexing
4. **Build ETL procedure**: sp_load_fact_movements with incremental logic

### Week 2: Integration & Testing  
1. **Integrate workflow**: Add movement population to reconciliation flow
2. **Create query functions**: Point-in-time analysis capabilities
3. **Data validation**: Verify movement data accuracy vs source
4. **Performance testing**: Validate sub-second aggregation targets

### Week 3: Power BI & Deployment
1. **Power BI model**: Connect to movement table with dynamic DAX
2. **Business validation**: Confirm point-in-time reporting accuracy  
3. **Production deployment**: Rollout with monitoring
4. **Optional**: Daily snapshot table if performance requires

---

## 🔥 NEXT IMMEDIATE ACTIONS

1. **CREATE TASK014**: Database folder consolidation (BLOCKING)
2. **UPDATE TASK013**: Incorporate schema analysis findings
3. **BEGIN PHASE 1**: After TASK014 completion, start movement table implementation
4. **VALIDATE APPROACH**: Confirm business requirements match technical design

**ASSESSMENT: All infrastructure ready, movement table highly feasible, database cleanup required first.**
