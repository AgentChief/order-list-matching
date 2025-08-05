# TASK013 - Consolidated Movement Table Implementation

**Status:** In Progress  
**Added:** August 3, 2025  
**Updated:** August 3, 2025  
**Priority:** HIGH 🚀

## Original Request
Implement a consolidated "movement" table that unifies orders and shipments into a single event-driven structure, enabling point-in-time reporting for invoiced amounts and open order book analysis. This builds directly on our existing reconciliation infrastructure and materialized cache patterns from TASK001.

## Thought Process

### Strategic Vision
Create a single `fact_order_movements` table that:
- **Orders** = positive quantity movements on order_date
- **Shipments** = negative quantity movements on ship_date  
- **Linked by match_group** from our existing reconciliation process
- **Enables point-in-time queries** for any historical date
- **Powers Power BI** with lightning-fast aggregations

### Perfect Synergy with Existing Work
1. **TASK001 Cache Pattern**: Apply same materialized approach to movements
2. **Existing Reconciliation**: `reconciliation_result.match_group` becomes our linking key
3. **Current Schema**: Reuse normalized fields from `FACT_ORDER_LIST` and `FACT_Orders_Shipped`
4. **No Disruption**: Existing reconciliation flow continues unchanged

### Business Impact
- **Point-in-Time Reporting**: "What was our open order book on March 15th?"
- **Power BI Performance**: Sub-second aggregations across years of data
- **Audit Trail**: Complete event history, never lose information
- **Predictive Analytics**: Historical patterns for forecasting

## Definition of Done

- [ ] `fact_order_movements` table created with proper schema and indexing
- [ ] ETL process built to populate from existing `FACT_ORDER_LIST` and `FACT_Orders_Shipped`
- [ ] `match_group` logic integrated from reconciliation results
- [ ] Point-in-time query functions implemented (SQL and Python)
- [ ] Power BI model updated to use movement table
- [ ] Performance testing validates sub-second aggregations
- [ ] Optional: Daily snapshot table for ultra-fast Power BI
- [ ] All existing reconciliation workflows continue unchanged

## Implementation Plan

### Step 1: Create Movement Table Schema
```sql
CREATE TABLE dbo.fact_order_movements (
    movement_id      BIGINT        IDENTITY(1,1) PRIMARY KEY,
    customer_id      INT           NOT NULL,
    customer_name    NVARCHAR(255) NOT NULL,
    aag_order_no     NVARCHAR(64)  NULL,
    po_number        NVARCHAR(50)  NOT NULL,
    style_code       NVARCHAR(100) NOT NULL,
    color_description NVARCHAR(100) NOT NULL,
    delivery_method  NVARCHAR(50)  NULL,
    size_code        NVARCHAR(20)  NULL,
    
    -- Movement specifics
    movement_type    VARCHAR(20)   NOT NULL,   -- ORDER | SHIP | ORDER_CANCEL
    movement_date    DATE          NOT NULL,   -- order_date or ship_date
    qty              INT           NOT NULL,   -- positive for orders, negative for ships
    
    -- Reconciliation links (from existing system)
    match_group      UNIQUEIDENTIFIER NULL,    -- from reconciliation_result
    match_flag       VARCHAR(20)   NULL,       -- EXACT_OK / HI_CONF / LOW_CONF / NO_MATCH
    reconciliation_id INT          NULL,       -- links to reconciliation_result.id
    
    -- Audit and lineage
    source_order_id  INT           NULL,       -- FK to FACT_ORDER_LIST.order_id
    source_shipment_id INT         NULL,       -- FK to FACT_Orders_Shipped.shipment_id
    load_ts          DATETIME2(3)  DEFAULT SYSUTCDATETIME(),
    
    -- Performance indexes
    INDEX IX_movements_customer_date (customer_id, movement_date),
    INDEX IX_movements_match_group (match_group),
    INDEX IX_movements_type_date (movement_type, movement_date),
    INDEX IX_movements_style_color (style_code, color_description)
);
```

### Step 2: ETL Process to Populate Movements
```sql
-- New stored procedure: sp_load_fact_movements
CREATE OR ALTER PROCEDURE dbo.sp_load_fact_movements
    @incremental BIT = 0,
    @batch_date DATE = NULL
AS
BEGIN
    DECLARE @batch UNIQUEIDENTIFIER = NEWID();
    
    -- A. Insert ORDER movements (positive qty)
    INSERT INTO dbo.fact_order_movements
    (customer_name, aag_order_no, po_number, style_code, color_description,
     delivery_method, size_code, movement_type, movement_date, qty,
     match_group, match_flag, reconciliation_id, source_order_id, load_ts)
    SELECT 
        o.customer_name, o.aag_order_number, o.po_number, 
        o.style_code, o.color_description, o.delivery_method, o.size,
        'ORDER', o.order_date, o.quantity,
        rr.match_group, rr.match_status, rr.id, o.order_id, GETDATE()
    FROM dbo.mart_fact_order_list o
    LEFT JOIN dbo.reconciliation_result rr ON o.order_id = rr.order_id
    WHERE (@incremental = 0 OR o.updated_at >= @batch_date);
    
    -- B. Insert SHIP movements (negative qty)  
    INSERT INTO dbo.fact_order_movements
    (customer_name, aag_order_no, po_number, style_code, color_description,
     delivery_method, size_code, movement_type, movement_date, qty,
     match_group, match_flag, reconciliation_id, source_shipment_id, load_ts)
    SELECT 
        s.customer_name, s.aag_order_number, s.po_number,
        s.style_code, s.color_description, s.delivery_method, s.size,
        'SHIP', s.shipped_date, -s.quantity,
        rr.match_group, rr.match_status, rr.id, s.shipment_id, GETDATE()
    FROM dbo.mart_fact_orders_shipped s
    LEFT JOIN dbo.reconciliation_result rr ON s.shipment_id = rr.shipment_id
    WHERE (@incremental = 0 OR s.updated_at >= @batch_date);
END
```

### Step 3: Point-in-Time Query Functions
```sql
-- Function for open order book at any date
CREATE OR ALTER FUNCTION dbo.fn_open_orders_at_date(@as_of_date DATE)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        customer_name,
        style_code,
        color_description,
        size_code,
        SUM(qty) as open_qty,
        COUNT(CASE WHEN movement_type = 'ORDER' THEN 1 END) as order_count,
        COUNT(CASE WHEN movement_type = 'SHIP' THEN 1 END) as shipment_count
    FROM dbo.fact_order_movements
    WHERE movement_date <= @as_of_date
        AND match_flag IN ('EXACT_OK', 'HI_CONF')  -- Only confident matches
    GROUP BY customer_name, style_code, color_description, size_code
    HAVING SUM(qty) > 0  -- Only positive balances (open orders)
);
```

### Step 4: Power BI Integration
Two approaches for Power BI:

**Option A: Dynamic DAX (flexible, good for <10M movements)**
```DAX
OpenQty := 
VAR selDate = MAX('Date'[Date])
RETURN
    CALCULATE(
        SUM(fact_order_movements[qty]),
        FILTER(ALL('Date'), 'Date'[Date] <= selDate),
        fact_order_movements[match_flag] IN {"EXACT_OK", "HI_CONF"}
    )
```

**Option B: Daily Snapshot (ultra-fast, unlimited scale)**
```sql
-- Daily pre-aggregated snapshot
CREATE TABLE dbo.fact_open_orders_daily (
    snapshot_date DATE NOT NULL,
    customer_name NVARCHAR(255) NOT NULL,
    style_code NVARCHAR(100) NOT NULL,
    color_description NVARCHAR(100) NOT NULL,
    open_qty INT NOT NULL,
    order_value DECIMAL(15,2) NULL,
    
    PRIMARY KEY (snapshot_date, customer_name, style_code, color_description)
);
```

### Step 5: Integration with Existing Workflow
The beauty is our existing reconciliation process doesn't change:
1. **Current Flow**: Orders/Shipments → Reconciliation → Results
2. **New Addition**: Results → Movement Table Population
3. **Existing Reports**: Continue working unchanged
4. **New Capability**: Point-in-time analysis now available

## Progress Tracking

**Overall Status:** Blocked by TASK014 - Database Folder Consolidation Required

### Subtasks
| ID | Description | Status | Updated | Notes |
|----|-------------|--------|---------|-------|
| 1.1 | Design movement table schema | Complete | Aug 3 | Schema designed, ready to implement |
| 1.2 | Create ETL stored procedure | Not Started | Aug 3 | Build incremental load logic |
| 1.3 | Implement point-in-time query functions | Not Started | Aug 3 | SQL functions for date-based queries |
| 1.4 | Update existing reconciliation flow | Not Started | Aug 3 | Add movement population step |
| 1.5 | Create Power BI model options | Not Started | Aug 3 | Dynamic DAX vs daily snapshot |
| 1.6 | Performance testing and optimization | Not Started | Aug 3 | Test with realistic data volumes |
| 1.7 | Optional: Daily snapshot implementation | Not Started | Aug 3 | If ultra-performance needed |

## Relevant Files

- `db/ddl/movements/fact_order_movements.sql` - New movement table schema
- `db/procedures/sp_load_fact_movements.sql` - ETL procedure
- `db/functions/fn_open_orders_at_date.sql` - Point-in-time query functions
- `src/reports/movement_analysis.py` - Python analysis tools
- `powerbi/movement_model.pbix` - Power BI model
- `existing reconciliation flow` - Unchanged, just adds movement population

## Test Coverage Mapping

| Implementation Task | Test File | Outcome Validated |
|---------------------|-----------|-------------------|
| Movement table schema | tests/movements/test_schema.py | Correct structure and constraints |
| ETL process | tests/movements/test_etl_logic.py | Data accuracy, incremental logic |
| Point-in-time queries | tests/movements/test_point_in_time.py | Correct open order calculations |
| Reconciliation integration | tests/movements/test_integration.py | No disruption to existing flows |
| Performance at scale | tests/movements/test_performance.py | Sub-second aggregations |

## Progress Log

### August 5, 2025
- **BLOCKED BY TASK014**: Critical database folder duplication discovered during planning phase
- **Schema Analysis Complete**: Unified schema confirmed throughout data model ✅
- **Infrastructure Ready**: All required components available for movement table implementation
- **Critical Blocker**: Must resolve database/ vs db/ folder duplication before proceeding
- **Implementation Plan**: Comprehensive order of operations documented and validated
- **Next Step**: Complete TASK014 database consolidation, then proceed with movement table creation

### August 3, 2025
- **Strategic Decision**: Confirmed this is the perfect evolution of our existing work
- **Schema Design**: Completed movement table structure with proper indexing
- **Integration Plan**: Designed seamless integration with existing reconciliation
- **No Disruption**: All existing workflows continue unchanged
- **Power BI Ready**: Two approaches designed for different scale requirements
- **Next Step**: Begin implementation of movement table and ETL process

## Technical Notes

### Why This is Perfect Timing
1. **TASK001 Complete**: We have the materialized cache pattern proven
2. **Reconciliation Mature**: Match groups and confidence scores are stable
3. **Schema Normalized**: FACT tables ready with clean structures
4. **Performance Focus**: Team understands importance of fast queries

### Implementation Advantages
- **Event Sourcing**: Never lose historical data
- **Audit Trail**: Complete lineage from source to movement
- **Scalability**: Linear performance with proper indexing
- **Flexibility**: Support any date range or aggregation
- **Power BI Ready**: Both dynamic and snapshot approaches available

### Risk Mitigation
- **Existing Flows Unchanged**: No disruption to current operations
- **Incremental Implementation**: Can be rolled out in phases
- **Rollback Plan**: Original tables remain as fallback
- **Performance Monitoring**: Clear metrics for success measurement

**This task represents a strategic leap forward in reporting capabilities while building perfectly on our existing foundation.**
