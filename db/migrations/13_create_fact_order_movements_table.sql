-- =====================================================================================
-- TASK013: Fact Order Movements Table - Unified Movement Tracking
-- =====================================================================================
-- Purpose: Event-driven order movement tracking for unified reporting and analytics
-- Features: Point-in-time reporting, split shipment support, Power BI integration
-- Created: March 2025

-- Drop existing table if it exists (for development/testing)
IF OBJECT_ID('dbo.fact_order_movements', 'U') IS NOT NULL
    DROP TABLE dbo.fact_order_movements;

-- Create fact order movements table
CREATE TABLE dbo.fact_order_movements (
    -- Primary key
    movement_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business keys
    order_id NVARCHAR(100) NOT NULL,
    shipment_id INT NULL,
    customer_name NVARCHAR(100) NOT NULL,
    po_number NVARCHAR(100) NOT NULL,
    
    -- Movement tracking
    movement_type NVARCHAR(20) NOT NULL, -- ORDER_PLACED, ORDER_PACKED, SHIPMENT_CREATED, SHIPMENT_SHIPPED, RECONCILED
    movement_date DATETIME2 NOT NULL,
    movement_status NVARCHAR(20) NOT NULL, -- PENDING, IN_PROGRESS, COMPLETED, CANCELLED
    
    -- Order details (denormalized for performance)
    style_code NVARCHAR(50) NOT NULL,
    color_description NVARCHAR(100),
    size_code NVARCHAR(20),
    order_quantity INT NOT NULL,
    unit_price DECIMAL(10,2),
    delivery_method NVARCHAR(50),
    
    -- Shipment details (when applicable)
    shipped_quantity INT NULL,
    shipped_date DATETIME2 NULL,
    tracking_number NVARCHAR(100) NULL,
    
    -- Split shipment support
    split_group_id NVARCHAR(50) NULL, -- Links related partial shipments
    is_primary_shipment BIT DEFAULT 0, -- Identifies the main shipment record
    parent_movement_id BIGINT NULL, -- Links to parent movement for splits
    
    -- Reconciliation metadata
    match_group_id NVARCHAR(50) NULL, -- Links order and shipment movements
    reconciliation_status NVARCHAR(20) DEFAULT 'PENDING', -- PENDING, MATCHED, UNMATCHED, MANUAL_REVIEW
    reconciliation_confidence DECIMAL(5,2) NULL,
    reconciliation_method NVARCHAR(50) NULL, -- LAYER_0, LAYER_1, LAYER_2, LAYER_3, MANUAL
    reconciliation_date DATETIME2 NULL,
    
    -- Quantity analysis
    quantity_variance INT NULL, -- For shipments: shipped_qty - order_qty
    quantity_variance_percent DECIMAL(5,2) NULL,
    cumulative_shipped_quantity INT DEFAULT 0, -- Running total for order
    remaining_quantity INT NULL, -- order_qty - cumulative_shipped_qty
    
    -- Business context
    order_type NVARCHAR(20), -- STANDARD, RUSH, SAMPLE, PREORDER
    priority_level NVARCHAR(10) DEFAULT 'NORMAL', -- HIGH, NORMAL, LOW
    expected_ship_date DATE NULL,
    promised_delivery_date DATE NULL,
    
    -- Financial context
    extended_price DECIMAL(12,2) NULL, -- quantity * unit_price
    currency_code CHAR(3) DEFAULT 'USD',
    
    -- Audit fields
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    
    -- Source system tracking
    source_system NVARCHAR(50) DEFAULT 'RECONCILIATION_ENGINE',
    source_record_id NVARCHAR(100),
    batch_id INT NULL, -- Links to reconciliation_batch
    
    -- Constraints
    CONSTRAINT CK_movement_type CHECK (movement_type IN (
        'ORDER_PLACED', 'ORDER_PACKED', 'SHIPMENT_CREATED', 'SHIPMENT_SHIPPED', 
        'RECONCILED', 'CANCELLED', 'RETURNED', 'DELIVERED'
    )),
    CONSTRAINT CK_movement_status CHECK (movement_status IN (
        'PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED', 'ERROR'
    )),
    CONSTRAINT CK_reconciliation_status CHECK (reconciliation_status IN (
        'PENDING', 'MATCHED', 'UNMATCHED', 'MANUAL_REVIEW', 'APPROVED', 'REJECTED'
    )),
    CONSTRAINT CK_order_type CHECK (order_type IN (
        'STANDARD', 'RUSH', 'SAMPLE', 'PREORDER', 'BACKORDER', 'DROPSHIP'
    )),
    CONSTRAINT CK_priority_level CHECK (priority_level IN ('HIGH', 'NORMAL', 'LOW')),
    CONSTRAINT CK_confidence_range CHECK (reconciliation_confidence >= 0.00 AND reconciliation_confidence <= 1.00),
    
    -- Self-referencing foreign key for parent movements
    CONSTRAINT FK_parent_movement FOREIGN KEY (parent_movement_id) 
        REFERENCES dbo.fact_order_movements(movement_id)
);

GO

-- Create performance indexes
CREATE NONCLUSTERED INDEX IX_fact_order_movements_order_lookup 
ON dbo.fact_order_movements (order_id, movement_type, movement_date DESC);

CREATE NONCLUSTERED INDEX IX_fact_order_movements_shipment_lookup 
ON dbo.fact_order_movements (shipment_id, movement_type, movement_date DESC)
WHERE shipment_id IS NOT NULL;

CREATE NONCLUSTERED INDEX IX_fact_order_movements_customer_po 
ON dbo.fact_order_movements (customer_name, po_number, movement_date DESC);

CREATE NONCLUSTERED INDEX IX_fact_order_movements_reconciliation 
ON dbo.fact_order_movements (reconciliation_status, match_group_id, reconciliation_date DESC);

CREATE NONCLUSTERED INDEX IX_fact_order_movements_split_group 
ON dbo.fact_order_movements (split_group_id, is_primary_shipment)
WHERE split_group_id IS NOT NULL;

CREATE NONCLUSTERED INDEX IX_fact_order_movements_date_range 
ON dbo.fact_order_movements (movement_date, movement_type, customer_name);

CREATE NONCLUSTERED INDEX IX_fact_order_movements_style_analysis 
ON dbo.fact_order_movements (style_code, color_description, movement_type, movement_date DESC);

GO

-- Create covering index for common reporting queries
CREATE NONCLUSTERED INDEX IX_fact_order_movements_reporting 
ON dbo.fact_order_movements (customer_name, movement_type, reconciliation_status, movement_date DESC)
INCLUDE (order_id, shipment_id, po_number, style_code, color_description, 
         order_quantity, shipped_quantity, quantity_variance, reconciliation_confidence);

GO

-- Add match_group field to reconciliation_result table
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.reconciliation_result') AND name = 'match_group_id')
BEGIN
    ALTER TABLE dbo.reconciliation_result 
    ADD match_group_id NVARCHAR(50) NULL;
    
    CREATE INDEX IX_reconciliation_result_match_group 
    ON dbo.reconciliation_result (match_group_id);
    
    PRINT 'Added match_group_id to reconciliation_result table';
END

GO

-- Create view for point-in-time order status
CREATE VIEW vw_order_status_summary AS
WITH OrderMovements AS (
    SELECT 
        order_id,
        customer_name,
        po_number,
        style_code,
        color_description,
        MAX(CASE WHEN movement_type = 'ORDER_PLACED' THEN movement_date END) as order_date,
        MAX(CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN movement_date END) as last_shipped_date,
        SUM(CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN shipped_quantity ELSE 0 END) as total_shipped_qty,
        MAX(order_quantity) as order_quantity,
        MAX(CASE WHEN movement_type = 'RECONCILED' THEN reconciliation_confidence END) as best_reconciliation_confidence,
        COUNT(CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN 1 END) as shipment_count,
        MAX(updated_at) as last_updated
    FROM dbo.fact_order_movements
    GROUP BY order_id, customer_name, po_number, style_code, color_description
)
SELECT 
    order_id,
    customer_name,
    po_number,
    style_code,
    color_description,
    order_date,
    last_shipped_date,
    order_quantity,
    total_shipped_qty,
    order_quantity - total_shipped_qty as remaining_quantity,
    CASE 
        WHEN total_shipped_qty = 0 THEN 'NOT_SHIPPED'
        WHEN total_shipped_qty >= order_quantity THEN 'FULLY_SHIPPED'
        WHEN total_shipped_qty < order_quantity THEN 'PARTIALLY_SHIPPED'
        ELSE 'UNKNOWN'
    END as fulfillment_status,
    shipment_count,
    best_reconciliation_confidence,
    CASE 
        WHEN shipment_count = 0 THEN 'NO_SHIPMENTS'
        WHEN shipment_count = 1 THEN 'SINGLE_SHIPMENT'
        ELSE 'SPLIT_SHIPMENTS'
    END as shipment_pattern,
    last_updated
FROM OrderMovements;

GO

-- Create view for open order book (unfulfilled orders)
CREATE VIEW vw_open_order_book AS
SELECT 
    order_id,
    customer_name,
    po_number,
    style_code,
    color_description,
    order_date,
    order_quantity,
    total_shipped_qty,
    remaining_quantity,
    DATEDIFF(DAY, order_date, GETDATE()) as days_since_order,
    CASE 
        WHEN DATEDIFF(DAY, order_date, GETDATE()) <= 7 THEN 'RECENT'
        WHEN DATEDIFF(DAY, order_date, GETDATE()) <= 30 THEN 'NORMAL'
        WHEN DATEDIFF(DAY, order_date, GETDATE()) <= 90 THEN 'AGING'
        ELSE 'CRITICAL'
    END as aging_category
FROM vw_order_status_summary
WHERE fulfillment_status IN ('NOT_SHIPPED', 'PARTIALLY_SHIPPED')
    AND remaining_quantity > 0;

GO

-- Create summary statistics view
CREATE VIEW vw_movement_analytics AS
SELECT 
    -- Date range
    MIN(movement_date) as earliest_movement,
    MAX(movement_date) as latest_movement,
    
    -- Order metrics
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT CASE WHEN movement_type = 'ORDER_PLACED' THEN order_id END) as orders_placed,
    COUNT(DISTINCT CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN order_id END) as orders_with_shipments,
    
    -- Shipment metrics
    COUNT(DISTINCT shipment_id) as total_shipments,
    COUNT(CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN 1 END) as shipped_movements,
    
    -- Reconciliation metrics
    COUNT(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 END) as matched_movements,
    COUNT(CASE WHEN reconciliation_status = 'UNMATCHED' THEN 1 END) as unmatched_movements,
    COUNT(CASE WHEN reconciliation_status = 'MANUAL_REVIEW' THEN 1 END) as review_movements,
    
    -- Split shipment analysis
    COUNT(DISTINCT split_group_id) as split_groups,
    COUNT(CASE WHEN split_group_id IS NOT NULL THEN 1 END) as split_movements,
    
    -- Layer distribution
    COUNT(CASE WHEN reconciliation_method = 'LAYER_0' THEN 1 END) as layer_0_matches,
    COUNT(CASE WHEN reconciliation_method = 'LAYER_1' THEN 1 END) as layer_1_matches,
    COUNT(CASE WHEN reconciliation_method = 'LAYER_2' THEN 1 END) as layer_2_matches,
    COUNT(CASE WHEN reconciliation_method = 'LAYER_3' THEN 1 END) as layer_3_matches,
    
    -- Performance metrics
    AVG(reconciliation_confidence) as avg_confidence,
    AVG(ABS(quantity_variance_percent)) as avg_quantity_variance_pct,
    
    -- Freshness
    MAX(updated_at) as last_updated
FROM dbo.fact_order_movements;

GO

-- Grant permissions (adjust as needed for your security model)
-- GRANT SELECT ON dbo.fact_order_movements TO [YourStreamlitUser];
-- GRANT SELECT ON dbo.vw_order_status_summary TO [YourStreamlitUser];
-- GRANT SELECT ON dbo.vw_open_order_book TO [YourStreamlitUser];
-- GRANT SELECT ON dbo.vw_movement_analytics TO [YourStreamlitUser];

PRINT 'Created fact_order_movements table with supporting views';
PRINT 'Movement table ready for event capture and analytics';
PRINT 'Added match_group_id to reconciliation_result table';