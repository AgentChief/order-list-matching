-- TASK001: Simplified Shipment Summary Cache Schema
-- Drop existing table if it exists
DROP TABLE IF EXISTS dbo.shipment_summary_cache;

-- Create materialized summary table
CREATE TABLE dbo.shipment_summary_cache (
    -- Primary identifiers
    shipment_id INT NOT NULL PRIMARY KEY,
    customer_name NVARCHAR(100) NOT NULL,
    
    -- Row number for UI ordering (pre-computed)
    row_number INT NOT NULL,
    
    -- Core shipment data (denormalized for performance)
    style_code NVARCHAR(50) NOT NULL,
    color_description NVARCHAR(100),
    delivery_method NVARCHAR(50),
    quantity INT NOT NULL,
    
    -- Match indicators (Y/N/P/U) - pre-computed from enhanced_matching_results
    style_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
    color_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
    delivery_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
    quantity_match_indicator CHAR(1) NOT NULL DEFAULT 'U',
    
    -- Match metadata (pre-computed aggregations)
    match_count INT NOT NULL DEFAULT 0,
    match_layers NVARCHAR(50),
    best_confidence DECIMAL(5,2) DEFAULT 0.00,
    avg_confidence DECIMAL(5,2) DEFAULT 0.00,
    
    -- Quantity analysis (pre-computed)
    total_matched_order_qty INT DEFAULT 0,
    quantity_variance INT DEFAULT 0,
    
    -- Overall status classification (pre-computed business rules)
    shipment_status NVARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
    
    -- Outstanding reviews (placeholder for HITL integration)
    outstanding_reviews INT DEFAULT 0,
    
    -- Cache management
    last_updated DATETIME2 DEFAULT GETDATE(),
    source_last_modified DATETIME2
);

-- Create performance indexes
CREATE NONCLUSTERED INDEX IX_customer_status 
ON dbo.shipment_summary_cache (customer_name, shipment_status);

CREATE NONCLUSTERED INDEX IX_status_updated 
ON dbo.shipment_summary_cache (shipment_status, last_updated);

CREATE NONCLUSTERED INDEX IX_confidence 
ON dbo.shipment_summary_cache (best_confidence DESC);

CREATE NONCLUSTERED INDEX IX_indicators 
ON dbo.shipment_summary_cache (style_match_indicator, color_match_indicator, delivery_match_indicator, quantity_match_indicator);

CREATE NONCLUSTERED INDEX IX_row_number 
ON dbo.shipment_summary_cache (row_number);

-- Create additional indexes for common query patterns
CREATE NONCLUSTERED INDEX IX_shipment_summary_cache_customer_lookup 
ON dbo.shipment_summary_cache (customer_name, shipment_status, row_number)
INCLUDE (style_code, color_description, delivery_method, quantity, 
         style_match_indicator, color_match_indicator, delivery_match_indicator, quantity_match_indicator,
         best_confidence, quantity_variance);

GO

-- Performance monitoring view
CREATE VIEW vw_shipment_summary_cache_stats AS
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_name) as unique_customers,
    AVG(CAST(match_count AS FLOAT)) as avg_matches_per_shipment,
    
    -- Status distribution
    SUM(CASE WHEN shipment_status = 'GOOD' THEN 1 ELSE 0 END) as good_count,
    SUM(CASE WHEN shipment_status = 'QUANTITY_ISSUES' THEN 1 ELSE 0 END) as quantity_issues_count,
    SUM(CASE WHEN shipment_status = 'DELIVERY_ISSUES' THEN 1 ELSE 0 END) as delivery_issues_count,
    SUM(CASE WHEN shipment_status = 'UNMATCHED' THEN 1 ELSE 0 END) as unmatched_count,
    SUM(CASE WHEN shipment_status = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown_count,
    
    -- Match indicator distribution  
    SUM(CASE WHEN style_match_indicator = 'Y' THEN 1 ELSE 0 END) as style_match_yes,
    SUM(CASE WHEN color_match_indicator = 'Y' THEN 1 ELSE 0 END) as color_match_yes,
    
    -- Performance metrics
    MAX(last_updated) as last_cache_refresh,
    MIN(last_updated) as oldest_cache_entry
FROM dbo.shipment_summary_cache;
