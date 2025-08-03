-- =====================================================================================
-- TASK001: Materialized Summary Tables - Performance Optimization
-- =====================================================================================
-- Purpose: Pre-computed shipment summary cache to replace complex real-time aggregation
-- Performance: Target <1 second response vs current 2-5 seconds
-- Scalability: Support 10,000+ shipments with linear scaling
-- Created: August 3, 2025

-- Drop existing table if it exists (for development/testing)
IF OBJECT_ID('dbo.shipment_summary_cache', 'U') IS NOT NULL
    DROP TABLE dbo.shipment_summary_cache;

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
    style_match_indicator CHAR(1) NOT NULL DEFAULT 'U',      -- Y=Yes, N=No, P=Partial, U=Unknown
    color_match_indicator CHAR(1) NOT NULL DEFAULT 'U',      -- Y=Yes, N=No, P=Partial, U=Unknown
    delivery_match_indicator CHAR(1) NOT NULL DEFAULT 'U',   -- Y=Yes, N=No, P=Partial, U=Unknown
    quantity_match_indicator CHAR(1) NOT NULL DEFAULT 'U',   -- Y=Yes, N=No, P=Partial, U=Unknown
    
    -- Match metadata (pre-computed aggregations)
    match_count INT NOT NULL DEFAULT 0,                      -- Count of matching records
    match_layers NVARCHAR(50),                               -- Consolidated layer info (e.g., "LAYER_0-LAYER_1")
    best_confidence DECIMAL(5,2) DEFAULT 0.00,               -- Highest confidence score (0.00-1.00)
    avg_confidence DECIMAL(5,2) DEFAULT 0.00,                -- Average confidence across matches
    
    -- Quantity analysis (pre-computed)
    total_matched_order_qty INT DEFAULT 0,                   -- Sum of matched order quantities
    quantity_variance INT DEFAULT 0,                         -- Shipment qty - matched order qty
    
    -- Overall status classification (pre-computed business rules)
    shipment_status NVARCHAR(20) NOT NULL DEFAULT 'UNKNOWN', -- GOOD/QUANTITY_ISSUES/DELIVERY_ISSUES/UNMATCHED
    
    -- Outstanding reviews (placeholder for HITL integration)
    outstanding_reviews INT DEFAULT 0,
    
    -- Cache management
    last_updated DATETIME2 DEFAULT GETDATE(),
    source_last_modified DATETIME2,                          -- When source data last changed
    
    -- Constraints
    CONSTRAINT CK_style_match_indicator CHECK (style_match_indicator IN ('Y', 'N', 'P', 'U')),
    CONSTRAINT CK_color_match_indicator CHECK (color_match_indicator IN ('Y', 'N', 'P', 'U')),
    CONSTRAINT CK_delivery_match_indicator CHECK (delivery_match_indicator IN ('Y', 'N', 'P', 'U')),
    CONSTRAINT CK_quantity_match_indicator CHECK (quantity_match_indicator IN ('Y', 'N', 'P', 'U')),
    CONSTRAINT CK_shipment_status CHECK (shipment_status IN ('GOOD', 'QUANTITY_ISSUES', 'DELIVERY_ISSUES', 'UNMATCHED', 'UNKNOWN')),
    CONSTRAINT CK_confidence_range CHECK (best_confidence >= 0.00 AND best_confidence <= 1.00),
    CONSTRAINT CK_avg_confidence_range CHECK (avg_confidence >= 0.00 AND avg_confidence <= 1.00)
);

GO

-- Create performance indexes separately
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

GO

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
    
    -- Match indicator distribution
    SUM(CASE WHEN style_match_indicator = 'Y' THEN 1 ELSE 0 END) as style_match_yes,
    SUM(CASE WHEN color_match_indicator = 'Y' THEN 1 ELSE 0 END) as color_match_yes,
    SUM(CASE WHEN delivery_match_indicator = 'Y' THEN 1 ELSE 0 END) as delivery_match_yes,
    SUM(CASE WHEN quantity_match_indicator = 'Y' THEN 1 ELSE 0 END) as quantity_match_yes,
    
    -- Cache freshness
    MIN(last_updated) as oldest_cache_entry,
    MAX(last_updated) as newest_cache_entry,
    AVG(DATEDIFF(MINUTE, source_last_modified, last_updated)) as avg_cache_lag_minutes
    
FROM dbo.shipment_summary_cache;

GO

-- Grant permissions (adjust as needed for your security model)
-- GRANT SELECT ON dbo.shipment_summary_cache TO [YourStreamlitUser];
-- GRANT SELECT ON dbo.vw_shipment_summary_cache_stats TO [YourStreamlitUser];

PRINT 'Created shipment_summary_cache table with performance indexes';
PRINT 'Ready for stored procedure implementation in next step';

-- Quick validation query template (for testing)
/*
SELECT TOP 10
    row_number,
    shipment_id,
    customer_name,
    style_code,
    style_match_indicator,
    color_match_indicator,
    delivery_match_indicator,
    quantity_match_indicator,
    shipment_status,
    best_confidence,
    last_updated
FROM dbo.shipment_summary_cache
WHERE customer_name LIKE 'GREYSON%'
ORDER BY row_number;
*/
