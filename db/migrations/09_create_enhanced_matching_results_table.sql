-- Enhanced Matching Results Storage Table
-- Stores results from enhanced matcher for HITL interface consumption

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='enhanced_matching_results' AND xtype='U')
CREATE TABLE enhanced_matching_results (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    po_number VARCHAR(100) NOT NULL,
    shipment_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    
    -- Match status
    match_layer VARCHAR(20) NOT NULL,  -- 'LAYER_0', 'LAYER_1', 'UNMATCHED'
    match_confidence DECIMAL(5,4),     -- Overall match confidence score
    
    -- Field match details
    style_match VARCHAR(10) NOT NULL,         -- 'MATCH', 'MISMATCH'
    color_match VARCHAR(10) NOT NULL,         -- 'MATCH', 'MISMATCH'  
    delivery_match VARCHAR(10) NOT NULL,      -- 'MATCH', 'MISMATCH'
    
    -- Field values for comparison
    shipment_style_code VARCHAR(255),
    order_style_code VARCHAR(255),
    shipment_color_description VARCHAR(255),
    order_color_description VARCHAR(255),
    shipment_delivery_method VARCHAR(255),
    order_delivery_method VARCHAR(255),
    
    -- Quantity information
    shipment_quantity INT,
    order_quantity INT,
    quantity_difference_percent DECIMAL(7,2),
    quantity_check_result VARCHAR(10),  -- 'PASS', 'FAIL'
    
    -- Metadata
    matching_session_id VARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE(),
    created_by VARCHAR(100) DEFAULT 'enhanced_matcher',
    
    -- Indexes
    INDEX IX_enhanced_matching_results_customer_po (customer_name, po_number),
    INDEX IX_enhanced_matching_results_delivery_mismatch (delivery_match, created_at),
    INDEX IX_enhanced_matching_results_session (matching_session_id)
);

PRINT 'Enhanced matching results table created successfully!';
