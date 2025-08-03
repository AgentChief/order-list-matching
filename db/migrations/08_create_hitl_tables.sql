-- HITL (Human-in-the-Loop) Support Tables
-- Tables to support value mappings and manual review decisions

-- Value Mappings Table
-- Stores mappings between variant field values (e.g., "SEA" -> "SEA FREIGHT")
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='value_mappings' AND xtype='U')
CREATE TABLE value_mappings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL,  -- NULL for global mappings
    field_name VARCHAR(100) NOT NULL,  -- e.g., 'delivery_method', 'color_description'
    source_value VARCHAR(500) NOT NULL,  -- Original value from data
    target_value VARCHAR(500) NOT NULL,  -- Standardized value to map to
    justification TEXT,  -- Business reason for the mapping
    status VARCHAR(20) DEFAULT 'active',  -- active, inactive, deprecated
    confidence_score DECIMAL(3,2) DEFAULT 1.0,  -- How confident we are in this mapping
    usage_count INT DEFAULT 0,  -- How many times this mapping has been applied
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by VARCHAR(100) DEFAULT 'system',
    updated_by VARCHAR(100) DEFAULT 'system',
    
    -- Foreign key constraint
    CONSTRAINT FK_value_mappings_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
    
    -- Unique constraint to prevent duplicate mappings
    CONSTRAINT UQ_value_mappings UNIQUE (customer_id, field_name, source_value, target_value),
    
    -- Check constraints
    CONSTRAINT CK_value_mappings_status CHECK (status IN ('active', 'inactive', 'deprecated')),
    CONSTRAINT CK_value_mappings_confidence CHECK (confidence_score BETWEEN 0.0 AND 1.0)
);
GO

-- HITL Decisions Table
-- Stores manual review decisions made by human reviewers
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hitl_decisions' AND xtype='U')
CREATE TABLE hitl_decisions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    match_type VARCHAR(50) NOT NULL,  -- 'delivery_method', 'quantity_tolerance', 'style_match', etc.
    shipment_id VARCHAR(100) NOT NULL,  -- Reference to shipment
    order_id VARCHAR(100) NOT NULL,  -- Reference to order
    decision VARCHAR(50) NOT NULL,  -- 'approve', 'reject', 'investigate', 'pending'
    justification TEXT,  -- Explanation for the decision
    original_score DECIMAL(5,4),  -- Original matching score if applicable
    reviewer_override DECIMAL(5,4),  -- Reviewer's assessment score
    reviewed_at DATETIME2 DEFAULT GETDATE(),
    expires_at DATETIME2,  -- When this decision expires (optional)
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by VARCHAR(100) DEFAULT 'system',
    updated_by VARCHAR(100) DEFAULT 'system',
    
    -- Check constraints
    CONSTRAINT CK_hitl_decisions_decision CHECK (decision IN ('approve', 'reject', 'investigate', 'pending', 'override')),
    CONSTRAINT CK_hitl_decisions_scores CHECK (
        (original_score IS NULL OR original_score BETWEEN 0.0 AND 1.0) AND
        (reviewer_override IS NULL OR reviewer_override BETWEEN 0.0 AND 1.0)
    )
);
GO

-- Mapping Usage Log Table
-- Track when and how value mappings are applied
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='mapping_usage_log' AND xtype='U')
CREATE TABLE mapping_usage_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    mapping_id INT NOT NULL,
    customer_name VARCHAR(255),
    po_number VARCHAR(100),
    field_name VARCHAR(100),
    source_value VARCHAR(500),
    target_value VARCHAR(500),
    applied_at DATETIME2 DEFAULT GETDATE(),
    applied_by VARCHAR(100) DEFAULT 'system',
    
    -- Foreign key constraint
    CONSTRAINT FK_mapping_usage_log_mapping FOREIGN KEY (mapping_id) REFERENCES value_mappings(id)
);
GO

-- Create indexes for performance
CREATE NONCLUSTERED INDEX IX_value_mappings_customer_field 
ON value_mappings (customer_id, field_name);

CREATE NONCLUSTERED INDEX IX_value_mappings_source_value 
ON value_mappings (field_name, source_value);

CREATE NONCLUSTERED INDEX IX_hitl_decisions_shipment_order 
ON hitl_decisions (shipment_id, order_id);

CREATE NONCLUSTERED INDEX IX_hitl_decisions_match_type 
ON hitl_decisions (match_type, decision);

CREATE NONCLUSTERED INDEX IX_mapping_usage_log_applied_at 
ON mapping_usage_log (applied_at DESC);
GO

-- Create stored procedure to apply value mappings
CREATE OR ALTER PROCEDURE sp_apply_value_mapping
    @customer_id INT = NULL,
    @field_name VARCHAR(100),
    @source_value VARCHAR(500),
    @applied_by VARCHAR(100) = 'system'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @mapping_id INT, @target_value VARCHAR(500);
    
    -- Find the most specific mapping (customer-specific first, then global)
    SELECT TOP 1 @mapping_id = id, @target_value = target_value
    FROM value_mappings
    WHERE field_name = @field_name 
      AND source_value = @source_value
      AND status = 'active'
      AND (customer_id = @customer_id OR customer_id IS NULL)
    ORDER BY 
        CASE WHEN customer_id = @customer_id THEN 1 ELSE 2 END,  -- Customer-specific first
        confidence_score DESC,  -- Then by confidence
        usage_count DESC;  -- Then by usage frequency
    
    IF @mapping_id IS NOT NULL
    BEGIN
        -- Update usage count
        UPDATE value_mappings 
        SET usage_count = usage_count + 1,
            updated_at = GETDATE()
        WHERE id = @mapping_id;
        
        -- Log the usage
        INSERT INTO mapping_usage_log (mapping_id, field_name, source_value, target_value, applied_by)
        VALUES (@mapping_id, @field_name, @source_value, @target_value, @applied_by);
        
        -- Return the mapped value
        SELECT @target_value as mapped_value, @mapping_id as mapping_id;
    END
    ELSE
    BEGIN
        -- No mapping found, return original value
        SELECT @source_value as mapped_value, NULL as mapping_id;
    END
END;
GO

-- Create view for HITL dashboard summary
CREATE OR ALTER VIEW v_hitl_summary AS
SELECT 
    match_type,
    COUNT(*) as total_decisions,
    SUM(CASE WHEN decision = 'approve' THEN 1 ELSE 0 END) as approved,
    SUM(CASE WHEN decision = 'reject' THEN 1 ELSE 0 END) as rejected,
    SUM(CASE WHEN decision = 'investigate' THEN 1 ELSE 0 END) as investigating,
    SUM(CASE WHEN decision = 'pending' THEN 1 ELSE 0 END) as pending,
    AVG(CASE WHEN original_score IS NOT NULL THEN original_score END) as avg_original_score,
    AVG(CASE WHEN reviewer_override IS NOT NULL THEN reviewer_override END) as avg_reviewer_score,
    MIN(created_at) as first_decision,
    MAX(created_at) as last_decision
FROM hitl_decisions
GROUP BY match_type;
GO

-- Create view for value mapping effectiveness
CREATE OR ALTER VIEW v_mapping_effectiveness AS
SELECT 
    vm.field_name,
    vm.source_value,
    vm.target_value,
    vm.confidence_score,
    vm.usage_count,
    vm.created_at,
    vm.created_by,
    COUNT(mul.id) as times_applied,
    MAX(mul.applied_at) as last_applied
FROM value_mappings vm
LEFT JOIN mapping_usage_log mul ON vm.id = mul.mapping_id
WHERE vm.status = 'active'
GROUP BY vm.id, vm.field_name, vm.source_value, vm.target_value, 
         vm.confidence_score, vm.usage_count, vm.created_at, vm.created_by;
GO

PRINT 'HITL support tables created successfully!';
