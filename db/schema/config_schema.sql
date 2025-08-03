-- Configuration Database Schema
-- Replaces YAML-based configuration with database-driven approach
-- Supports global defaults with customer-specific overrides

-- Core customer information
CREATE TABLE customers (
    id INT IDENTITY(1,1) PRIMARY KEY,
    canonical_name NVARCHAR(100) NOT NULL UNIQUE,
    status NVARCHAR(20) NOT NULL CHECK (status IN ('approved', 'review', 'deprecated')),
    packed_products NVARCHAR(100),
    shipped NVARCHAR(100),
    master_order_list NVARCHAR(100),
    mon_customer_ms NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER
);

-- Customer name aliases and variations
CREATE TABLE customer_aliases (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    alias_name NVARCHAR(100) NOT NULL,
    is_primary BIT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    UNIQUE(customer_id, alias_name)
);

-- Column mappings from Order to Shipment fields
-- NULL customer_id = global mapping
CREATE TABLE column_mappings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL, -- NULL = global mapping
    order_column NVARCHAR(100) NOT NULL,
    shipment_column NVARCHAR(100) NOT NULL,
    is_active BIT DEFAULT 1,
    priority INT DEFAULT 1, -- Lower number = higher priority
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    INDEX IX_column_mappings_customer_active (customer_id, is_active),
    INDEX IX_column_mappings_order_column (order_column)
);

-- Matching strategy configuration
-- NULL customer_id = global strategy
CREATE TABLE matching_strategies (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL, -- NULL = global strategy
    strategy_name NVARCHAR(100) NOT NULL,
    primary_match_fields NVARCHAR(500), -- JSON array: fields that must match exactly
    secondary_match_fields NVARCHAR(500), -- JSON array: fields allowing fuzzy matching
    fuzzy_threshold DECIMAL(3,2) DEFAULT 0.85,
    quantity_tolerance DECIMAL(3,2) DEFAULT 0.05,
    confidence_high DECIMAL(3,2) DEFAULT 0.90,
    confidence_medium DECIMAL(3,2) DEFAULT 0.70,
    confidence_low DECIMAL(3,2) DEFAULT 0.50,
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    INDEX IX_matching_strategies_customer_active (customer_id, is_active)
);

-- Exclusion rules (e.g., exclude ORDER TYPE = 'CANCELLED')
-- NULL customer_id = global rule
CREATE TABLE exclusion_rules (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL, -- NULL = global rule
    table_name NVARCHAR(100) NOT NULL, -- 'orders' or 'shipments'
    field_name NVARCHAR(100) NOT NULL,
    exclude_values NVARCHAR(1000), -- JSON array of values to exclude
    rule_type NVARCHAR(20) DEFAULT 'exclude' CHECK (rule_type IN ('exclude', 'include')),
    description NVARCHAR(500),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    INDEX IX_exclusion_rules_customer_table_field (customer_id, table_name, field_name)
);

-- Data quality unique key definitions
-- For duplicate detection, not matching logic
CREATE TABLE data_quality_keys (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL, -- NULL = global keys
    table_name NVARCHAR(100) NOT NULL, -- 'orders' or 'shipments'
    key_type NVARCHAR(20) NOT NULL CHECK (key_type IN ('unique_keys', 'extra_checks')),
    field_names NVARCHAR(500), -- JSON array of field names
    description NVARCHAR(500),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    INDEX IX_data_quality_keys_customer_table (customer_id, table_name)
);

-- Value mappings for canonicalization
-- E.g., "SEA-FB" -> "FAST BOAT", "476 - WOLF BLUE" -> "WOLF BLUE"
CREATE TABLE value_mappings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NULL, -- NULL = global mapping
    field_name NVARCHAR(100) NOT NULL,
    source_value NVARCHAR(200) NOT NULL,
    canonical_value NVARCHAR(200) NOT NULL,
    mapping_type NVARCHAR(50) DEFAULT 'exact', -- exact, fuzzy, regex
    confidence DECIMAL(3,2) DEFAULT 1.0,
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    updated_by NVARCHAR(100) DEFAULT SYSTEM_USER,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    INDEX IX_value_mappings_field_source (field_name, source_value),
    INDEX IX_value_mappings_customer_field (customer_id, field_name)
);

-- Configuration change audit trail
CREATE TABLE configuration_audit (
    id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(100) NOT NULL,
    record_id INT NOT NULL,
    action NVARCHAR(20) NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_values NVARCHAR(MAX), -- JSON
    new_values NVARCHAR(MAX), -- JSON
    changed_by NVARCHAR(100) NOT NULL,
    changed_at DATETIME2 DEFAULT GETDATE(),
    change_reason NVARCHAR(500),
    INDEX IX_configuration_audit_table_record (table_name, record_id),
    INDEX IX_configuration_audit_changed_at (changed_at)
);

-- Insert global default configurations
-- Global column mappings
INSERT INTO column_mappings (customer_id, order_column, shipment_column, priority) VALUES
(NULL, 'PO NUMBER', 'Customer_PO', 1),
(NULL, 'PLANNED DELIVERY METHOD', 'Shipping_Method', 1),
(NULL, 'CUSTOMER STYLE', 'Style', 1),
(NULL, 'CUSTOMER COLOUR DESCRIPTION', 'Color', 1),
(NULL, 'SIZE', 'Size', 1);

-- Global matching strategy
INSERT INTO matching_strategies (
    customer_id, 
    strategy_name, 
    primary_match_fields, 
    secondary_match_fields,
    fuzzy_threshold,
    quantity_tolerance,
    confidence_high,
    confidence_medium,
    confidence_low
) VALUES (
    NULL, 
    'Global Default Strategy',
    '["Style", "Color", "Customer_PO"]',
    '["Shipping_Method", "Size"]',
    0.85,
    0.05,
    0.90,
    0.70,
    0.50
);

-- Global exclusion rule for cancelled orders
INSERT INTO exclusion_rules (
    customer_id,
    table_name,
    field_name,
    exclude_values,
    description
) VALUES (
    NULL,
    'orders',
    'order_type',
    '["CANCELLED"]',
    'Exclude cancelled orders from matching process'
);

-- Global data quality keys for orders
INSERT INTO data_quality_keys (
    customer_id,
    table_name,
    key_type,
    field_names,
    description
) VALUES 
(NULL, 'orders', 'unique_keys', '["aag_order_number", "delivery_method", "style_code"]', 'Global order duplicate detection'),
(NULL, 'orders', 'extra_checks', '["po_number", "order_type"]', 'Global order validation fields'),
(NULL, 'shipments', 'unique_keys', '["Customer_PO", "Shipping_Method", "Style", "Color", "Size"]', 'Global shipment duplicate detection'),
(NULL, 'shipments', 'extra_checks', '["shippingCountry"]', 'Global shipment validation fields');

-- Create views for easy configuration access
GO

CREATE VIEW vw_customer_configurations AS
SELECT 
    c.id as customer_id,
    c.canonical_name,
    c.status,
    ISNULL(cm.column_mappings, '[]') as column_mappings,
    ISNULL(ms.matching_strategy, '{}') as matching_strategy,
    ISNULL(er.exclusion_rules, '[]') as exclusion_rules,
    ISNULL(dk.data_quality_keys, '[]') as data_quality_keys,
    ISNULL(vm.value_mappings, '[]') as value_mappings
FROM customers c
LEFT JOIN (
    SELECT 
        customer_id,
        (SELECT order_column, shipment_column, priority 
         FROM column_mappings cm2 
         WHERE cm2.customer_id = cm.customer_id AND cm2.is_active = 1
         FOR JSON PATH) as column_mappings
    FROM column_mappings cm
    WHERE cm.is_active = 1
    GROUP BY customer_id
) cm ON c.id = cm.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        (SELECT TOP 1 strategy_name, primary_match_fields, secondary_match_fields, 
                fuzzy_threshold, quantity_tolerance, confidence_high, confidence_medium, confidence_low
         FROM matching_strategies ms2 
         WHERE ms2.customer_id = ms.customer_id AND ms2.is_active = 1
         ORDER BY ms2.id DESC
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as matching_strategy
    FROM matching_strategies ms
    WHERE ms.is_active = 1
    GROUP BY customer_id
) ms ON c.id = ms.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        (SELECT table_name, field_name, exclude_values, rule_type, description
         FROM exclusion_rules er2 
         WHERE er2.customer_id = er.customer_id AND er2.is_active = 1
         FOR JSON PATH) as exclusion_rules
    FROM exclusion_rules er
    WHERE er.is_active = 1
    GROUP BY customer_id
) er ON c.id = er.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        (SELECT table_name, key_type, field_names, description
         FROM data_quality_keys dk2 
         WHERE dk2.customer_id = dk.customer_id AND dk2.is_active = 1
         FOR JSON PATH) as data_quality_keys
    FROM data_quality_keys dk
    WHERE dk.is_active = 1
    GROUP BY customer_id
) dk ON c.id = dk.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        (SELECT field_name, source_value, canonical_value, mapping_type, confidence
         FROM value_mappings vm2 
         WHERE vm2.customer_id = vm.customer_id AND vm2.is_active = 1
         FOR JSON PATH) as value_mappings
    FROM value_mappings vm
    WHERE vm.is_active = 1
    GROUP BY customer_id
) vm ON c.id = vm.customer_id;

GO

-- Create stored procedures for configuration management
CREATE PROCEDURE sp_get_customer_config
    @customer_name NVARCHAR(100)
AS
BEGIN
    -- Get customer-specific config with global fallbacks
    SELECT 
        c.canonical_name,
        c.status,
        -- Column mappings with fallback to global
        COALESCE(
            (SELECT order_column, shipment_column, priority 
             FROM column_mappings 
             WHERE customer_id = c.id AND is_active = 1
             FOR JSON PATH),
            (SELECT order_column, shipment_column, priority 
             FROM column_mappings 
             WHERE customer_id IS NULL AND is_active = 1
             FOR JSON PATH)
        ) as column_mappings,
        -- Matching strategy with fallback to global
        COALESCE(
            (SELECT TOP 1 strategy_name, primary_match_fields, secondary_match_fields, 
                    fuzzy_threshold, quantity_tolerance, confidence_high, confidence_medium, confidence_low
             FROM matching_strategies 
             WHERE customer_id = c.id AND is_active = 1
             ORDER BY id DESC
             FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            (SELECT TOP 1 strategy_name, primary_match_fields, secondary_match_fields, 
                    fuzzy_threshold, quantity_tolerance, confidence_high, confidence_medium, confidence_low
             FROM matching_strategies 
             WHERE customer_id IS NULL AND is_active = 1
             ORDER BY id DESC
             FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        ) as matching_strategy,
        -- Exclusion rules (combine customer + global)
        (SELECT table_name, field_name, exclude_values, rule_type, description
         FROM exclusion_rules 
         WHERE (customer_id = c.id OR customer_id IS NULL) AND is_active = 1
         FOR JSON PATH) as exclusion_rules
    FROM customers c
    WHERE c.canonical_name = @customer_name
       OR c.id IN (SELECT customer_id FROM customer_aliases WHERE alias_name = @customer_name);
END;
