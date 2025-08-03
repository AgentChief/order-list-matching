-- customer_attribute_config Table
-- Stores customer-specific configurations for attribute matching

CREATE TABLE [dbo].[customer_attribute_config] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NOT NULL, -- Links to customer table
    [customer_name] NVARCHAR(255) NOT NULL, -- Canonical customer name
    [attribute_name] NVARCHAR(100) NOT NULL, -- e.g., 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE'
    [weight] FLOAT DEFAULT 1.0, -- Weight of this attribute in matching algorithm (0.0-10.0)
    [comparison_method] NVARCHAR(50) NOT NULL DEFAULT 'exact', -- 'exact', 'string_similarity', 'numeric', etc.
    [threshold] FLOAT DEFAULT 0.9, -- Threshold for fuzzy matching (0.0-1.0)
    [requires_exact_match] BIT DEFAULT 0, -- Whether this attribute must match exactly regardless of overall score
    [is_active] BIT DEFAULT 1, -- Whether this configuration is active
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER,
    [updated_by] NVARCHAR(100) DEFAULT SYSTEM_USER
);

-- Unique constraint to prevent duplicate configurations
CREATE UNIQUE INDEX [UX_customer_attribute_config] ON [dbo].[customer_attribute_config] 
    ([customer_id], [attribute_name]) 
    WHERE [is_active] = 1;

-- Indexes for performance
CREATE INDEX [IX_customer_attribute_config_lookup] ON [dbo].[customer_attribute_config] 
    ([customer_name], [is_active]);
