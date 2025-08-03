-- customer_match_config Table
-- Stores customer-specific matching configuration settings

CREATE TABLE [dbo].[customer_match_config] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NOT NULL, -- Links to customer table
    [customer_name] NVARCHAR(255) NOT NULL, -- Canonical customer name
    [config_type] NVARCHAR(50) NOT NULL, -- Type of config ('threshold', 'attribute_weight', etc.)
    [config_key] NVARCHAR(100) NOT NULL, -- Configuration parameter name
    [config_value] NVARCHAR(MAX) NOT NULL, -- Configuration value (can be complex, stored as JSON if needed)
    [is_active] BIT NOT NULL DEFAULT 1, -- Whether this config is currently active
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    [created_by] NVARCHAR(100) NULL, -- User who created this config
    [updated_by] NVARCHAR(100) NULL, -- User who last updated this config
    CONSTRAINT [UQ_customer_config] UNIQUE ([customer_name], [config_type], [config_key])
);

-- Sample config entries:
/*
INSERT INTO [dbo].[customer_match_config]
    ([customer_name], [config_type], [config_key], [config_value])
VALUES
    ('GREYSON', 'threshold', 'exact_match', '1.0'),
    ('GREYSON', 'threshold', 'fuzzy_match', '0.85'),
    ('GREYSON', 'threshold', 'uncertain_match', '0.7'),
    ('GREYSON', 'attribute_weight', 'style', '3.0'),
    ('GREYSON', 'attribute_weight', 'color', '2.0'),
    ('GREYSON', 'attribute_weight', 'size', '1.0');
*/

-- Indexes for performance
CREATE INDEX [IX_customer_match_config_customer] ON [dbo].[customer_match_config] ([customer_name], [is_active]);
CREATE INDEX [IX_customer_match_config_type] ON [dbo].[customer_match_config] ([config_type], [is_active]);
