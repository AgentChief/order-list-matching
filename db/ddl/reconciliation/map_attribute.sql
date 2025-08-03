-- map_attribute Table
-- Stores canonical mappings for attribute values between systems

CREATE TABLE [dbo].[map_attribute] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NULL, -- NULL means global mapping
    [attribute_name] NVARCHAR(100) NOT NULL, -- e.g., 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE'
    [source_value] NVARCHAR(255) NOT NULL, -- Original value from source system
    [canonical_value] NVARCHAR(255) NOT NULL, -- Standardized value
    [source_system] NVARCHAR(50) NOT NULL, -- 'ORDERS' or 'SHIPMENTS'
    [confidence] FLOAT DEFAULT 1.0, -- Confidence level of this mapping (0.0-1.0)
    [is_active] BIT DEFAULT 1, -- Whether this mapping is active
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER,
    [updated_by] NVARCHAR(100) DEFAULT SYSTEM_USER,
    [note] NVARCHAR(MAX) NULL -- Optional note about this mapping
);

-- Unique constraint to prevent duplicate mappings
CREATE UNIQUE INDEX [UX_map_attribute] ON [dbo].[map_attribute] 
    ([customer_id], [attribute_name], [source_value], [source_system]) 
    WHERE [is_active] = 1;

-- Indexes for performance
CREATE INDEX [IX_map_attribute_lookup] ON [dbo].[map_attribute] 
    ([attribute_name], [source_value], [source_system], [is_active]);
