-- add_fields_to_fm_orders_shipped.sql
-- Script to add missing columns to the FM_orders_shipped table

-- First, check if the primary key column exists and add it if it doesn't
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.FM_orders_shipped') AND name = 'id')
BEGIN
    -- Add an identity column as primary key
    ALTER TABLE [dbo].[FM_orders_shipped] ADD
        [id] INT IDENTITY(1,1);
    
    -- Make it the primary key
    ALTER TABLE [dbo].[FM_orders_shipped] ADD CONSTRAINT PK_FM_orders_shipped PRIMARY KEY (id);
    
    PRINT 'Added id column as primary key';
END

-- Add reconciliation tracking fields if they don't exist
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.FM_orders_shipped') AND name = 'reconciliation_status')
BEGIN
    ALTER TABLE [dbo].[FM_orders_shipped] ADD
        [reconciliation_status] NVARCHAR(20) NULL,           -- 'MATCHED', 'UNMATCHED', 'PENDING_REVIEW'
        [reconciliation_id] INT NULL,                         -- Reference to reconciliation_result.id
        [reconciliation_date] DATETIME NULL,                  -- When reconciliation was performed
        [split_shipment] BIT DEFAULT 0,                       -- Whether this is part of a split shipment
        [split_group_id] NVARCHAR(100) NULL,                  -- Group ID for related split shipments
        [parent_shipment_id] INT NULL,                        -- For hierarchical relationship in splits
        [last_reviewed_by] NVARCHAR(100) NULL,                -- User who last reviewed this record
        [last_reviewed_date] DATETIME NULL;                   -- When the record was last reviewed
    
    PRINT 'Added reconciliation tracking fields';
END

-- Add audit fields if they don't exist
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('dbo.FM_orders_shipped') AND name = 'created_at')
BEGIN
    ALTER TABLE [dbo].[FM_orders_shipped] ADD
        [created_at] DATETIME DEFAULT GETDATE(),             -- When the record was created
        [updated_at] DATETIME DEFAULT GETDATE();             -- When the record was last updated
    
    PRINT 'Added audit fields';
END

-- Create index for efficient querying by reconciliation status if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FM_orders_shipped_reconciliation' AND object_id = OBJECT_ID('dbo.FM_orders_shipped'))
BEGIN
    CREATE INDEX [IX_FM_orders_shipped_reconciliation] ON [dbo].[FM_orders_shipped] 
        ([reconciliation_status], [reconciliation_date]);
    
    PRINT 'Created reconciliation index';
END

-- Create index for efficient querying of split shipments if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FM_orders_shipped_split' AND object_id = OBJECT_ID('dbo.FM_orders_shipped'))
BEGIN
    CREATE INDEX [IX_FM_orders_shipped_split] ON [dbo].[FM_orders_shipped] 
        ([split_group_id], [split_shipment]);
    
    PRINT 'Created split shipment index';
END

PRINT 'Column addition complete. FM_orders_shipped table now has all required fields.';
