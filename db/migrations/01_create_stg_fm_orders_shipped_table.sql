-- 01_create_stg_fm_orders_shipped_table.sql
-- Creates a staging table for shipment data at the aggregated level (no size level)

-- Check if the table already exists and drop it if it does
IF OBJECT_ID('dbo.stg_fm_orders_shipped_table', 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping existing stg_fm_orders_shipped_table';
    DROP TABLE [dbo].[stg_fm_orders_shipped_table];
END

-- Create the staging table
CREATE TABLE [dbo].[stg_fm_orders_shipped_table] (
    [shipment_id] INT IDENTITY(1,1) PRIMARY KEY,               -- Primary key for this table
    [source_shipment_id] INT,                                  -- Original ID from source (MIN(id) from FM_orders_shipped)
    [customer_name] NVARCHAR(255),                             -- Customer name
    [po_number] NVARCHAR(100),                                 -- Purchase order number
    [style_code] NVARCHAR(100),                                -- Style code
    [color_description] NVARCHAR(255),                         -- Color description
    [delivery_method] NVARCHAR(100),                           -- Shipping/delivery method
    [shipped_date] DATETIME,                                   -- Date shipped
    [quantity] INT,                                            -- Total quantity (sum of all sizes)
    [size_breakdown] NVARCHAR(MAX),                            -- Concatenated string of size/qty pairs
    
    -- Business keys for matching
    [style_color_key] AS (CONCAT([style_code], '-', [color_description])) PERSISTED, -- Composite key for style-color matching
    [customer_po_key] AS (CONCAT([customer_name], '-', [po_number])) PERSISTED,      -- Composite key for customer-PO matching
    
    -- Reconciliation fields
    [reconciliation_status] NVARCHAR(20) NULL,                 -- 'MATCHED', 'UNMATCHED', 'PENDING_REVIEW'
    [reconciliation_id] INT NULL,                              -- Reference to reconciliation batch ID
    [reconciliation_date] DATETIME NULL,                       -- When reconciliation was performed
    [split_shipment] BIT DEFAULT 0,                            -- Whether this is part of a split shipment
    [split_group_id] NVARCHAR(100) NULL,                       -- Group ID for related split shipments
    [parent_shipment_id] INT NULL,                             -- For hierarchical relationship in splits
    
    -- Matching fields
    [matched_order_id] NVARCHAR(100) NULL,                     -- Reference to matched order record_uuid
    [match_confidence] DECIMAL(5,2) NULL,                      -- Confidence score of the match (0-100)
    [match_method] NVARCHAR(50) NULL,                          -- How the match was made (exact, fuzzy, manual)
    [match_notes] NVARCHAR(MAX) NULL,                          -- Additional notes about the match
    
    -- Review tracking
    [last_reviewed_by] NVARCHAR(100) NULL,                     -- User who last reviewed this record
    [last_reviewed_date] DATETIME NULL,                        -- When the record was last reviewed
    
    -- Audit fields
    [created_at] DATETIME DEFAULT GETDATE(),                   -- When the record was created
    [updated_at] DATETIME DEFAULT GETDATE(),                   -- When the record was last updated
    [last_sync_date] DATETIME DEFAULT GETDATE()                -- When data was last synced from source
);

-- Create indexes for efficient querying
CREATE INDEX [IX_stg_fm_shipped_customer_po] ON [dbo].[stg_fm_orders_shipped_table] ([customer_name], [po_number]);
CREATE INDEX [IX_stg_fm_shipped_style_color] ON [dbo].[stg_fm_orders_shipped_table] ([style_code], [color_description]);
CREATE INDEX [IX_stg_fm_shipped_style_color_key] ON [dbo].[stg_fm_orders_shipped_table] ([style_color_key]);
CREATE INDEX [IX_stg_fm_shipped_customer_po_key] ON [dbo].[stg_fm_orders_shipped_table] ([customer_po_key]);
CREATE INDEX [IX_stg_fm_shipped_reconciliation] ON [dbo].[stg_fm_orders_shipped_table] ([reconciliation_status]);
CREATE INDEX [IX_stg_fm_shipped_matched_order] ON [dbo].[stg_fm_orders_shipped_table] ([matched_order_id]);

PRINT 'Created stg_fm_orders_shipped_table with all required fields and indexes.';
