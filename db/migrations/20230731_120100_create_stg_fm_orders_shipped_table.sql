-- stg_fm_orders_shipped_table.sql
-- Create a staging table for aggregated shipment data
-- This script creates a permanent table that will be populated with aggregated shipment data

-- Check if the table already exists and drop it if it does
IF OBJECT_ID('dbo.stg_fm_orders_shipped_table', 'U') IS NOT NULL
    DROP TABLE [dbo].[stg_fm_orders_shipped_table];

-- Create the staging table
CREATE TABLE [dbo].[stg_fm_orders_shipped_table] (
    [stg_shipment_id] INT IDENTITY(1,1) PRIMARY KEY,
    [representative_shipment_id] INT,           -- Original representative ID from source
    [customer_name] NVARCHAR(255),
    [po_number] NVARCHAR(100),
    [style_code] NVARCHAR(100),
    [color_description] NVARCHAR(255),
    [delivery_method] NVARCHAR(100),
    [shipped_date] DATETIME,
    [quantity] INT,
    [size_breakdown] NVARCHAR(MAX),             -- Concatenated string of size/qty pairs
    
    -- Reconciliation fields
    [reconciliation_status] NVARCHAR(20),
    [reconciliation_id] INT,
    [reconciliation_date] DATETIME,
    [split_shipment] BIT,
    [split_group_id] NVARCHAR(100),
    [parent_shipment_id] INT,
    
    -- Review tracking
    [last_reviewed_by] NVARCHAR(100),
    [last_reviewed_date] DATETIME,
    
    -- Matching fields (additional columns for reconciliation)
    [matched_order_id] NVARCHAR(100) NULL,      -- Reference to matched order
    [match_confidence] DECIMAL(5,2) NULL,       -- Confidence score of the match (0-100)
    [match_method] NVARCHAR(50) NULL,           -- How the match was made (exact, fuzzy, manual)
    [match_date] DATETIME NULL,                 -- When the match was made
    
    -- Audit fields
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    [last_sync_date] DATETIME NULL              -- When data was last synced from source
);

-- Create indexes for efficient querying
CREATE INDEX [IX_stg_fm_shipped_customer_po] ON [dbo].[stg_fm_orders_shipped_table] 
    ([customer_name], [po_number]);

CREATE INDEX [IX_stg_fm_shipped_style_color] ON [dbo].[stg_fm_orders_shipped_table] 
    ([style_code], [color_description]);

CREATE INDEX [IX_stg_fm_shipped_reconciliation] ON [dbo].[stg_fm_orders_shipped_table] 
    ([reconciliation_status]);

CREATE INDEX [IX_stg_fm_shipped_matched_order] ON [dbo].[stg_fm_orders_shipped_table] 
    ([matched_order_id]);

PRINT 'Created stg_fm_orders_shipped_table with all required fields and indexes.';
