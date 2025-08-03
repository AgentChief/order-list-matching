-- FM_orders_shipped Table
-- This table contains all shipments from FileMaker shipping system

CREATE TABLE [dbo].[FM_orders_shipped] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [Customer] NVARCHAR(255) NOT NULL,
    [Customer_PO] NVARCHAR(50) NOT NULL,
    [Style] NVARCHAR(100),
    [Color] NVARCHAR(100),
    [Shipping_Method] NVARCHAR(50),
    [Shipped_Date] DATE,
    [Qty] INT,
    [Size] NVARCHAR(20),
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX [IX_FM_orders_shipped_CUSTOMER_PO] ON [dbo].[FM_orders_shipped] ([Customer], [Customer_PO]);
CREATE INDEX [IX_FM_orders_shipped_STYLE_COLOR] ON [dbo].[FM_orders_shipped] ([Style], [Color]);
CREATE INDEX [IX_FM_orders_shipped_SHIPPED_DATE] ON [dbo].[FM_orders_shipped] ([Shipped_Date]);


-- FM_orders_shipped Table Modifications
-- Add fields for reconciliation status tracking

-- First, add new columns to FM_orders_shipped
ALTER TABLE [dbo].[FM_orders_shipped] ADD
    [reconciliation_status] NVARCHAR(20) NULL, -- 'MATCHED', 'UNMATCHED', 'PENDING_REVIEW'
    [reconciliation_id] INT NULL, -- Reference to reconciliation_result.id
    [reconciliation_date] DATETIME NULL, -- When reconciliation was performed
    [split_shipment] BIT DEFAULT 0, -- Whether this is part of a split shipment
    [split_group_id] NVARCHAR(100) NULL, -- Group ID for related split shipments
    [parent_shipment_id] INT NULL, -- For hierarchical relationship in splits
    [last_reviewed_by] NVARCHAR(100) NULL, -- User who last reviewed this record
    [last_reviewed_date] DATETIME NULL; -- When the record was last reviewed

-- Create index for efficient querying by reconciliation status
CREATE INDEX [IX_FM_orders_shipped_reconciliation] ON [dbo].[FM_orders_shipped] 
    ([reconciliation_status], [reconciliation_date]);

-- Create index for efficient querying of split shipments
CREATE INDEX [IX_FM_orders_shipped_split] ON [dbo].[FM_orders_shipped] 
    ([split_group_id], [split_shipment]);