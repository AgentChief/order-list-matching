-- FACT_Orders_Shipped Table
-- This table contains reconciled and standardized shipment data

CREATE TABLE [dbo].[FACT_Orders_Shipped] (
    [shipment_id] INT IDENTITY(1,1) PRIMARY KEY,
    [source_shipment_id] INT NOT NULL,  -- References original id in FM_orders_shipped
    
    -- Core shipment fields with standardized naming
    [customer_name] NVARCHAR(255) NOT NULL,
    [po_number] NVARCHAR(50) NOT NULL,
    [style_code] NVARCHAR(100) NOT NULL,
    [color_description] NVARCHAR(100) NOT NULL,
    [delivery_method] NVARCHAR(50) NULL,
    [shipped_date] DATE NOT NULL,
    [quantity] INT NOT NULL,
    [size] NVARCHAR(20) NOT NULL,
    
    -- Additional fields derived from reconciliation process
    [order_reference_id] INT NULL,  -- Reference to matched FACT_ORDER_LIST record
    
    -- Fields from canonical mapping not in original FM_orders_shipped
    [alias_related_item] NVARCHAR(255) NULL,
    [original_alias_related_item] NVARCHAR(255) NULL,
    [pattern_id] NVARCHAR(50) NULL,
    [customer_alt_po] NVARCHAR(100) NULL,
    [shipping_country] NVARCHAR(50) NULL,
    
    -- Reconciliation tracking fields
    [reconciliation_status] NVARCHAR(20) NULL, -- 'MATCHED', 'UNMATCHED', 'PENDING_REVIEW'
    [reconciliation_id] INT NULL, -- Reference to reconciliation_result.id
    [reconciliation_date] DATETIME NULL, -- When reconciliation was performed
    [split_shipment] BIT DEFAULT 0, -- Whether this is part of a split shipment
    [split_group_id] NVARCHAR(100) NULL, -- Group ID for related split shipments
    [parent_shipment_id] INT NULL, -- For hierarchical relationship in splits
    [last_reviewed_by] NVARCHAR(100) NULL, -- User who last reviewed this record
    [last_reviewed_date] DATETIME NULL, -- When the record was last reviewed
    
    -- Standard tracking fields
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX [IX_FACT_Orders_Shipped_source] ON [dbo].[FACT_Orders_Shipped] ([source_shipment_id]);
CREATE INDEX [IX_FACT_Orders_Shipped_customer_po] ON [dbo].[FACT_Orders_Shipped] ([customer_name], [po_number]);
CREATE INDEX [IX_FACT_Orders_Shipped_style_color] ON [dbo].[FACT_Orders_Shipped] ([style_code], [color_description]);
CREATE INDEX [IX_FACT_Orders_Shipped_shipped_date] ON [dbo].[FACT_Orders_Shipped] ([shipped_date]);
CREATE INDEX [IX_FACT_Orders_Shipped_reconciliation] ON [dbo].[FACT_Orders_Shipped] 
    ([reconciliation_status], [reconciliation_date]);
CREATE INDEX [IX_FACT_Orders_Shipped_split] ON [dbo].[FACT_Orders_Shipped] 
    ([split_group_id], [split_shipment]);
CREATE INDEX [IX_FACT_Orders_Shipped_order_reference] ON [dbo].[FACT_Orders_Shipped] 
    ([order_reference_id]);
