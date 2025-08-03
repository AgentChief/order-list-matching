-- reconciliation_result Table
-- Stores the results of reconciliation processes

CREATE TABLE [dbo].[reconciliation_result] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NOT NULL, -- Links to customer table
    [customer_name] NVARCHAR(255) NOT NULL, -- Canonical customer name
    [order_id] INT NULL, -- ID in ORDERS_UNIFIED table
    [shipment_id] INT NOT NULL, -- ID in FM_orders_shipped table
    [po_number] NVARCHAR(50) NOT NULL, -- PO number for reference
    [match_status] NVARCHAR(20) NOT NULL, -- 'matched', 'unmatched', 'uncertain'
    [confidence_score] FLOAT, -- Overall confidence score
    [match_method] NVARCHAR(50) NOT NULL, -- 'exact', 'fuzzy', 'recordlinkage', 'hitl'
    [match_details] NVARCHAR(MAX) NULL, -- JSON with detailed match information
    [is_split_shipment] BIT DEFAULT 0, -- Whether this is part of a split shipment
    [split_group_id] NVARCHAR(100) NULL, -- Identifier for split shipment group
    [reconciliation_date] DATETIME DEFAULT GETDATE(),
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX [IX_reconciliation_result_order] ON [dbo].[reconciliation_result] ([order_id]);
CREATE INDEX [IX_reconciliation_result_shipment] ON [dbo].[reconciliation_result] ([shipment_id]);
CREATE INDEX [IX_reconciliation_result_customer_po] ON [dbo].[reconciliation_result] 
    ([customer_name], [po_number], [match_status]);
