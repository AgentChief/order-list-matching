-- alias_review_queue Table
-- Queue of uncertain matches for HITL review

CREATE TABLE [dbo].[alias_review_queue] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [customer_id] INT NOT NULL, -- Links to customer table
    [customer_name] NVARCHAR(255) NOT NULL, -- Canonical customer name
    [order_id] INT NULL, -- ID in ORDERS_UNIFIED table (NULL if no match found)
    [shipment_id] INT NULL, -- ID in FM_orders_shipped table (NULL if reviewing attribute only)
    [attribute_name] NVARCHAR(100) NOT NULL, -- Attribute being reviewed
    [order_value] NVARCHAR(255) NULL, -- Value from order (NULL if reviewing attribute only)
    [shipment_value] NVARCHAR(255) NULL, -- Value from shipment (NULL if reviewing attribute only)
    [suggested_canonical] NVARCHAR(255) NULL, -- System-suggested canonical value
    [confidence_score] FLOAT, -- Confidence score from algorithm
    [status] NVARCHAR(20) DEFAULT 'pending', -- 'pending', 'approved', 'rejected', 'modified'
    [reviewer_canonical] NVARCHAR(255) NULL, -- Human-provided canonical value (if modified)
    [reviewed_at] DATETIME NULL, -- When this was reviewed
    [reviewed_by] NVARCHAR(100) NULL, -- Who reviewed this
    [notes] NVARCHAR(MAX) NULL, -- Optional notes from reviewer
    [created_at] DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX [IX_alias_review_queue_status] ON [dbo].[alias_review_queue] ([status]);
CREATE INDEX [IX_alias_review_queue_customer] ON [dbo].[alias_review_queue] ([customer_name], [status]);
