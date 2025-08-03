-- alias_review_history Table
-- History of past HITL decisions

CREATE TABLE [dbo].[alias_review_history] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [review_queue_id] INT NOT NULL, -- Reference to the review queue entry
    [customer_id] INT NOT NULL, -- Links to customer table
    [customer_name] NVARCHAR(255) NOT NULL, -- Canonical customer name
    [attribute_name] NVARCHAR(100) NOT NULL, -- Attribute that was reviewed
    [original_value] NVARCHAR(255) NOT NULL, -- Original value before review
    [canonical_value] NVARCHAR(255) NOT NULL, -- Approved canonical value
    [decision] NVARCHAR(20) NOT NULL, -- 'approved', 'rejected', 'modified'
    [decision_reason] NVARCHAR(MAX) NULL, -- Optional reason for decision
    [applied_to_mapping] BIT DEFAULT 0, -- Whether this was applied to mapping table
    [applied_at] DATETIME NULL, -- When this was applied to mapping table
    [created_at] DATETIME DEFAULT GETDATE(),
    [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER
);

-- Indexes for performance
CREATE INDEX [IX_alias_review_history_review_id] ON [dbo].[alias_review_history] ([review_queue_id]);
CREATE INDEX [IX_alias_review_history_attribute] ON [dbo].[alias_review_history] 
    ([customer_name], [attribute_name], [original_value]);
