-- match_attribute_score Table
-- Stores individual attribute comparison scores from recordlinkage process

CREATE TABLE [dbo].[match_attribute_score] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [reconciliation_id] INT NOT NULL, -- Links to reconciliation_result
    [attribute_name] NVARCHAR(100) NOT NULL, -- Name of compared attribute (e.g., 'color', 'style')
    [order_value] NVARCHAR(255) NULL, -- Value from order record
    [shipment_value] NVARCHAR(255) NULL, -- Value from shipment record
    [match_score] FLOAT NOT NULL, -- Individual attribute match score (0.0-1.0)
    [match_method] NVARCHAR(50) NOT NULL, -- 'exact', 'fuzzy', 'alias'
    [is_key_attribute] BIT NOT NULL DEFAULT 0, -- Whether this attribute was used as a key in matching
    [weight] FLOAT NOT NULL DEFAULT 1.0, -- Weight given to this attribute in overall score
    [created_at] DATETIME DEFAULT GETDATE(),
    CONSTRAINT [FK_match_attribute_score_reconciliation] FOREIGN KEY ([reconciliation_id]) 
        REFERENCES [dbo].[reconciliation_result] ([id]) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX [IX_match_attribute_score_reconciliation] ON [dbo].[match_attribute_score] ([reconciliation_id]);
CREATE INDEX [IX_match_attribute_score_attribute] ON [dbo].[match_attribute_score] ([attribute_name]);
