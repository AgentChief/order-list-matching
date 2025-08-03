-- attribute_equivalence_rule Table
-- Stores rules for field comparisons and transformations

CREATE TABLE [dbo].[attribute_equivalence_rule] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [rule_type] NVARCHAR(50) NOT NULL, -- 'transformation', 'pattern', 'lookup', etc.
    [attribute_name] NVARCHAR(100) NOT NULL, -- e.g., 'PLANNED DELIVERY METHOD', 'CUSTOMER STYLE'
    [rule_definition] NVARCHAR(MAX) NOT NULL, -- JSON or rule pattern
    [description] NVARCHAR(255), -- Human-readable description
    [is_active] BIT DEFAULT 1, -- Whether this rule is active
    [priority] INT DEFAULT 100, -- Processing order (lower numbers processed first)
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER,
    [updated_by] NVARCHAR(100) DEFAULT SYSTEM_USER
);

-- Indexes for performance
CREATE INDEX [IX_attribute_equivalence_rule_lookup] ON [dbo].[attribute_equivalence_rule] 
    ([attribute_name], [is_active], [priority]);
