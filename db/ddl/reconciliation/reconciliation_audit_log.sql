-- reconciliation_audit_log Table
-- Tracks all changes to reconciliation data

CREATE TABLE [dbo].[reconciliation_audit_log] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [entity_type] NVARCHAR(50) NOT NULL, -- 'reconciliation_result', 'alias_review', etc.
    [entity_id] INT NOT NULL, -- ID of the entity being audited
    [action] NVARCHAR(20) NOT NULL, -- 'create', 'update', 'delete', 'review', 'approve', 'reject'
    [field_name] NVARCHAR(100) NULL, -- Field that was changed (if applicable)
    [old_value] NVARCHAR(MAX) NULL, -- Previous value (if applicable)
    [new_value] NVARCHAR(MAX) NULL, -- New value (if applicable)
    [reason] NVARCHAR(MAX) NULL, -- Reason for change (if applicable)
    [user_id] NVARCHAR(100) NULL, -- User who made the change
    [timestamp] DATETIME DEFAULT GETDATE() -- When the change occurred
);

-- Indexes for performance
CREATE INDEX [IX_reconciliation_audit_log_entity] ON [dbo].[reconciliation_audit_log] ([entity_type], [entity_id]);
CREATE INDEX [IX_reconciliation_audit_log_user] ON [dbo].[reconciliation_audit_log] ([user_id]);
CREATE INDEX [IX_reconciliation_audit_log_time] ON [dbo].[reconciliation_audit_log] ([timestamp]);
