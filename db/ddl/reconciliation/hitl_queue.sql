-- hitl_queue Table
-- Manages queue for human-in-the-loop review of uncertain matches

CREATE TABLE [dbo].[hitl_queue] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [reconciliation_id] INT NOT NULL, -- Links to reconciliation_result
    [priority] INT DEFAULT 5, -- Priority (1-10, 10 being highest)
    [status] NVARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'in_review', 'reviewed'
    [assigned_to] NVARCHAR(100) NULL, -- User assigned for review
    [review_notes] NVARCHAR(MAX) NULL, -- Notes from reviewer
    [review_decision] NVARCHAR(20) NULL, -- 'approve', 'reject', 'need_more_info'
    [decision_reason] NVARCHAR(MAX) NULL, -- Reason for decision
    [review_started_at] DATETIME NULL, -- When review started
    [review_completed_at] DATETIME NULL, -- When review completed
    [created_at] DATETIME DEFAULT GETDATE(),
    [updated_at] DATETIME DEFAULT GETDATE(),
    CONSTRAINT [FK_hitl_queue_reconciliation] FOREIGN KEY ([reconciliation_id]) 
        REFERENCES [dbo].[reconciliation_result] ([id]) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX [IX_hitl_queue_status] ON [dbo].[hitl_queue] ([status], [priority]);
CREATE INDEX [IX_hitl_queue_assigned] ON [dbo].[hitl_queue] ([assigned_to], [status]);
