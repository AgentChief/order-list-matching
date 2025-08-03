-- 05_create_reconciliation_tables.sql
-- Creates tables for tracking reconciliation batches and results

-- Create reconciliation batch table if it doesn't exist
IF OBJECT_ID('dbo.reconciliation_batch', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[reconciliation_batch] (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX) NULL,
        [start_time] DATETIME NOT NULL,
        [end_time] DATETIME NULL,
        [status] NVARCHAR(20) DEFAULT 'RUNNING', -- RUNNING, COMPLETED, ERROR
        [matched_count] INT DEFAULT 0,
        [unmatched_count] INT DEFAULT 0,
        [fuzzy_threshold] INT DEFAULT 85,
        [error_message] NVARCHAR(MAX) NULL,
        [created_at] DATETIME DEFAULT GETDATE(),
        [updated_at] DATETIME DEFAULT GETDATE(),
        [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER
    );
    
    PRINT 'Created reconciliation_batch table';
END

-- Create reconciliation result table if it doesn't exist
IF OBJECT_ID('dbo.reconciliation_result', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[reconciliation_result] (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [batch_id] INT NOT NULL,
        [shipment_id] INT NOT NULL,
        [order_id] NVARCHAR(100) NULL,
        [match_status] NVARCHAR(20) NOT NULL, -- 'matched', 'unmatched', 'review'
        [match_confidence] DECIMAL(5,2) NULL,
        [match_method] NVARCHAR(50) NULL,
        [match_notes] NVARCHAR(MAX) NULL,
        [created_at] DATETIME DEFAULT GETDATE(),
        [updated_at] DATETIME DEFAULT GETDATE(),
        [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER,
        
        CONSTRAINT [FK_reconciliation_result_batch] FOREIGN KEY ([batch_id]) 
            REFERENCES [dbo].[reconciliation_batch]([id])
    );
    
    CREATE INDEX [IX_reconciliation_result_shipment] ON [dbo].[reconciliation_result] ([shipment_id]);
    CREATE INDEX [IX_reconciliation_result_order] ON [dbo].[reconciliation_result] ([order_id]);
    CREATE INDEX [IX_reconciliation_result_batch] ON [dbo].[reconciliation_result] ([batch_id]);
    
    PRINT 'Created reconciliation_result table';
END

PRINT 'Reconciliation tables are ready for use.';
