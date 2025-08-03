-- reconciliation_job Table
-- Tracks reconciliation job runs

CREATE TABLE [dbo].[reconciliation_job] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [job_name] NVARCHAR(255) NOT NULL, -- Descriptive name for the job
    [customer_id] INT NULL, -- Links to customer table (NULL if multi-customer job)
    [customer_name] NVARCHAR(255) NULL, -- Canonical customer name (NULL if multi-customer job)
    [po_number] NVARCHAR(50) NULL, -- PO number if specific to a PO
    [job_parameters] NVARCHAR(MAX) NULL, -- JSON with job parameters
    [status] NVARCHAR(20) NOT NULL, -- 'queued', 'running', 'completed', 'failed'
    [start_time] DATETIME NULL, -- When job started
    [end_time] DATETIME NULL, -- When job completed or failed
    [total_records] INT DEFAULT 0, -- Total records processed
    [matched_count] INT DEFAULT 0, -- Number of matched records
    [unmatched_count] INT DEFAULT 0, -- Number of unmatched records
    [uncertain_count] INT DEFAULT 0, -- Number of uncertain matches
    [error_message] NVARCHAR(MAX) NULL, -- Error message if job failed
    [created_by] NVARCHAR(100) NULL, -- User who created the job
    [created_at] DATETIME DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX [IX_reconciliation_job_status] ON [dbo].[reconciliation_job] ([status]);
CREATE INDEX [IX_reconciliation_job_customer] ON [dbo].[reconciliation_job] ([customer_name]);
CREATE INDEX [IX_reconciliation_job_time] ON [dbo].[reconciliation_job] ([start_time], [end_time]);
