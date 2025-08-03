-- 06_create_orders_extended_table.sql
-- Creates the int_orders_extended table for order data

-- Create the int_orders_extended table if it doesn't exist
IF OBJECT_ID('dbo.int_orders_extended', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[int_orders_extended] (
        [id] INT IDENTITY(1,1) PRIMARY KEY,
        [order_id] NVARCHAR(100) NOT NULL,
        [customer_name] NVARCHAR(255) NOT NULL,
        [po_number] NVARCHAR(100) NOT NULL,
        [style_code] NVARCHAR(100) NOT NULL,
        [color_code] NVARCHAR(100) NULL,
        [color_description] NVARCHAR(255) NULL,
        [style_color_key] NVARCHAR(255) NULL,
        [quantity] INT NOT NULL,
        [order_date] DATETIME NULL,
        [delivery_method] NVARCHAR(50) NULL,
        [created_at] DATETIME DEFAULT GETDATE(),
        [updated_at] DATETIME DEFAULT GETDATE(),
        [created_by] NVARCHAR(100) DEFAULT SYSTEM_USER
    );
    
    -- Create indexes for fast lookups
    CREATE INDEX [IX_int_orders_extended_order_id] ON [dbo].[int_orders_extended] ([order_id]);
    CREATE INDEX [IX_int_orders_extended_customer] ON [dbo].[int_orders_extended] ([customer_name]);
    CREATE INDEX [IX_int_orders_extended_po] ON [dbo].[int_orders_extended] ([po_number]);
    CREATE INDEX [IX_int_orders_extended_style_color] ON [dbo].[int_orders_extended] ([style_color_key]);
    
    PRINT 'Created int_orders_extended table';
END

PRINT 'Orders extended table is ready for use.';

GO

-- Create procedure to populate orders table from real production data
CREATE OR ALTER PROCEDURE [dbo].[sp_populate_orders_from_production]
    @customer_name NVARCHAR(255) = NULL,
    @po_number NVARCHAR(100) = NULL,
    @clear_existing BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Clear existing data if requested
    IF @clear_existing = 1
    BEGIN
        DELETE FROM [dbo].[int_orders_extended]
        WHERE (@customer_name IS NULL OR customer_name = @customer_name)
        AND (@po_number IS NULL OR po_number = @po_number);
        
        PRINT 'Cleared existing order data';
    END
    
    -- Populate from real production data via stg_order_list view
    INSERT INTO [dbo].[int_orders_extended] (
        [order_id],
        [customer_name], 
        [po_number],
        [style_code],
        [color_code],
        [color_description],
        [style_color_key],
        [quantity],
        [order_date],
        [delivery_method],
        [created_at],
        [updated_at]
    )
    SELECT 
        o.[order_id],
        o.[customer_name],
        o.[po_number],
        o.[style_code],
        ISNULL(o.[customer_color_code], '') as color_code,
        o.[color_description],
        CONCAT(o.[style_code], '-', ISNULL(o.[color_description], '')) as style_color_key,
        o.[quantity],
        o.[order_date],
        o.[delivery_method],
        GETDATE() as created_at,
        GETDATE() as updated_at
    FROM [dbo].[stg_order_list] o
    WHERE (@customer_name IS NULL OR o.[customer_name] = @customer_name)
    AND (@po_number IS NULL OR o.[po_number] = @po_number);
    
    DECLARE @inserted_count INT = @@ROWCOUNT;
    
    PRINT 'Populated ' + CAST(@inserted_count AS VARCHAR) + ' orders from production data';
    
    -- Return summary
    SELECT 
        @inserted_count as orders_inserted,
        @customer_name as customer_filter,
        @po_number as po_filter;
END

GO

PRINT 'Real production data population procedure created successfully.';
