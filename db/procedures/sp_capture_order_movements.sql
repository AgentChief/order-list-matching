-- =====================================================================================
-- TASK013: Order Movement Capture Procedures
-- =====================================================================================
-- Purpose: Capture and track order lifecycle events in fact_order_movements table
-- Features: Event-driven capture, split shipment support, reconciliation integration

-- Procedure to capture order placement events
CREATE OR ALTER PROCEDURE sp_capture_order_placed
    @order_id NVARCHAR(100),
    @customer_name NVARCHAR(100),
    @po_number NVARCHAR(100),
    @style_code NVARCHAR(50),
    @color_description NVARCHAR(100) = NULL,
    @size_code NVARCHAR(20) = NULL,
    @order_quantity INT,
    @unit_price DECIMAL(10,2) = NULL,
    @delivery_method NVARCHAR(50) = NULL,
    @order_type NVARCHAR(20) = 'STANDARD',
    @expected_ship_date DATE = NULL,
    @batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Generate match group ID for this order
    DECLARE @match_group_id NVARCHAR(50) = CONCAT(@customer_name, '_', @po_number, '_', @order_id);
    
    -- Insert order placement movement
    INSERT INTO dbo.fact_order_movements (
        order_id, customer_name, po_number, style_code, color_description, size_code,
        movement_type, movement_date, movement_status,
        order_quantity, unit_price, delivery_method, order_type,
        expected_ship_date, match_group_id, reconciliation_status,
        extended_price, batch_id, source_system
    )
    VALUES (
        @order_id, @customer_name, @po_number, @style_code, @color_description, @size_code,
        'ORDER_PLACED', GETDATE(), 'COMPLETED',
        @order_quantity, @unit_price, @delivery_method, @order_type,
        @expected_ship_date, @match_group_id, 'PENDING',
        @order_quantity * ISNULL(@unit_price, 0), @batch_id, 'ORDER_SYSTEM'
    );
    
    SELECT SCOPE_IDENTITY() as movement_id, @match_group_id as match_group_id;
END;

GO

-- Procedure to capture shipment creation events
CREATE OR ALTER PROCEDURE sp_capture_shipment_created
    @shipment_id INT,
    @customer_name NVARCHAR(100),
    @po_number NVARCHAR(100),
    @style_code NVARCHAR(50),
    @color_description NVARCHAR(100) = NULL,
    @size_code NVARCHAR(20) = NULL,
    @shipped_quantity INT,
    @delivery_method NVARCHAR(50) = NULL,
    @tracking_number NVARCHAR(100) = NULL,
    @split_group_id NVARCHAR(50) = NULL,
    @is_primary_shipment BIT = 0,
    @batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Generate match group ID for shipment matching
    DECLARE @match_group_id NVARCHAR(50) = CONCAT(@customer_name, '_', @po_number, '_SHIP_', @shipment_id);
    
    -- Insert shipment creation movement
    INSERT INTO dbo.fact_order_movements (
        shipment_id, customer_name, po_number, style_code, color_description, size_code,
        movement_type, movement_date, movement_status,
        shipped_quantity, delivery_method, tracking_number,
        split_group_id, is_primary_shipment, match_group_id,
        reconciliation_status, batch_id, source_system
    )
    VALUES (
        @shipment_id, @customer_name, @po_number, @style_code, @color_description, @size_code,
        'SHIPMENT_CREATED', GETDATE(), 'COMPLETED',
        @shipped_quantity, @delivery_method, @tracking_number,
        @split_group_id, @is_primary_shipment, @match_group_id,
        'PENDING', @batch_id, 'WMS_SYSTEM'
    );
    
    SELECT SCOPE_IDENTITY() as movement_id, @match_group_id as match_group_id;
END;

GO

-- Procedure to capture shipment shipped events
CREATE OR ALTER PROCEDURE sp_capture_shipment_shipped
    @shipment_id INT,
    @shipped_date DATETIME2 = NULL,
    @tracking_number NVARCHAR(100) = NULL,
    @batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SET @shipped_date = ISNULL(@shipped_date, GETDATE());
    
    -- Get existing shipment movement
    DECLARE @existing_movement_id BIGINT;
    DECLARE @customer_name NVARCHAR(100);
    DECLARE @po_number NVARCHAR(100);
    DECLARE @style_code NVARCHAR(50);
    DECLARE @color_description NVARCHAR(100);
    DECLARE @size_code NVARCHAR(20);
    DECLARE @shipped_quantity INT;
    DECLARE @delivery_method NVARCHAR(50);
    DECLARE @split_group_id NVARCHAR(50);
    DECLARE @is_primary_shipment BIT;
    DECLARE @match_group_id NVARCHAR(50);
    
    SELECT TOP 1
        @existing_movement_id = movement_id,
        @customer_name = customer_name,
        @po_number = po_number,
        @style_code = style_code,
        @color_description = color_description,
        @size_code = size_code,
        @shipped_quantity = shipped_quantity,
        @delivery_method = delivery_method,
        @split_group_id = split_group_id,
        @is_primary_shipment = is_primary_shipment,
        @match_group_id = match_group_id
    FROM dbo.fact_order_movements
    WHERE shipment_id = @shipment_id 
        AND movement_type = 'SHIPMENT_CREATED'
    ORDER BY movement_id DESC;
    
    IF @existing_movement_id IS NOT NULL
    BEGIN
        -- Insert shipment shipped movement
        INSERT INTO dbo.fact_order_movements (
            shipment_id, customer_name, po_number, style_code, color_description, size_code,
            movement_type, movement_date, movement_status,
            shipped_quantity, shipped_date, delivery_method, tracking_number,
            split_group_id, is_primary_shipment, match_group_id,
            reconciliation_status, parent_movement_id, batch_id, source_system
        )
        VALUES (
            @shipment_id, @customer_name, @po_number, @style_code, @color_description, @size_code,
            'SHIPMENT_SHIPPED', @shipped_date, 'COMPLETED',
            @shipped_quantity, @shipped_date, @delivery_method, @tracking_number,
            @split_group_id, @is_primary_shipment, @match_group_id,
            'PENDING', @existing_movement_id, @batch_id, 'WMS_SYSTEM'
        );
        
        SELECT SCOPE_IDENTITY() as movement_id, @match_group_id as match_group_id;
    END
    ELSE
    BEGIN
        RAISERROR('Shipment %d not found in movement table', 16, 1, @shipment_id);
    END
END;

GO

-- Procedure to capture reconciliation events
CREATE OR ALTER PROCEDURE sp_capture_reconciliation_event
    @order_id NVARCHAR(100) = NULL,
    @shipment_id INT = NULL,
    @match_group_id NVARCHAR(50),
    @reconciliation_status NVARCHAR(20),
    @reconciliation_confidence DECIMAL(5,2) = NULL,
    @reconciliation_method NVARCHAR(50) = NULL,
    @quantity_variance INT = NULL,
    @batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update existing movements with reconciliation status
    IF @order_id IS NOT NULL
    BEGIN
        UPDATE dbo.fact_order_movements
        SET reconciliation_status = @reconciliation_status,
            reconciliation_confidence = @reconciliation_confidence,
            reconciliation_method = @reconciliation_method,
            reconciliation_date = GETDATE(),
            updated_at = GETDATE()
        WHERE order_id = @order_id AND match_group_id = @match_group_id;
    END
    
    IF @shipment_id IS NOT NULL
    BEGIN
        UPDATE dbo.fact_order_movements
        SET reconciliation_status = @reconciliation_status,
            reconciliation_confidence = @reconciliation_confidence,
            reconciliation_method = @reconciliation_method,
            reconciliation_date = GETDATE(),
            quantity_variance = @quantity_variance,
            quantity_variance_percent = CASE 
                WHEN order_quantity > 0 THEN (@quantity_variance * 100.0 / order_quantity)
                ELSE NULL
            END,
            updated_at = GETDATE()
        WHERE shipment_id = @shipment_id AND match_group_id = @match_group_id;
    END
    
    -- Create a dedicated reconciliation movement event
    DECLARE @customer_name NVARCHAR(100);
    DECLARE @po_number NVARCHAR(100);
    
    SELECT TOP 1 @customer_name = customer_name, @po_number = po_number
    FROM dbo.fact_order_movements
    WHERE match_group_id = @match_group_id;
    
    INSERT INTO dbo.fact_order_movements (
        order_id, shipment_id, customer_name, po_number,
        movement_type, movement_date, movement_status,
        match_group_id, reconciliation_status, reconciliation_confidence,
        reconciliation_method, quantity_variance, batch_id, source_system
    )
    VALUES (
        @order_id, @shipment_id, @customer_name, @po_number,
        'RECONCILED', GETDATE(), 'COMPLETED',
        @match_group_id, @reconciliation_status, @reconciliation_confidence,
        @reconciliation_method, @quantity_variance, @batch_id, 'RECONCILIATION_ENGINE'
    );
    
    SELECT SCOPE_IDENTITY() as movement_id;
END;

GO

-- Procedure to populate movement table from existing data
CREATE OR ALTER PROCEDURE sp_populate_movement_table_from_existing
    @customer_filter NVARCHAR(100) = NULL,
    @batch_size INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @batch_id INT;
    INSERT INTO dbo.reconciliation_batch (name, description, start_time, status)
    VALUES ('MOVEMENT_POPULATION', 'Initial population of movement table from existing data', GETDATE(), 'RUNNING');
    SET @batch_id = SCOPE_IDENTITY();
    
    BEGIN TRY
        -- Populate orders from FACT_ORDER_LIST
        INSERT INTO dbo.fact_order_movements (
            order_id, customer_name, po_number, style_code, color_description, size_code,
            movement_type, movement_date, movement_status,
            order_quantity, unit_price, delivery_method, order_type,
            match_group_id, reconciliation_status, extended_price,
            batch_id, source_system, created_at
        )
        SELECT 
            CAST(fol.id AS NVARCHAR(100)) as order_id,
            fol.customer_name,
            fol.po_number,
            fol.style_code,
            fol.color_description,
            fol.size_code,
            'ORDER_PLACED',
            ISNULL(fol.order_date, '2023-01-01') as movement_date,
            'COMPLETED',
            fol.quantity,
            fol.unit_price,
            fol.delivery_method,
            ISNULL(fol.order_type, 'STANDARD'),
            CONCAT(fol.customer_name, '_', fol.po_number, '_', fol.id) as match_group_id,
            'PENDING',
            fol.quantity * ISNULL(fol.unit_price, 0),
            @batch_id,
            'MIGRATION_ORDER_SYSTEM',
            GETDATE()
        FROM FACT_ORDER_LIST fol
        WHERE (@customer_filter IS NULL OR fol.customer_name LIKE @customer_filter + '%');
        
        DECLARE @order_count INT = @@ROWCOUNT;
        
        -- Populate shipments from FM_orders_shipped
        INSERT INTO dbo.fact_order_movements (
            shipment_id, customer_name, po_number, style_code, color_description, size_code,
            movement_type, movement_date, movement_status,
            shipped_quantity, shipped_date, delivery_method, tracking_number,
            match_group_id, reconciliation_status,
            batch_id, source_system, created_at
        )
        SELECT 
            fmos.shipment_id,
            fmos.Customer as customer_name,
            fmos.Customer_PO as po_number,
            fmos.Style as style_code,
            fmos.Color as color_description,
            fmos.Size as size_code,
            'SHIPMENT_SHIPPED',
            ISNULL(fmos.Shipped_Date, '2023-01-01') as movement_date,
            'COMPLETED',
            fmos.Quantity as shipped_quantity,
            fmos.Shipped_Date,
            fmos.Shipping_Method as delivery_method,
            fmos.Tracking_Number,
            CONCAT(fmos.Customer, '_', fmos.Customer_PO, '_SHIP_', fmos.shipment_id) as match_group_id,
            'PENDING',
            @batch_id,
            'MIGRATION_WMS_SYSTEM',
            GETDATE()
        FROM FM_orders_shipped fmos
        WHERE (@customer_filter IS NULL OR fmos.Customer LIKE @customer_filter + '%');
        
        DECLARE @shipment_count INT = @@ROWCOUNT;
        
        -- Populate reconciliation events from existing results
        INSERT INTO dbo.fact_order_movements (
            order_id, shipment_id, customer_name, po_number,
            movement_type, movement_date, movement_status,
            match_group_id, reconciliation_status, reconciliation_confidence,
            reconciliation_method, batch_id, source_system, created_at
        )
        SELECT 
            rr.order_id,
            CAST(rr.shipment_id AS INT),
            'MIGRATION_CUSTOMER' as customer_name,
            'MIGRATION_PO' as po_number,
            'RECONCILED',
            ISNULL(rr.created_at, GETDATE()),
            'COMPLETED',
            ISNULL(rr.match_group_id, CONCAT('MIGRATION_', rr.id)) as match_group_id,
            rr.match_status,
            rr.match_confidence,
            rr.match_method,
            @batch_id,
            'MIGRATION_RECONCILIATION_ENGINE',
            GETDATE()
        FROM dbo.reconciliation_result rr
        WHERE rr.match_status IN ('matched', 'unmatched', 'review');
        
        DECLARE @reconciliation_count INT = @@ROWCOUNT;
        
        -- Update batch status
        UPDATE dbo.reconciliation_batch
        SET end_time = GETDATE(),
            status = 'COMPLETED',
            matched_count = @order_count + @shipment_count + @reconciliation_count,
            updated_at = GETDATE()
        WHERE id = @batch_id;
        
        -- Return summary
        SELECT 
            @batch_id as batch_id,
            @order_count as orders_migrated,
            @shipment_count as shipments_migrated,
            @reconciliation_count as reconciliation_events_migrated,
            @order_count + @shipment_count + @reconciliation_count as total_movements_created;
            
    END TRY
    BEGIN CATCH
        -- Update batch status on error
        UPDATE dbo.reconciliation_batch
        SET end_time = GETDATE(),
            status = 'ERROR',
            error_message = ERROR_MESSAGE(),
            updated_at = GETDATE()
        WHERE id = @batch_id;
        
        -- Re-raise the error
        THROW;
    END CATCH
END;

GO

-- Procedure to update cumulative quantities
CREATE OR ALTER PROCEDURE sp_update_cumulative_quantities
    @customer_filter NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update cumulative shipped quantities and remaining quantities
    WITH OrderShipments AS (
        SELECT 
            order_id,
            customer_name,
            po_number,
            style_code,
            color_description,
            SUM(CASE WHEN movement_type = 'SHIPMENT_SHIPPED' THEN shipped_quantity ELSE 0 END) as total_shipped,
            MAX(CASE WHEN movement_type = 'ORDER_PLACED' THEN order_quantity ELSE 0 END) as order_qty
        FROM dbo.fact_order_movements
        WHERE (@customer_filter IS NULL OR customer_name LIKE @customer_filter + '%')
            AND order_id IS NOT NULL
        GROUP BY order_id, customer_name, po_number, style_code, color_description
    )
    UPDATE fm
    SET cumulative_shipped_quantity = os.total_shipped,
        remaining_quantity = os.order_qty - os.total_shipped,
        updated_at = GETDATE()
    FROM dbo.fact_order_movements fm
    INNER JOIN OrderShipments os ON fm.order_id = os.order_id 
        AND fm.customer_name = os.customer_name
        AND fm.po_number = os.po_number
        AND fm.style_code = os.style_code
        AND ISNULL(fm.color_description, '') = ISNULL(os.color_description, '');
    
    SELECT @@ROWCOUNT as updated_records;
END;

GO

PRINT 'Created movement capture procedures:';
PRINT '- sp_capture_order_placed';
PRINT '- sp_capture_shipment_created'; 
PRINT '- sp_capture_shipment_shipped';
PRINT '- sp_capture_reconciliation_event';
PRINT '- sp_populate_movement_table_from_existing';
PRINT '- sp_update_cumulative_quantities';