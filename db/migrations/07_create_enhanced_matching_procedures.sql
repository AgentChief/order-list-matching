-- 07_create_enhanced_matching_procedures.sql
-- Enhanced matching procedures with business-realistic matching logic
-- Phase 1: Style matching (mandatory), color confidence scoring, delivery method mapping, quantity tolerance ±5%

-- Create enhanced batch reconciliation procedure
CREATE OR ALTER PROCEDURE [dbo].[sp_enhanced_batch_reconcile]
    @customer_name NVARCHAR(255),
    @po_number NVARCHAR(100) = NULL,
    @quantity_tolerance_percent DECIMAL(5,2) = 5.0,
    @style_match_required BIT = 1,
    @color_confidence_threshold DECIMAL(5,2) = 85.0,
    @batch_description NVARCHAR(MAX) = 'Enhanced matching batch'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @batch_id INT;
    DECLARE @total_shipments INT = 0;
    DECLARE @exact_matches INT = 0;
    DECLARE @style_matches INT = 0;
    DECLARE @hitl_reviews INT = 0;
    DECLARE @manual_reviews INT = 0;
    
    -- Create batch record
    INSERT INTO [dbo].[reconciliation_batch] (
        [name], 
        [description], 
        [status], 
        [created_at], 
        [created_by]
    ) VALUES (
        'Enhanced_' + @customer_name + ISNULL('_PO' + @po_number, '') + '_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss'),
        @batch_description,
        'PROCESSING',
        GETDATE(),
        SYSTEM_USER
    );
    
    SET @batch_id = SCOPE_IDENTITY();
    
    -- Create temp table for enhanced matching results
    CREATE TABLE #enhanced_matches (
        shipment_id INT,
        order_id INT,
        match_confidence DECIMAL(5,2),
        match_method NVARCHAR(50),
        match_notes NVARCHAR(MAX),
        style_match_score DECIMAL(5,2),
        color_match_score DECIMAL(5,2),
        quantity_match_score DECIMAL(5,2),
        delivery_method_score DECIMAL(5,2),
        recommendation NVARCHAR(20)
    );
    
    -- Enhanced matching algorithm
    INSERT INTO #enhanced_matches (
        shipment_id, order_id, match_confidence, match_method, match_notes,
        style_match_score, color_match_score, quantity_match_score, delivery_method_score,
        recommendation
    )
    SELECT 
        s.source_shipment_id,
        o.id as order_id,
        -- Overall confidence calculation
        CASE 
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description 
                AND ABS(s.quantity - o.quantity) <= (o.quantity * @quantity_tolerance_percent / 100.0)
                THEN 100.0  -- EXACT_MATCH
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description
                THEN 95.0   -- STYLE_COLOR_MATCH with quantity variance
            WHEN s.style_code = o.style_code
                THEN 75.0   -- STYLE_MATCH only
            ELSE 0.0        -- NO_MATCH
        END as match_confidence,
        
        -- Match method determination
        CASE 
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description 
                AND ABS(s.quantity - o.quantity) <= (o.quantity * @quantity_tolerance_percent / 100.0)
                THEN 'EXACT_MATCH'
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description
                THEN 'STYLE_COLOR_MATCH'
            WHEN s.style_code = o.style_code
                THEN 'STYLE_MATCH'
            ELSE 'NO_MATCH'
        END as match_method,
        
        -- Detailed match notes
        CASE 
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description 
                AND ABS(s.quantity - o.quantity) <= (o.quantity * @quantity_tolerance_percent / 100.0)
                THEN 'Perfect match: Style=' + s.style_code + ', Color=' + s.color_description + ', Qty=' + CAST(s.quantity AS VARCHAR) + '/' + CAST(o.quantity AS VARCHAR)
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description
                THEN 'Style/Color match with quantity variance: ' + CAST(s.quantity AS VARCHAR) + '/' + CAST(o.quantity AS VARCHAR) + ' (' + CAST(ROUND(ABS(s.quantity - o.quantity) * 100.0 / o.quantity, 1) AS VARCHAR) + '% diff)'
            WHEN s.style_code = o.style_code
                THEN 'Style match only: Style=' + s.style_code + ', Ship Color=' + ISNULL(s.color_description, 'NULL') + ', Order Color=' + ISNULL(o.color_description, 'NULL')
            ELSE 'No viable match'
        END as match_notes,
        
        -- Individual scoring components
        CASE WHEN s.style_code = o.style_code THEN 100.0 ELSE 0.0 END as style_match_score,
        CASE WHEN s.color_description = o.color_description THEN 100.0 ELSE 0.0 END as color_match_score,
        CASE 
            WHEN ABS(s.quantity - o.quantity) <= (o.quantity * @quantity_tolerance_percent / 100.0) THEN 100.0
            ELSE GREATEST(0.0, 100.0 - (ABS(s.quantity - o.quantity) * 100.0 / o.quantity))
        END as quantity_match_score,
        
        -- Delivery method scoring (simplified for now)
        CASE 
            WHEN s.delivery_method IN ('AIR', 'SEA', 'SEA-FB', 'DHL') THEN 100.0
            ELSE 50.0
        END as delivery_method_score,
        
        -- Recommendation logic
        CASE 
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description 
                AND ABS(s.quantity - o.quantity) <= (o.quantity * @quantity_tolerance_percent / 100.0)
                THEN 'AUTO_MATCH'
            WHEN s.style_code = o.style_code AND s.color_description = o.color_description
                THEN 'HITL_REVIEW'
            WHEN s.style_code = o.style_code
                THEN 'HITL_REVIEW'
            ELSE 'MANUAL_REVIEW'
        END as recommendation
        
    FROM [dbo].[stg_fm_orders_shipped_table] s
    CROSS JOIN [dbo].[int_orders_extended] o
    WHERE s.customer_name = @customer_name
        AND (@po_number IS NULL OR s.po_number = @po_number)
        AND (@style_match_required = 0 OR s.style_code = o.style_code)  -- Enforce style matching if required
        AND o.customer_name = @customer_name
        AND (@po_number IS NULL OR o.po_number = @po_number);
    
    -- Count results by category
    SELECT @total_shipments = COUNT(DISTINCT shipment_id) FROM #enhanced_matches;
    SELECT @exact_matches = COUNT(*) FROM #enhanced_matches WHERE recommendation = 'AUTO_MATCH';
    SELECT @hitl_reviews = COUNT(*) FROM #enhanced_matches WHERE recommendation = 'HITL_REVIEW';
    SELECT @manual_reviews = COUNT(*) FROM #enhanced_matches WHERE recommendation = 'MANUAL_REVIEW';
    
    -- Insert results into reconciliation_result table
    INSERT INTO [dbo].[reconciliation_result] (
        [batch_id],
        [shipment_id],
        [order_id],
        [match_confidence],
        [match_method],
        [match_notes],
        [manual_review_required],
        [created_at],
        [created_by]
    )
    SELECT 
        @batch_id,
        shipment_id,
        order_id,
        match_confidence,
        match_method,
        match_notes + ' | Scores: Style=' + CAST(style_match_score AS VARCHAR) + 
            ', Color=' + CAST(color_match_score AS VARCHAR) + 
            ', Qty=' + CAST(quantity_match_score AS VARCHAR) + 
            ', Delivery=' + CAST(delivery_method_score AS VARCHAR) +
            ' | Recommendation=' + recommendation,
        CASE WHEN recommendation IN ('HITL_REVIEW', 'MANUAL_REVIEW') THEN 1 ELSE 0 END,
        GETDATE(),
        SYSTEM_USER
    FROM #enhanced_matches
    WHERE match_confidence > 0;  -- Only insert viable matches
    
    -- Update batch status with summary
    UPDATE [dbo].[reconciliation_batch]
    SET 
        [status] = 'COMPLETED',
        [completed_at] = GETDATE(),
        [description] = @batch_description + 
            ' | Results: ' + CAST(@total_shipments AS VARCHAR) + ' shipments, ' +
            CAST(@exact_matches AS VARCHAR) + ' exact, ' +
            CAST(@hitl_reviews AS VARCHAR) + ' HITL, ' +
            CAST(@manual_reviews AS VARCHAR) + ' manual'
    WHERE [id] = @batch_id;
    
    -- Return summary
    SELECT 
        @batch_id as batch_id,
        @total_shipments as total_shipments,
        @exact_matches as exact_matches,
        @hitl_reviews as hitl_reviews,
        @manual_reviews as manual_reviews,
        CAST(@exact_matches * 100.0 / NULLIF(@total_shipments, 0) AS DECIMAL(5,1)) as auto_match_rate;
    
    -- Clean up
    DROP TABLE #enhanced_matches;
END

GO

-- Create delivery method mapping procedure
CREATE OR ALTER PROCEDURE [dbo].[sp_map_delivery_methods]
    @batch_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create or update delivery method mapping table
    IF OBJECT_ID('dbo.delivery_method_mapping', 'U') IS NULL
    BEGIN
        CREATE TABLE [dbo].[delivery_method_mapping] (
            [id] INT IDENTITY(1,1) PRIMARY KEY,
            [shipment_method] NVARCHAR(50) NOT NULL,
            [order_method] NVARCHAR(50) NULL,
            [mapping_confidence] DECIMAL(5,2) DEFAULT 100.0,
            [mapping_notes] NVARCHAR(MAX),
            [created_at] DATETIME DEFAULT GETDATE(),
            [updated_at] DATETIME DEFAULT GETDATE()
        );
        
        -- Insert default mappings
        INSERT INTO [dbo].[delivery_method_mapping] (shipment_method, order_method, mapping_confidence, mapping_notes)
        VALUES 
            ('AIR', 'AIR', 100.0, 'Direct air freight mapping'),
            ('SEA', 'SEA', 100.0, 'Direct sea freight mapping'),
            ('SEA-FB', 'SEA', 85.0, 'Sea freight with fulfillment - maps to SEA with lower confidence'),
            ('DHL', 'EXPRESS', 90.0, 'DHL express service mapping'),
            ('DHL', 'AIR', 75.0, 'DHL can also map to AIR with lower confidence');
        
        PRINT 'Created delivery method mapping table with default mappings';
    END
    
    -- Analyze delivery methods in current batch or all data
    SELECT 
        s.delivery_method as shipment_method,
        COUNT(*) as usage_count,
        COUNT(DISTINCT s.customer_name) as customer_count,
        AVG(CAST(s.quantity AS DECIMAL(10,2))) as avg_quantity,
        STRING_AGG(s.customer_name, ', ') WITHIN GROUP (ORDER BY s.customer_name) as customers
    FROM [dbo].[stg_fm_orders_shipped_table] s
    LEFT JOIN [dbo].[reconciliation_result] r ON s.source_shipment_id = r.shipment_id
    WHERE @batch_id IS NULL OR r.batch_id = @batch_id
    GROUP BY s.delivery_method
    ORDER BY usage_count DESC;
END

GO

PRINT 'Enhanced matching procedures created successfully';
PRINT 'Available procedures:';
PRINT '  - sp_enhanced_batch_reconcile: Main enhanced matching procedure';
PRINT '  - sp_map_delivery_methods: Delivery method analysis and mapping';
