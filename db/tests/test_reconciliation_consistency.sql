-- test_reconciliation_consistency.sql
-- Test to validate reconciliation data consistency

CREATE OR ALTER PROCEDURE [dbo].[test_reconciliation_consistency]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Test 1: Check for shipments with reconciliation_status = 'MATCHED' but no order_reference_id
    SELECT
        'Inconsistent match status' AS test_name,
        COUNT(*) AS failure_count,
        'Shipments marked as MATCHED but missing order_reference_id' AS description
    FROM
        [dbo].[FACT_Orders_Shipped]
    WHERE
        reconciliation_status = 'MATCHED'
        AND order_reference_id IS NULL;
    
    -- Test 2: Check for orders referenced by multiple shipments (may be valid for split shipments)
    SELECT
        'Multiple shipments per order' AS test_name,
        order_reference_id,
        COUNT(*) AS shipment_count,
        'Order referenced by multiple shipments' AS description
    FROM
        [dbo].[FACT_Orders_Shipped]
    WHERE
        order_reference_id IS NOT NULL
    GROUP BY
        order_reference_id
    HAVING
        COUNT(*) > 1;
    
    -- Test 3: Check for quantity mismatches between orders and shipments
    SELECT
        'Quantity mismatch' AS test_name,
        o.order_id,
        s.shipment_id,
        o.quantity AS order_quantity,
        s.quantity AS shipment_quantity,
        ABS(o.quantity - s.quantity) AS quantity_difference,
        'Order and shipment quantities do not match' AS description
    FROM
        [dbo].[mart_fact_order_list] o
    JOIN
        [dbo].[mart_fact_orders_shipped] s ON o.order_id = s.order_reference_id
    WHERE
        o.quantity <> s.quantity
        AND s.split_shipment = 0; -- Exclude split shipments which may have partial quantities
    
    RETURN 0;
END;
