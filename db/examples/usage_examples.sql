-- Examples of using the new dbt-style models

-- 1. Get a unified view of all orders
SELECT *
FROM [dbo].[mart_fact_order_list]
ORDER BY order_date DESC;

-- 2. Get a unified view of all shipments
SELECT *
FROM [dbo].[mart_fact_orders_shipped]
ORDER BY ship_date DESC;

-- 3. Get reconciliation summary with matched orders and shipments
SELECT *
FROM [dbo].[mart_reconciliation_summary]
WHERE is_reconciled = 'YES'
ORDER BY ship_date DESC;

-- 4. Find unmatched shipments
SELECT *
FROM [dbo].[mart_fact_orders_shipped]
WHERE reconciliation_status = 'UNMATCHED'
ORDER BY ship_date DESC;

-- 5. Get average ship time by customer
SELECT
    customer_name,
    AVG(days_to_ship) AS avg_ship_days,
    MIN(days_to_ship) AS min_ship_days,
    MAX(days_to_ship) AS max_ship_days,
    COUNT(*) AS total_orders
FROM
    [dbo].[mart_reconciliation_summary]
WHERE
    is_reconciled = 'YES'
GROUP BY
    customer_name
ORDER BY
    AVG(days_to_ship) DESC;

-- 6. Get reconciliation rates by customer
SELECT
    customer_name,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN is_reconciled = 'YES' THEN 1 ELSE 0 END) AS matched_shipments,
    CAST(SUM(CASE WHEN is_reconciled = 'YES' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 AS match_rate_pct
FROM
    [dbo].[mart_reconciliation_summary]
GROUP BY
    customer_name
ORDER BY
    match_rate_pct DESC;

-- 7. Get shipments that need human review (potential matches)
SELECT
    s.shipment_id,
    s.customer_name,
    s.po_number,
    s.style_code,
    s.color_code,
    s.quantity,
    s.ship_date,
    s.reconciliation_status
FROM
    [dbo].[mart_fact_orders_shipped] s
WHERE
    s.reconciliation_status = 'NEEDS_REVIEW'
ORDER BY
    s.ship_date DESC;

-- 8. Run consistency tests
EXEC [dbo].[test_reconciliation_consistency];
