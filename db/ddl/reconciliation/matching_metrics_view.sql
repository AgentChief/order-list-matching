-- matching_metrics_view View
-- Provides metrics for matching performance

CREATE VIEW [dbo].[matching_metrics_view] AS
SELECT 
    r.[customer_name],
    CONVERT(DATE, r.[reconciliation_date]) AS reconciliation_date,
    COUNT(*) AS total_records,
    SUM(CASE WHEN r.[match_status] = 'matched' THEN 1 ELSE 0 END) AS matched_count,
    SUM(CASE WHEN r.[match_status] = 'unmatched' THEN 1 ELSE 0 END) AS unmatched_count,
    SUM(CASE WHEN r.[match_status] = 'uncertain' THEN 1 ELSE 0 END) AS uncertain_count,
    CAST(SUM(CASE WHEN r.[match_status] = 'matched' THEN 1 ELSE 0 END) AS FLOAT) / 
        NULLIF(COUNT(*), 0) * 100 AS match_percentage,
    AVG(CASE WHEN r.[match_status] = 'matched' THEN r.[confidence_score] ELSE NULL END) AS avg_confidence_score,
    COUNT(DISTINCT r.[match_method]) AS methods_used
FROM 
    [dbo].[reconciliation_result] r
GROUP BY 
    r.[customer_name],
    CONVERT(DATE, r.[reconciliation_date]);
