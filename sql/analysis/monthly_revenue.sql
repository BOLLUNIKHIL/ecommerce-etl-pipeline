-- ================================================
-- QUERY 1: Monthly Revenue Trend
-- Business Question: How did revenue grow
-- month by month across 2016-2018?
-- ================================================

SELECT
    d.year,
    d.month,
    d.month_name,

    -- Total revenue for this month
    ROUND(SUM(f.total_amount)::numeric, 2)     AS total_revenue,

    -- Total number of orders this month
    COUNT(DISTINCT f.order_id)                  AS total_orders,

    -- Average order value this month
    ROUND(AVG(f.total_amount)::numeric, 2)      AS avg_order_value,

    -- Total items sold this month
    COUNT(f.sales_key)                          AS total_items_sold

FROM fact_sales f
JOIN dim_date d
    ON f.date_key = d.date_key

-- Only look at completed delivered orders
WHERE f.order_status = 'Delivered'

GROUP BY
    d.year,
    d.month,
    d.month_name

ORDER BY
    d.year,
    d.month;