-- ================================================
-- QUERY 3: Revenue by Region
-- Business Question: Which regions of Brazil
-- generate the most sales?
-- ================================================

SELECT
    l.customer_region,
    l.customer_state,

    -- Total revenue from this region
    ROUND(SUM(f.total_amount)::numeric, 2)  AS total_revenue,

    -- Number of orders
    COUNT(DISTINCT f.order_id)               AS total_orders,

    -- Number of unique customers
    COUNT(DISTINCT f.customer_key)           AS unique_customers,

    -- Average order value
    ROUND(AVG(f.total_amount)::numeric, 2)  AS avg_order_value

FROM fact_sales f
JOIN dim_location l
    ON f.location_key = l.location_key

WHERE f.order_status = 'Delivered'

GROUP BY
    l.customer_region,
    l.customer_state

ORDER BY total_revenue DESC;