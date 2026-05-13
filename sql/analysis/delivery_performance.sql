-- ================================================
-- QUERY 5: Delivery Performance by Region
-- Business Question: Which regions have the
-- fastest and slowest delivery times?
-- ================================================

SELECT
    l.customer_region,

    -- Average delivery days
    ROUND(AVG(f.delivery_time_days)::numeric, 1)  AS avg_delivery_days,

    -- Fastest delivery
    MIN(f.delivery_time_days)                      AS min_delivery_days,

    -- Slowest delivery
    MAX(f.delivery_time_days)                      AS max_delivery_days,

    -- Number of delivered orders
    COUNT(f.sales_key)                             AS total_deliveries

FROM fact_sales f
JOIN dim_location l
    ON f.location_key = l.location_key

-- Only look at delivered orders with valid delivery time
WHERE f.order_status = 'Delivered'
AND   f.delivery_time_days IS NOT NULL
AND   f.delivery_time_days > 0

GROUP BY
    l.customer_region

ORDER BY avg_delivery_days ASC;