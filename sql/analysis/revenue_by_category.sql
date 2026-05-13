-- ================================================
-- QUERY 4: Revenue by Product Category
-- Business Question: Which product categories
-- drive the most revenue?
-- ================================================

SELECT
    p.product_category_name,

    -- Total revenue
    ROUND(SUM(f.total_amount)::numeric, 2)  AS total_revenue,

    -- Number of orders
    COUNT(DISTINCT f.order_id)               AS total_orders,

    -- Items sold
    COUNT(f.sales_key)                       AS items_sold,

    -- Average price in this category
    ROUND(AVG(f.price)::numeric, 2)          AS avg_price

FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key

WHERE f.order_status = 'Delivered'

GROUP BY
    p.product_category_name

ORDER BY total_revenue DESC

LIMIT 15;