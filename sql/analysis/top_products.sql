-- ================================================
-- QUERY 2: Top 10 Products by Revenue
-- Business Question: Which products generate
-- the most revenue?
-- ================================================

SELECT
    p.product_id,
    p.product_category_name,
    p.weight_category,

    -- Total revenue from this product
    ROUND(SUM(f.total_amount)::numeric, 2)  AS total_revenue,

    -- How many times this product was sold
    COUNT(f.sales_key)                       AS times_sold,

    -- Average selling price
    ROUND(AVG(f.price)::numeric, 2)          AS avg_price

FROM fact_sales f
JOIN dim_product p
    ON f.product_key = p.product_key

WHERE f.order_status = 'Delivered'

GROUP BY
    p.product_id,
    p.product_category_name,
    p.weight_category

ORDER BY total_revenue DESC

-- Only show top 10
LIMIT 10;