DROP SCHEMA IF EXISTS analytics CASCADE;
CREATE SCHEMA analytics;


-- Orders
DROP MATERIALIZED VIEW IF EXISTS analytics.orders_enriched;
CREATE MATERIALIZED VIEW analytics.orders_enriched AS
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))/86400 as delivery_days_actual,
    EXTRACT(EPOCH FROM (o.order_estimated_delivery_date - o.order_purchase_timestamp))/86400 as delivery_days_estimated,
    
    SUM(oi.price) as order_value,
    SUM(oi.freight_value) as freight_value,
    SUM(oi.price + oi.freight_value) as order_total_value,
    COUNT(oi.order_item_id) as items_count,
    
    MAX(op.payment_type) as payment_type,
    MAX(op.payment_installments) as payment_installments,
    
    MAX(r.review_score) as review_score,
    MAX(r.review_creation_date) as review_date
    
FROM raw.orders o
LEFT JOIN raw.customers C       ON o.customer_id = c.customer_id
LEFT JOIN raw.order_items oi    ON o.order_id = oi.order_id
LEFT JOIN raw.order_payments op ON o.order_id = op.order_id
LEFT JOIN raw.order_reviews r   ON o.order_id = r.order_id
GROUP BY o.order_id, o.customer_id, c.customer_unique_id, c.customer_city, 
         c.customer_state, o.order_status, o.order_purchase_timestamp,
         o.order_delivered_customer_date, o.order_estimated_delivery_date;


-- Products
DROP MATERIALIZED VIEW IF EXISTS analytics.products_enriched;
CREATE MATERIALIZED VIEW analytics.products_enriched AS
SELECT 
    p.*,
    pc.product_category_name_english as category_english
FROM raw.products p
LEFT JOIN raw.product_categories pc 
    ON p.product_category_name = pc.product_category_name;


-- Daily metrics
DROP MATERIALIZED VIEW IF EXISTS analytics.daily_metrics;
CREATE MATERIALIZED VIEW analytics.daily_metrics AS
SELECT 
    DATE(order_purchase_timestamp) as date,
    COUNT(DISTINCT order_id) as orders,
    COUNT(DISTINCT customer_id) as active_customers,
    SUM(order_total_value) as gmv,
    AVG(order_total_value) as avg_order_value,
    SUM(order_value) as revenue,
    SUM(freight_value) as freight_revenue
FROM analytics.orders_enriched
WHERE order_status = 'delivered'
GROUP BY DATE(order_purchase_timestamp)
ORDER BY DATE;


-- Weekly metrics
DROP MATERIALIZED VIEW IF EXISTS analytics.weekly_metrics;
CREATE MATERIALIZED VIEW analytics.weekly_metrics AS
SELECT 
    DATE_TRUNC('week', order_purchase_timestamp) as week,
    COUNT(DISTINCT order_id) as orders,
    COUNT(DISTINCT customer_id) as active_customers,
    SUM(order_total_value) as gmv,
    AVG(order_total_value) as avg_order_value
FROM analytics.orders_enriched
WHERE order_status = 'delivered'
GROUP BY DATE_TRUNC('week', order_purchase_timestamp)
ORDER BY week;


-- Monthly metrics
DROP MATERIALIZED VIEW IF EXISTS analytics.monthly_metrics;
CREATE MATERIALIZED VIEW analytics.monthly_metrics AS
SELECT 
    DATE_TRUNC('month', order_purchase_timestamp) as month,
    COUNT(DISTINCT order_id) as orders,
    COUNT(DISTINCT customer_id) as active_customers,
    COUNT(DISTINCT customer_unique_id) as unique_customers,
    SUM(order_total_value) as gmv,
    AVG(order_total_value) as avg_order_value,
    SUM(order_value) as revenue,
    SUM(freight_value) as freight_revenue
FROM analytics.orders_enriched
WHERE order_status = 'delivered'
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY MONTH;


-- Customer cohorts
DROP MATERIALIZED VIEW IF EXISTS analytics.customer_cohorts;
CREATE MATERIALIZED VIEW analytics.customer_cohorts AS
SELECT 
    customer_unique_id,
    DATE_TRUNC('month', MIN(order_purchase_timestamp)) as cohort_month,
    MIN(order_purchase_timestamp) as first_order_date,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_total_value) as total_spent
FROM analytics.orders_enriched
WHERE order_status = 'delivered'
GROUP BY customer_unique_id;

CREATE INDEX idx_cohorts_customer ON analytics.customer_cohorts(customer_unique_id);
CREATE INDEX idx_cohorts_month ON analytics.customer_cohorts(cohort_month);


-- Cohort activity
DROP MATERIALIZED VIEW IF EXISTS analytics.cohort_activity;
CREATE MATERIALIZED VIEW analytics.cohort_activity AS
WITH cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_unique_id) as cohort_size
    FROM analytics.customer_cohorts
    GROUP BY cohort_month
)
SELECT 
    cc.cohort_month,
    DATE_TRUNC('month', o.order_purchase_timestamp) as activity_month,
    EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', o.order_purchase_timestamp), cc.cohort_month)) as month_number,
    COUNT(DISTINCT o.customer_unique_id) as active_customers,
    cs.cohort_size,
    ROUND(100.0 * COUNT(DISTINCT o.customer_unique_id) / cs.cohort_size, 1) as retention_pct
FROM analytics.customer_cohorts cc
JOIN analytics.orders_enriched o ON cc.customer_unique_id = o.customer_unique_id
JOIN cohort_size cs ON cc.cohort_month = cs.cohort_month
WHERE o.order_status = 'delivered'
GROUP BY cc.cohort_month, DATE_TRUNC('month', o.order_purchase_timestamp), cs.cohort_size
ORDER BY cc.cohort_month, activity_month;

CREATE INDEX idx_cohort_activity ON analytics.cohort_activity(cohort_month, month_number);


-- Order funnel
DROP MATERIALIZED VIEW IF EXISTS analytics.order_funnel;
CREATE MATERIALIZED VIEW analytics.order_funnel AS
WITH funnel_data AS (
    SELECT 
        DATE_TRUNC('month', order_purchase_timestamp) as month,
        COUNT(*) as orders_created,
        COUNT(CASE WHEN order_status NOT IN ('canceled', 'unavailable') THEN 1 END) as orders_approved,
        COUNT(CASE WHEN order_delivered_carrier_date IS NOT NULL THEN 1 END)        as orders_shipped,
        COUNT(CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 END)       as orders_delivered,
        COUNT(CASE WHEN order_status = 'delivered' THEN 1 END)                      as orders_completed
    FROM raw.orders
    GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
)
SELECT
    month,
    orders_created,
    orders_approved,
    orders_shipped,
    orders_delivered,
    orders_completed,
    ROUND(100.0 * orders_approved  / NULLIF(orders_created, 0), 1)  as approval_rate,
    ROUND(100.0 * orders_shipped   / NULLIF(orders_approved, 0), 1) as shipping_rate,
    ROUND(100.0 * orders_delivered / NULLIF(orders_shipped, 0), 1)  as delivery_rate,
    ROUND(100.0 * orders_completed / NULLIF(orders_created, 0), 1)  as completion_rate
FROM funnel_data
ORDER BY MONTH;


-- RFM segments
DROP MATERIALIZED VIEW IF EXISTS analytics.rfm_segments;
CREATE MATERIALIZED VIEW analytics.rfm_segments AS
WITH reference_date AS (
    SELECT MAX(DATE(order_purchase_timestamp)) as max_order_date
    FROM analytics.orders_enriched
    WHERE order_status = 'delivered'
),
rfm_calc AS (
SELECT 
    o.customer_unique_id,
    r.max_order_date - MAX(DATE(o.order_purchase_timestamp)) as recency_days,
    COUNT(DISTINCT o.order_id) as frequency,
    SUM(o.order_total_value) as monetary
FROM analytics.orders_enriched o
CROSS JOIN reference_date r
WHERE o.order_status = 'delivered'
GROUP BY o.customer_unique_id, r.max_order_date
),
rfm_scores AS (
    SELECT 
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM rfm_calc
)
SELECT 
    customer_unique_id,
    recency_days,
    frequency,
    ROUND(monetary::numeric, 2) as monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score, f_score, m_score) as rfm_score,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 OR r_score >= 3 AND f_score <= 2 THEN 'New & Recent'
        WHEN r_score >= 3 AND f_score >= 2 THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND (f_score >= 3 OR m_score >= 3) THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
        ELSE 'Other'
    END as segment
FROM rfm_scores;

CREATE INDEX IF NOT EXISTS idx_rfm_segment ON analytics.rfm_segments(segment);


-- Category performance
DROP MATERIALIZED VIEW IF EXISTS analytics.category_performance;
CREATE MATERIALIZED VIEW analytics.category_performance AS
SELECT 
    COALESCE(p.category_english, 'Unknown') as category,
    COUNT(DISTINCT oi.order_id) as orders,
    COUNT(DISTINCT o.customer_unique_id) as customers,
    SUM(oi.price) as revenue,
    ROUND(AVG(oi.price), 2) as avg_item_price,
    SUM(oi.price + oi.freight_value) as total_gmv,
    ROUND(AVG(o.delivery_days_actual), 1) as avg_delivery_days,
    ROUND(AVG(o.review_score), 2) as avg_review_score,
    COUNT(oi.order_item_id) as items_sold
FROM raw.order_items oi
JOIN analytics.orders_enriched o ON oi.order_id = o.order_id
JOIN analytics.products_enriched p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY COALESCE(p.category_english, 'Unknown');

CREATE INDEX IF NOT EXISTS idx_category_perf ON analytics.category_performance(category);


-- Unit economics
DROP MATERIALIZED VIEW IF EXISTS analytics.unit_economics;
CREATE MATERIALIZED VIEW analytics.unit_economics AS
SELECT 
    DATE_TRUNC('month', order_purchase_timestamp) as month,
    customer_state,
    COUNT(DISTINCT order_id) as orders,
    SUM(order_value) as revenue,
    SUM(freight_value) as delivery_cost,
    SUM(order_value - freight_value) as contribution_margin,
    ROUND(AVG(order_value), 2) as avg_revenue_per_order,
    ROUND(AVG(freight_value), 2) as avg_delivery_cost_per_order,
    ROUND(AVG(order_value - freight_value), 2) as avg_margin_per_order,
    ROUND(100.0 * (SUM(order_value) - SUM(freight_value)) / NULLIF(SUM(order_value), 0), 1) as margin_pct
FROM analytics.orders_enriched
WHERE order_status = 'delivered'
GROUP BY DATE_TRUNC('month', order_purchase_timestamp), customer_state;


-- Satisfaction metrics
DROP MATERIALIZED VIEW IF EXISTS analytics.satisfaction_metrics;
CREATE MATERIALIZED VIEW analytics.satisfaction_metrics AS
WITH review_metrics AS (
    SELECT 
        DATE_TRUNC('month', order_purchase_timestamp) as month,
        review_score,
        delivery_days_actual,
        CASE 
            WHEN delivery_days_actual <= delivery_days_estimated THEN 'On Time'
            ELSE 'Delayed'
        END as delivery_status,
        customer_state,
        order_total_value
    FROM analytics.orders_enriched
    WHERE order_status = 'delivered' 
        AND review_score IS NOT NULL
)
SELECT 
    month,
    COUNT(*) as reviews_total,
    ROUND(AVG(review_score), 2) as avg_score,
    COUNT(CASE WHEN review_score >= 4 THEN 1 END) as promoters,
    COUNT(CASE WHEN review_score = 3 THEN 1 END) as passives,
    COUNT(CASE WHEN review_score <= 2 THEN 1 END) as detractors,
    ROUND(100.0 * (COUNT(CASE WHEN review_score >= 4 THEN 1 END) - COUNT(CASE WHEN review_score <= 2 THEN 1 END)) / COUNT(*), 1) as nps,
    ROUND(AVG(CASE WHEN delivery_status = 'On Time' THEN review_score END), 2) as avg_score_on_time,
    ROUND(AVG(CASE WHEN delivery_status = 'Delayed' THEN review_score END), 2) as avg_score_delayed
FROM review_metrics
GROUP BY month
ORDER BY month;

SELECT 'Data marts created successfully' AS status;
