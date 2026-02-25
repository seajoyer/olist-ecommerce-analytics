DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;


-- Create staging tables with no constraints first (no PK, no UNIQUE)
CREATE UNLOGGED TABLE staging.orders_stg (
    order_id                       VARCHAR(32),
    customer_id                    VARCHAR(32),
    order_status                   VARCHAR(20),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP
);

CREATE UNLOGGED TABLE staging.order_items_stg (
    order_id             VARCHAR(32),
    order_item_id        INTEGER,
    product_id           VARCHAR(32),
    seller_id            VARCHAR(32),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2)
);

CREATE UNLOGGED TABLE staging.payments_stg (
    order_id              VARCHAR(32),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(20),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2)
);

CREATE UNLOGGED TABLE staging.reviews_stg (
    review_id               VARCHAR(32),
    order_id                VARCHAR(32),
    review_score            INTEGER,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE UNLOGGED TABLE staging.customers_stg (
    customer_id               VARCHAR(32),
    customer_unique_id        VARCHAR(32),
    customer_zip_code_prefix  VARCHAR(5),
    customer_city             VARCHAR(50),
    customer_state            VARCHAR(2)
);

CREATE UNLOGGED TABLE staging.sellers_stg (
    seller_id               VARCHAR(32),
    seller_zip_code_prefix  VARCHAR(5),
    seller_city             VARCHAR(50),
    seller_state            VARCHAR(2)
);

CREATE UNLOGGED TABLE staging.products_stg (
    product_id                  VARCHAR(32),
    product_category_name       VARCHAR(50),
    product_name_length         INTEGER,
    product_description_length  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            INTEGER,
    product_length_cm           INTEGER,
    product_height_cm           INTEGER,
    product_width_cm            INTEGER
);

CREATE UNLOGGED TABLE staging.categories_stg (
    product_category_name          VARCHAR(50),
    product_category_name_english  VARCHAR(50)
);

CREATE UNLOGGED TABLE staging.geolocation_stg (
    geolocation_zip_code_prefix  VARCHAR(5),
    geolocation_lat              NUMERIC(18,15),
    geolocation_lng              NUMERIC(18,15),
    geolocation_city             VARCHAR(50),
    geolocation_state            VARCHAR(2)
);


-- Load all data
COPY staging.orders_stg      FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_orders_dataset.csv'              CSV HEADER;
COPY staging.order_items_stg FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_order_items_dataset.csv'         CSV HEADER;
COPY staging.payments_stg    FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_order_payments_dataset.csv'      CSV HEADER;
COPY staging.reviews_stg     FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_order_reviews_dataset.csv'       CSV HEADER;
COPY staging.customers_stg   FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_customers_dataset.csv'           CSV HEADER;
COPY staging.sellers_stg     FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_sellers_dataset.csv'             CSV HEADER;
COPY staging.products_stg    FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_products_dataset.csv'            CSV HEADER;
COPY staging.categories_stg  FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/product_category_name_translation.csv' CSV HEADER;
COPY staging.geolocation_stg FROM '/home/dmitry/Projects/DataScience/pet/olist_analysis/data/raw/olist_geolocation_dataset.csv'         CSV HEADER;


-- Move to final tables + deduplicate
TRUNCATE raw.orders, raw.order_items, raw.order_payments, raw.order_reviews,
         raw.customers, raw.sellers, raw.products, raw.product_categories, raw.geolocation;

INSERT INTO raw.product_categories
SELECT DISTINCT *
FROM staging.categories_stg
ON CONFLICT (product_category_name) DO NOTHING;

-- Add missing categories from products (english = NULL)
INSERT INTO raw.product_categories (product_category_name)
SELECT DISTINCT product_category_name
FROM staging.products_stg
WHERE product_category_name IS NOT NULL
  AND product_category_name NOT IN (SELECT product_category_name FROM raw.product_categories)
ON CONFLICT (product_category_name) DO NOTHING;

INSERT INTO raw.customers
SELECT DISTINCT *
FROM staging.customers_stg
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO raw.sellers
SELECT DISTINCT *
FROM staging.sellers_stg
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO raw.geolocation
SELECT DISTINCT *
FROM staging.geolocation_stg
ON CONFLICT DO NOTHING;

INSERT INTO raw.products
SELECT DISTINCT *
FROM staging.products_stg
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO raw.orders
SELECT DISTINCT *
FROM staging.orders_stg
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO raw.order_items
SELECT DISTINCT *
FROM staging.order_items_stg
ON CONFLICT (order_id, order_item_id) DO NOTHING;

INSERT INTO raw.order_payments
SELECT DISTINCT *
FROM staging.payments_stg
ON CONFLICT (order_id, payment_sequential) DO NOTHING;

INSERT INTO raw.order_reviews
SELECT DISTINCT *
FROM staging.reviews_stg
ON CONFLICT (review_id) DO NOTHING;


-- Delete staging schema
DROP SCHEMA staging CASCADE;


-- Row counts
SELECT 'raw.geolocation'        AS "table", COUNT(*) FROM raw.geolocation         UNION ALL
SELECT 'raw.orders'             AS "table", COUNT(*) FROM raw.orders              UNION ALL
SELECT 'raw.order_items'        AS "table", COUNT(*) FROM raw.order_items         UNION ALL
SELECT 'raw.order_payments'     AS "table", COUNT(*) FROM raw.order_payments      UNION ALL
SELECT 'raw.order_reviews'      AS "table", COUNT(*) FROM raw.order_reviews       UNION ALL
SELECT 'raw.customers'          AS "table", COUNT(*) FROM raw.customers           UNION ALL
SELECT 'raw.sellers'            AS "table", COUNT(*) FROM raw.sellers             UNION ALL
SELECT 'raw.products'           AS "table", COUNT(*) FROM raw.products            UNION ALL
SELECT 'raw.product_categories' AS "table", COUNT(*) FROM raw.product_categories;
