DROP SCHEMA IF EXISTS raw CASCADE;
CREATE SCHEMA raw;


CREATE TABLE raw.product_categories (
    product_category_name         VARCHAR(50) PRIMARY KEY,
    product_category_name_english VARCHAR(50)
);

CREATE TABLE raw.customers (
    customer_id              VARCHAR(32) PRIMARY KEY,
    customer_unique_id       VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(5),
    customer_city            VARCHAR(50),
    customer_state           VARCHAR(2)
);

CREATE TABLE raw.sellers (
    seller_id                VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix   VARCHAR(5),
    seller_city              VARCHAR(50),
    seller_state             VARCHAR(2)
);

CREATE TABLE raw.geolocation (
    geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
    geolocation_lat             NUMERIC(18,15),
    geolocation_lng             NUMERIC(18,15),
    geolocation_city            VARCHAR(50),
    geolocation_state           VARCHAR(2)
);

CREATE INDEX idx_customers_zip ON raw.customers(customer_zip_code_prefix);
CREATE INDEX idx_sellers_zip   ON raw.sellers(seller_zip_code_prefix);
CREATE INDEX idx_geo_zip       ON raw.geolocation(geolocation_zip_code_prefix);


CREATE TABLE raw.products (
    product_id                   VARCHAR(32) PRIMARY KEY,
    product_category_name        VARCHAR(50),
    product_name_length          INTEGER,
    product_description_length   INTEGER,
    product_photos_qty           INTEGER,
    product_weight_g             INTEGER,
    product_length_cm            INTEGER,
    product_height_cm            INTEGER,
    product_width_cm             INTEGER,

    CONSTRAINT fk_products_category
        FOREIGN KEY (product_category_name)
        REFERENCES raw.product_categories(product_category_name)
        ON DELETE SET NULL
);

CREATE TABLE raw.orders (
    order_id                       VARCHAR(32) PRIMARY KEY,
    customer_id                    VARCHAR(32) NOT NULL,
    order_status                   VARCHAR(20),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES raw.customers(customer_id)
        ON DELETE RESTRICT
);


CREATE TABLE raw.order_items (
    order_id            VARCHAR(32) NOT NULL,
    order_item_id       INTEGER     NOT NULL,
    product_id          VARCHAR(32) NOT NULL,
    seller_id           VARCHAR(32) NOT NULL,
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),

    PRIMARY KEY (order_id, order_item_id),

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES raw.orders(order_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES raw.products(product_id)
        ON DELETE RESTRICT,

    CONSTRAINT fk_order_items_seller
        FOREIGN KEY (seller_id)
        REFERENCES raw.sellers(seller_id)
        ON DELETE RESTRICT
);

CREATE TABLE raw.order_payments (
    order_id             VARCHAR(32) NOT NULL,
    payment_sequential   INTEGER     NOT NULL,
    payment_type         VARCHAR(20),
    payment_installments INTEGER,
    payment_value        NUMERIC(10,2),

    PRIMARY KEY (order_id, payment_sequential),

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES raw.orders(order_id)
        ON DELETE CASCADE
);

CREATE TABLE raw.order_reviews (
    review_id               VARCHAR(32) PRIMARY KEY,
    order_id                VARCHAR(32) NOT NULL,
    review_score            INTEGER CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,

    CONSTRAINT fk_reviews_order
        FOREIGN KEY (order_id)
        REFERENCES raw.orders(order_id)
        ON DELETE RESTRICT
);

CREATE INDEX idx_orders_customer ON raw.orders(customer_id);
CREATE INDEX idx_orders_status ON raw.orders(order_status);
CREATE INDEX idx_orders_timestamp ON raw.orders(order_purchase_timestamp);
CREATE INDEX idx_order_items_product ON raw.order_items(product_id);
CREATE INDEX idx_order_items_seller ON raw.order_items(seller_id);
CREATE INDEX idx_reviews_score ON raw.order_reviews(review_score);

SELECT 'Tables for raw data created successfully' AS status;
