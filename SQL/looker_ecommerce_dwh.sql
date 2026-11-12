CREATE DATABASE IF NOT EXISTS looker_ecommerce_dwh;

USE looker_ecommerce_dwh;

CREATE TABLE IF NOT EXISTS dim_users (
user_key        BIGINT   NOT NULL,
user_id         INT      NOT NULL,
first_name      STRING,
last_name       STRING,
email           STRING,
age             INT,
gender          STRING,
state           STRING,
street_address  STRING,
city            STRING,
country         STRING,
registered_date_key   INT,
start_date      DATE,
end_date        DATE,
is_current      BOOLEAN,
CONSTRAINT pk_dim_users PRIMARY KEY (user_key) DISABLE NOVALIDATE
)
PARTITIONED BY (traffic_source STRING)
CLUSTERED BY (user_key) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim_products (
product_key                BIGINT  NOT NULL,
product_id                 INT     NOT NULL,
product_name               STRING,
brand                      STRING,
category                   STRING,
department                 STRING,
cost                       DECIMAL(10,2),
retail_price               DECIMAL(10,2),
distribution_center_name   STRING,
distribution_center_state  STRING,
start_date                 DATE,
end_date                   DATE,
is_current                 BOOLEAN,
CONSTRAINT pk_dim_products PRIMARY KEY (product_key) DISABLE NOVALIDATE
)
CLUSTERED BY (product_key) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim_order_status (
status_id    INT NOT NULL,
status_name  STRING,
CONSTRAINT pk_dim_order_status PRIMARY KEY (status_id) DISABLE NOVALIDATE
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS dim_date (
date_key          INT      NOT NULL,
full_date         DATE,
day_of_month      INT,
day_of_week       INT,
day_name          STRING,
day_of_year       INT,
week_of_year      INT,
month_number      INT,
month_name        STRING,
calendar_quarter  INT,
calendar_year     INT,
is_weekend        BOOLEAN,
CONSTRAINT pk_dim_date PRIMARY KEY (date_key) DISABLE NOVALIDATE
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS fact_order_items (
order_item_id       INT           NOT NULL,
order_id            INT,
user_key            BIGINT,
product_key         BIGINT,
created_date_key    INT,
shipped_date_key    INT,
delivered_date_key  INT,
returned_date_key   INT,
status_id           INT,
sale_price          DECIMAL(10,2),
CONSTRAINT pk_fact_order_items PRIMARY KEY (order_item_id) DISABLE NOVALIDATE,
CONSTRAINT fk_foi_user_key    FOREIGN KEY (user_key)           REFERENCES looker_ecommerce_dwh.dim_users(user_key)       DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_product_key FOREIGN KEY (product_key)        REFERENCES looker_ecommerce_dwh.dim_products(product_key) DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_created_at FOREIGN KEY (created_date_key)   REFERENCES looker_ecommerce_dwh.dim_date(date_key)       DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_shipped_at FOREIGN KEY (shipped_date_key)   REFERENCES looker_ecommerce_dwh.dim_date(date_key)       DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_delivered_at   FOREIGN KEY (delivered_date_key) REFERENCES looker_ecommerce_dwh.dim_date(date_key)       DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_returned_at  FOREIGN KEY (returned_date_key)  REFERENCES looker_ecommerce_dwh.dim_date(date_key)       DISABLE NOVALIDATE RELY,
CONSTRAINT fk_foi_status_id  FOREIGN KEY (status_id)  REFERENCES looker_ecommerce_dwh.dim_order_status(status_id)       DISABLE NOVALIDATE RELY
)
PARTITIONED BY (order_year INT)
CLUSTERED BY (user_key) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS fact_events (
event_id          BIGINT NOT NULL,
user_key          BIGINT,
created_date_key  INT,
session_id        STRING,
sequence_number   INT,
event_type        STRING,
uri               STRING,
browser           STRING,
traffic_source    STRING,
city              STRING,
state             STRING,
CONSTRAINT pk_fact_events PRIMARY KEY (event_id) DISABLE NOVALIDATE,
CONSTRAINT fk_fe_user_key    FOREIGN KEY (user_key)         REFERENCES looker_ecommerce_dwh.dim_users(user_key) DISABLE NOVALIDATE RELY,
CONSTRAINT fk_fe_created_at FOREIGN KEY (created_date_key) REFERENCES looker_ecommerce_dwh.dim_date(date_key)  DISABLE NOVALIDATE RELY
)
PARTITIONED BY (event_year INT,event_month INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
