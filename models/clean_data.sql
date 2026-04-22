{{ config(
    materialized='incremental',
    unique_key=['cart_id', 'product_id'],
    incremental_strategy='merge',
    schema='clean'
) }}


SELECT
    CAST(JSON_VALUE(cart_id) AS INT64) AS cart_id,
    CAST(JSON_VALUE(cart_total_value) AS FLOAT64) AS cart_total_value,
    CAST(JSON_VALUE(user_id) AS INT64) AS user_id,
    
    CAST(JSON_VALUE(product_id) AS INT64) AS product_id,
    JSON_VALUE(title) AS title,
    CAST(JSON_VALUE(price) AS FLOAT64) AS price,
    CAST(JSON_VALUE(quantity) AS INT64) AS quantity,
    CAST(JSON_VALUE(discountPercentage) AS FLOAT64) AS discountPercentage,
    CAST(JSON_VALUE(product_discountedTotal) AS FLOAT64) AS product_discountedTotal,
    CURRENT_DATE() AS ingestion_date

FROM
    `dbt-bigquery-493617.raw.raw_sales_datav1`