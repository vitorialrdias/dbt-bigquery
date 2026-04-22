{{ config(
    materialized='incremental',
    unique_key=['cart_id'],
    incremental_strategy='merge',
    schema='clean'
) }}


SELECT
    CAST(cart_id AS INT64) AS cart_id,
    CAST(cart_total_value AS NUMERIC) AS cart_total_value,
    CAST(user_id AS INT64) AS user_id
FROM
    `dbt-bigquery-493617.raw.raw_carts`