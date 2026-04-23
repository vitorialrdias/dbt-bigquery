{{ config(

    schema='raw'
) }}

SELECT
    JSON_VALUE(cart, '$.id') AS cart_id,
    JSON_VALUE(cart, '$.total') AS cart_total_value,
    JSON_VALUE(cart, '$.userId') AS user_id
FROM
    `dbt-bigquery-493617.raw.raw_sales_data`,
    UNNEST(JSON_EXTRACT_ARRAY(carts)) AS cart