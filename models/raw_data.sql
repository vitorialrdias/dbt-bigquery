CREATE OR REPLACE TABLE `dbt-bigquery-493617.raw.raw_sales_datav1` AS
SELECT
    -- Dados do nível do Carrinho (Carts)
    JSON_VALUE(cart, '$.id') AS cart_id,
    JSON_VALUE(cart, '$.total') AS cart_total_value,
    JSON_VALUE(cart, '$.userId') AS user_id,
    
    -- Dados do nível do Produto (dentro de products)
    JSON_VALUE(product, '$.id') AS product_id,
    JSON_VALUE(product, '$.title') AS title,
    JSON_VALUE(product, '$.price') AS price,
    JSON_VALUE(product, '$.quantity') AS quantity,
    JSON_VALUE(product, '$.discountPercentage') AS discountPercentage,
    JSON_VALUE(product, '$.discountedTotal') AS product_discountedTotal

FROM
    `dbt-bigquery-493617.raw.raw_sales_data`,
    UNNEST(JSON_EXTRACT_ARRAY(carts)) AS cart,
    UNNEST(JSON_EXTRACT_ARRAY(cart, '$.products')) AS product