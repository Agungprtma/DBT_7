-- dim_product
{{
  config(
    materialized='table'
  )
}}

With t_data AS (
SELECT DISTINCT 
  Style AS style,
  SKU AS sku,
  Category AS category,
  Size AS size,
FROM
    {{ source('bronze', 'Amazon_Sale_Report') }}
)

SELECT {{ dbt_utils.generate_surrogate_key([
				'sku'
			]) }} as product_id, *
FROM t_data
