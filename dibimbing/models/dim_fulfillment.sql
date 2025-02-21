-- dim_fulfillment
{{
  config(
    materialized='table'
  )
}}

With t_data AS (
SELECT DISTINCT 
  Fulfilment AS fulfillment, 
  `fulfilled-by` AS fulfilled_by
FROM
    {{ source('bronze', 'Amazon_Sale_Report') }}
)

SELECT 
{{ dbt_utils.generate_surrogate_key([
				'fulfillment', 
				'fulfilled_by'
			])}} AS fulfillment_id, *
FROM t_data
