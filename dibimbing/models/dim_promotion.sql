-- dim_product
{{
    config(
        materialized='table'
    )
}}

With t_data AS (
SELECT DISTINCT 
    `promotion-ids` AS promotion_ids
FROM
    {{ source('bronze', 'Amazon_Sale_Report') }}
)

SELECT {{ dbt_utils.generate_surrogate_key([
				'promotion_ids'
			]) }} as promotion_id, *
FROM t_data
