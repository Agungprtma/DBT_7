{{
    config(materialized = 'table')
}}

WITH t_data AS (
SELECT DISTINCT 
    `ship-service-level` AS ship_service_level,
    `ship-city` AS ship_city,
    `ship-state` AS ship_state,
    `ship-postal-code` AS ship_postal_code,
    `ship-country` AS ship_country
FROM 
    {{ source('bronze', 'Amazon_Sale_Report') }}
)

SELECT {{ dbt_utils.generate_surrogate_key([
                'ship_service_level',
                'ship_city',
                'ship_state',
                'ship_postal_code',
                'ship_country'
            ]) }} AS sales_shipment, *
FROM t_data