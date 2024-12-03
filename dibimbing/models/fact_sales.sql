{{
    config(materialized='table')
}}

SELECT
    `Order ID` as order_id,
    Date as date,
    Status as status,
    {{ dbt_utils.generate_surrogate_key([
                'Fulfilment', 
                '`fulfilled-by`'
            ])}} AS fulfillment_id,
    {{ dbt_utils.generate_surrogate_key([
                'SKU'
            ]) }} as product_id,
    {{ dbt_utils.generate_surrogate_key([
                '`promotion-ids`'
            ]) }} as promotion_id,
    {{ dbt_utils.generate_surrogate_key([
                '`Sales Channel `'
            ]) }} AS sales_channel_id,
    {{ dbt_utils.generate_surrogate_key([
                '`ship-service-level`',
                '`ship-city`',
                '`ship-state`',
                '`ship-postal-code`',
                '`ship-country`'
            ]) }} AS sales_shipment,
    sum(qty) AS qty,
    COALESCE(sum(amount), 0) AS amount
FROM
    {{ source('bronze', 'Amazon_Sale_Report') }}
GROUP BY
    ALL
