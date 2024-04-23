WITH data_transfer_source as (

SELECT * 
FROM {{ ref ('elastic_billing_data_transfer_by_deployment_source')}}

),

resource_source as (

SELECT * 
FROM {{ ref ('elastic_billing_resources_by_deployment_source')}}

),

renamed as (

SELECT
    deployment_id,
    extraction_start_date,
    extraction_end_date,
    'data_transfer' as charge_type,
    sku,
    cost,
    resource_quantity_value as usage,
    formated_quantity_value as usage_unit,
    ARRAY_CONSTRUCT(type, resource_name) as details
FROM data_transfer_source
UNION ALL
SELECT
    deployment_id,
    extraction_start_date,
    extraction_end_date,
    'capacity' as charge_type,
    sku,
    cost,
    hours as usage,
    'hours' as usage_unit,
    ARRAY_CONSTRUCT(resource_start_date, resource_end_date, instance_count, price_per_hour, resource_name)
FROM resource_source
)

SELECT * 
FROM renamed