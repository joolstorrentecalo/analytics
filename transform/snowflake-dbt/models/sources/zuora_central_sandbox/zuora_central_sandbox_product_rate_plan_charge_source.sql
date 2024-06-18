WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'product_rate_plan_charge') }}

), renamed AS (

    SELECT 
      id                    AS product_rate_plan_charge_id,
      name                  AS product_rate_plan_charge_name,
      product_rate_plan_id  AS product_rate_plan_id,
      product_id            AS product_id,
      charge_delivery_c     AS product_rate_plan_charge_delivery,
      charge_tier_c         AS product_rate_plan_charge_tier,
      charge_deployment_c   AS product_rate_plan_charge_deployment
       
    FROM source
    
)

SELECT *
FROM renamed