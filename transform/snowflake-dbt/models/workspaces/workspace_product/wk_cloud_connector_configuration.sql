{{ config(
    materialized='table',
    tags=["product"]
) }}

SELECT *
FROM {{ ref('cloud_connector_configuration')}}
