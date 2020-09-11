{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        source.primary_key                      AS source_primary_key,
        credits_flat.value['name']::VARCHAR     AS credit_description,
        credits_flat.value['amount']::FLOAT     AS credit_amount,
        {{ dbt_utils.surrogate_key([
            'source_primary_key',
            'credit_description',
            'credits_flat.value'] ) }}          AS credit_pk
    FROM source,
    LATERAL FLATTEN(input => credits) credits_flat

)

SELECT *
FROM renamed
