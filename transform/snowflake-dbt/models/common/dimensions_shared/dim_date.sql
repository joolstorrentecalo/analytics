{{ config({
    "alias": "dim_date"
}) }}


WITH dates AS (

  SELECT
    {{ dbt_utils.star(
           from=ref('prep_date'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
  FROM {{ ref('prep_date') }}

)

{{ dbt_audit(
    cte_ref="dates",
    created_by="sffl",
    updated_by="jahsfkjsf",
    created_date="2020-06-01",
    updated_date="2023-08-14"
) }}