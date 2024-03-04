{{ config(
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id"
) }}

WITH final AS (

  SELECT {{ dbt_utils.star(from=ref('wk_prep_crm_opportunity_fy25'), except=["CREATED_BY", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_UPDATED_DATE", "DBT_UPDATED_AT", "DBT_CREATED_AT"]) }}
  FROM {{ ref('wk_prep_crm_opportunity_fy25') }}
  WHERE is_live = 0
  {% if is_incremental() %}
  
    AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)

SELECT * 
FROM final