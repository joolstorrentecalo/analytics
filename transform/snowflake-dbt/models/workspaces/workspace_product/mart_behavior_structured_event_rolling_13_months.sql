{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',13) }}"],
    cluster_by=['behavior_at::DATE','event_action']
  )

}}

WITH structured_event_13_months AS (
   
    SELECT
    {{ 
      dbt_utils.star(from=ref('mart_behavior_structured_event'), 
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ]) 
    }}
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE DATE_TRUNC(DAY, behavior_at) >= DATEADD(MONTH, -13, DATE_TRUNC(DAY, CURRENT_DATE))
    {% if is_incremental() %}
      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
    {% endif %}

)

{{ dbt_audit(
    cte_ref="structured_event_13_months",
    created_by="@lmai1",
    updated_by="@lmai1",
    created_date="2024-08-29",
    updated_date="2024-08-29"
) }}