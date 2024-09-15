{{config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    on_schema_change='sync_all_columns',
    full_refresh= only_force_full_refresh(),
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
  WHERE behavior_at >= DATEADD(MONTH, -13, CURRENT_DATE)

  {% if is_incremental() %}
      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'behavior_at') }})) FROM {{ this }})
  {% else %}
  -- This will cover the first creation of the table or a full refresh and requires that the table be backfilled
  AND behavior_at > DATEADD('day', -30 ,CURRENT_DATE())

  {% endif %}

)

SELECT *
FROM structured_event_13_months
