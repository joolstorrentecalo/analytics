{{config(

    materialized='incremental',
    unique_key='fct_behavior_website_page_view_sk',
    tags=['product'],
    on_schema_change='sync_all_columns',
    post_hook=["{{ rolling_window_delete('behavior_at','month',13) }}"]
  )

}}

WITH fct_behavior_website_page_view_13_months AS (
   
    SELECT
    {{ 
      dbt_utils.star(from=ref('fct_behavior_website_page_view'), 
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ]) 
    }}
  FROM {{ ref('fct_behavior_website_page_view') }}
  WHERE DATE_TRUNC(DAY, behavior_at) >= DATEADD(MONTH, -13, DATE_TRUNC(DAY, CURRENT_DATE))
    {% if is_incremental() %}
      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})
    {% endif %}

)

{{ dbt_audit(
    cte_ref="fct_behavior_website_page_view_13_months",
    created_by="@lmai1",
    updated_by="@lmai1",
    created_date="2024-09-03",
    updated_date="2024-09-03"
) }}