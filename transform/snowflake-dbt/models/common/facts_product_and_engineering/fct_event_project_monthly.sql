{{ config(
    tags=["mnpi_exception", "product"],
    materialized = "incremental",
    unique_key = "event_project_monthly_pk",
    on_schema_change = "sync_all_columns",
    full_refresh=only_force_full_refresh()
) }}

{{ simple_cte([
    ('fct_event_valid', 'fct_event_valid'),
    ('dim_date', 'dim_date'),
    ('dim_project','dim_project'),
    ('dim_crm_account', 'dim_crm_account')
    ])
}},

--Find event data at a monthly project level

fact_with_month AS (

  SELECT
    fct_event_valid.*,
    dim_date.first_day_of_month AS event_calendar_month
  FROM fct_event_valid
  LEFT JOIN dim_date
    ON fct_event_valid.dim_event_date_id = dim_date.date_id
  WHERE dim_date.date_actual < dim_date.current_first_day_of_month
  {% if is_incremental() %}
  -- This means that the data will only be fully changed if it has been more than an full month from the last load.
  AND IFF( (SELECT MAX(event_calendar_month) FROM {{ this }}) < DATEADD('month', -1, dim_date.current_first_day_of_month), TRUE, FALSE)
  {% endif %}

),

--find project-specific information

project AS (

  SELECT
    dim_project.dim_project_id,
    DATE_TRUNC('month',dim_project.created_at)         AS project_created_month,
    dim_project.ultimate_parent_namespace_id,
    dim_project.dim_user_id_creator                    AS project_creator_id,
    dim_project.is_learn_gitlab                        AS project_is_learn_gitlab
  FROM dim_project

),

--find namespace's plan on last event of the month

plan_by_month AS (
                                                            
  SELECT                                                    
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date
  FROM fact_with_month
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_calendar_month
      ORDER BY event_created_at DESC) = 1

),

/*
Aggregate namespace event data by month
Exclude the current month because the data is incomplete (and the plan could change)
*/

fct_event_project_monthly AS (
    
    SELECT
      --Primary Key 
      {{ dbt_utils.generate_surrogate_key(['fact_with_month.event_calendar_month', 'fact_with_month.event_name', 'fact_with_month.dim_ultimate_parent_namespace_id','fact_with_month.dim_project_id']) }}       
                                                   AS event_project_monthly_pk,
                                            
      --Foreign Keys
      fact_with_month.dim_ultimate_parent_namespace_id,
      fact_with_month.dim_project_id,
      fact_with_month.dim_latest_product_tier_id,
      fact_with_month.dim_latest_subscription_id,
      fact_with_month.dim_crm_account_id,
      dim_crm_account.crm_account_name,
      fact_with_month.dim_billing_account_id,
      project.project_created_month,
      project.project_creator_id,
      project.project_is_learn_gitlab,
      plan_by_month.plan_id_at_event_date         AS plan_id_at_event_month,
      plan_by_month.plan_name_at_event_date       AS plan_name_at_event_month,
      plan_by_month.plan_was_paid_at_event_date   AS plan_was_paid_at_event_month,
      fact_with_month.event_calendar_month,
      fact_with_month.event_name,
      fact_with_month.section_name,
      fact_with_month.stage_name,
      fact_with_month.group_name,
      fact_with_month.is_smau,
      fact_with_month.is_gmau,
      fact_with_month.is_umau,
      fact_with_month.data_source,
      
      --Facts
      COUNT(*)                                    AS event_count,
      COUNT(DISTINCT fact_with_month.dim_user_id) AS user_count,
      COUNT(DISTINCT fact_with_month.event_date)  AS event_date_count

    FROM fact_with_month
    INNER JOIN plan_by_month
      ON fact_with_month.dim_ultimate_parent_namespace_id = plan_by_month.dim_ultimate_parent_namespace_id
      AND fact_with_month.event_calendar_month = plan_by_month.event_calendar_month
    INNER JOIN project
      ON fact_with_month.dim_project_id = project.dim_project_id
      AND fact_with_month.dim_ultimate_parent_namespace_id = project.ultimate_parent_namespace_id
    LEFT JOIN dim_crm_account --To include the crm_account_name field
      ON dim_crm_account.dim_crm_account_id = fact_with_month.dim_crm_account_id
    WHERE fact_with_month.dim_ultimate_parent_namespace_id IS NOT NULL
      AND fact_with_month.dim_project_id IS NOT NULL
    {{ dbt_utils.group_by(n=23) }}
        
)

{{ dbt_audit(
    cte_ref="fct_event_project_monthly",
    created_by="@dpeterson",
    updated_by="@dpeterson",
    created_date="2024-05-13",
    updated_date="2024-05-13"
) }}