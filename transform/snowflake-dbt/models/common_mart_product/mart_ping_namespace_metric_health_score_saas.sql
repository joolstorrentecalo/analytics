{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
  config({
    "materialized": "table"
  })
}}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(table=ref ('health_score_metrics'), column='metric_name', max_records=1000, default=['']) %}

{{ simple_cte([
    ('prep_saas_usage_ping_namespace','prep_saas_usage_ping_namespace'),
    ('dim_date','dim_date'),
    ('gainsight_wave_metrics','health_score_metrics'),
    ('instance_types', 'dim_host_instance_type'),
    ('map_subscription_namespace_month', 'map_latest_subscription_namespace_monthly')
]) }}

, instance_types_ordering AS (
    SELECT
      *,
      CASE
        WHEN instance_type = 'Production' THEN 1
        WHEN instance_type = 'Non-Production' THEN 2
        WHEN instance_type = 'Unknown' THEN 3
        ELSE 4
      END AS ordering_field
    FROM instance_types
)

, joined AS (

    SELECT 
      prep_saas_usage_ping_namespace.dim_namespace_id,
      prep_saas_usage_ping_namespace.ping_date,
      prep_saas_usage_ping_namespace.ping_name,
      prep_saas_usage_ping_namespace.counter_value,
      dim_date.first_day_of_month                           AS reporting_month, 
      map_subscription_namespace_month.dim_subscription_id,
      map_subscription_namespace_month.dim_subscription_id_original,
      instance_types_ordering.instance_type
    FROM prep_saas_usage_ping_namespace
    LEFT JOIN instance_types_ordering
      ON prep_saas_usage_ping_namespace.dim_namespace_id = instance_types_ordering.namespace_id
    INNER JOIN dim_date
      ON prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    INNER JOIN gainsight_wave_metrics
      ON prep_saas_usage_ping_namespace.ping_name = gainsight_wave_metrics.metric_name
    INNER JOIN map_subscription_namespace_month
      ON prep_saas_usage_ping_namespace.dim_namespace_id = map_subscription_namespace_month.dim_namespace_id
        AND dim_date.first_day_of_month = map_subscription_namespace_month.date_month
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY 
        dim_date.first_day_of_month,
        map_subscription_namespace_month.dim_subscription_id_original,
        prep_saas_usage_ping_namespace.dim_namespace_id,
        prep_saas_usage_ping_namespace.ping_name
      ORDER BY 
        prep_saas_usage_ping_namespace.ping_date DESC,
        instance_types_ordering.ordering_field ASC --prioritizing Production instances

    ) = 1

), pivoted AS (

    SELECT
      dim_namespace_id,
      dim_subscription_id,
      dim_subscription_id_original,
      reporting_month,
      instance_type,
      MAX(ping_date)                                        AS ping_date,
      {{ dbt_utils.pivot('ping_name', gainsight_wave_metrics, then_value='counter_value') }}
    FROM joined
    {{ dbt_utils.group_by(n=5)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@mdrusell",
    updated_by="@mdrussell",
    created_date="2022-10-12",
    updated_date="2022-10-21"
) }}
