{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) | int %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) | int %}
{% set start_date = modules.datetime.datetime(year_value, month_value, 1) %}
{% set end_date = (start_date + modules.datetime.timedelta(days=31)).strftime('%Y-%m-01') %}

{{config({
    "unique_key":"event_id",
    "cluster_by":['derived_tstamp_date']
  })
}}

WITH base AS (

  SELECT
    event_id,
    derived_tstamp,
    contexts
  {% if target.name not in ("prod") -%}

  FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

  {%- else %}

  FROM {{ ref('snowplow_gitlab_good_events_source') }}

  {%- endif %}

  WHERE TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
    AND derived_tstamp >= '{{ start_date }}'
    AND derived_tstamp < '{{ end_date }}'

),

deduped_base AS (

    SELECT *
    FROM base AS base_1
    WHERE NOT EXISTS (
      SELECT 1
      FROM base AS base_2
      WHERE base_1.event_id = base_2.event_id
      GROUP BY event_id
      HAVING COUNT(*) > 1
    )

),

events_with_context_flattened AS (

  SELECT
    deduped_base.*,
    flat_contexts.value['schema']::VARCHAR      AS context_data_schema,
    TRY_PARSE_JSON(flat_contexts.value['data']) AS context_data
  FROM deduped_base
  INNER JOIN LATERAL FLATTEN(input => TRY_PARSE_JSON(contexts), path => 'data') AS flat_contexts

)

SELECT DISTINCT -- Some event_id are not unique despite haveing the same experiment context as discussed in MR 6288
  events_with_context_flattened.event_id,
  events_with_context_flattened.derived_tstamp::DATE              AS derived_tstamp_date,
  context_data                                                    AS experiment_context,
  context_data_schema                                             AS experiment_context_schema,
  context_data['experiment']::VARCHAR                             AS experiment_name,
  context_data['key']::VARCHAR                                    AS context_key,
  context_data['variant']::VARCHAR                                AS experiment_variant,
  ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ')  AS experiment_migration_keys
FROM events_with_context_flattened
WHERE LOWER(context_data_schema) LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%'