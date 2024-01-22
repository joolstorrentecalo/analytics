{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) | int %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) | int %}
{% set start_date = modules.datetime.datetime(year_value, month_value, 1) %}
{% set end_date = (start_date + modules.datetime.timedelta(days=31)).strftime('%Y-%m-01') %}

{{config({
    "unique_key":"event_id",
    "cluster_by":['derived_tstamp_date']
  })
}}


{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


WITH filtered_source as (

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
)

, base AS (
  
    SELECT DISTINCT * 
    FROM filtered_source

), events_with_context_flattened AS (
    /*
    we need to extract the web_page_id from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The context we are looking for containing the web_page_id is this one:
      {
      'data': {
      'id': 'de5069f7-32cf-4ad4-98e4-dafe05667089'
      },
      'schema': 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
      }
    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data (where the web_page_id will be contained)
    */
    SELECT 
      base.*,
      f.value['schema']::TEXT     AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the web_page context, which has this context schema: iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0
Then we extract the id from the context_data column
*/
SELECT 
    events_with_context_flattened.event_id,
    events_with_context_flattened.derived_tstamp::DATE  AS derived_tstamp_date,
    context_data                                        AS web_page_context,
    context_data_schema                                 AS web_page_context_schema,
    context_data['id']::TEXT                            AS web_page_id
FROM events_with_context_flattened
WHERE context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0'
