{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }} 

WITH unify AS (
SELECT
REPLACE(f.value,'"','') AS crm_account_name,
o.gitlab_global_user_id,
o.requested_at AS _datetime,
'gitlab_ide_extension' AS app_id,
o.language,
o.delivery_type,
o.product_deployment_type,
o.is_direct_connection,
o.suggestion_id,
NULL AS behavior_structured_event_pk,
o.suggestion_source,
o.model_engine,
o.model_name,
o.extension_name,
o.extension_version,
o.ide_name,
o.ide_version,
o.ide_vendor,
o.language_server_version,
o.has_advanced_context,
o.is_streaming,
o.is_invoked,
o.options_count,
o.display_time_in_ms,
o.load_time_in_ms,
o.time_to_show_in_ms,
o.suggestion_outcome,
o.was_accepted,
o.was_cancelled,
o.was_error,
o.was_loaded,
o.was_not_provided,
o.was_rejected,
o.was_requested,
o.was_shown,
o.was_stream_completed,
o.was_stream_started
FROM
 {{ ref('rpt_behavior_code_suggestion_outcome') }} o, LATERAL FLATTEN(input => crm_account_names::ARRAY ) AS f
WHERE
f.value IS NOT NULL
AND
o.requested_at >= '2024-06-26'


UNION ALL 
SELECT
REPLACE(f.value,'"','') AS crm_account_name,
r.gitlab_global_user_id,
r.behavior_at,
r.app_id,
r.language,
r.delivery_type,
r.product_deployment_type,
r.is_direct_connection,
NULL AS suggestion_id,
r.behavior_structured_event_pk,
NULL AS suggestion_source,
NULL AS model_engine,
NULL AS model_name,
NULL AS extension_name,
NULL AS extension_version,
NULL AS ide_name,
NULL AS ide_version,
NULL AS ide_vendor,
NULL AS language_server_version,
NULL AS has_advanced_context,
NULL AS is_streaming,
NULL AS is_invoked,
NULL AS options_count,
NULL AS display_time_in_ms,
NULL AS load_time_in_ms,
NULL AS time_to_show_in_ms,
NULL AS suggestion_outcome,
NULL AS was_accepted,
NULL AS was_cancelled,
NULL AS was_error,
NULL AS was_loaded,
NULL AS was_not_provided,
NULL AS was_rejected,
NULL AS was_requested,
NULL AS was_shown,
NULL AS was_stream_completed,
NULL AS was_stream_started
FROM 
{{ ref('rpt_behavior_code_suggestion_gateway_request') }} r, LATERAL FLATTEN(input => crm_account_names::ARRAY ) AS f
WHERE
f.value IS NOT NULL
AND
r.behavior_at >= '2024-06-26'
)

SELECT
*
FROM
unify u
