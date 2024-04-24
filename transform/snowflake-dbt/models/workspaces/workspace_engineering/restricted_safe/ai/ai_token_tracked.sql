WITH mart AS (
  SELECT * FROM
    {{ ref ('mart_behavior_structured_event') }}
  WHERE event_action IN ('tokens_per_user_request_response', 'tokens_per_user_request_prompt')
    AND behavior_at >= '2023-10-01'

),

source_all_feats_except_cs AS (
  SELECT
    behavior_date  AS behavior_date,
    event_action   AS event_action,
    event_label    AS feature,
    event_category AS model,
    event_property AS request_id,
    event_value    AS token_amount,
    gsc_is_gitlab_team_member
  FROM
    mart
  WHERE
    app_id = 'gitlab'
),

source_cs AS (

  SELECT
    behavior_date  AS behavior_date,
    event_action   AS event_action,
    event_label    AS feature,
    event_category AS model,
    event_property AS request_id,
    event_value    AS token_amount,
    gsc_is_gitlab_team_member
  FROM mart
  WHERE
    event_category = 'code_suggestions'
    AND event_label IN ('code_generation', 'code_completion')

),

source AS (

  SELECT * FROM source_all_feats_except_cs
  UNION ALL
  SELECT * FROM source_cs

)


SELECT
  DATE_TRUNC('day', behavior_date) AS day,
  CASE WHEN model LIKE '%Anthropic%' OR LOWER(model) LIKE '%aigateway%' THEN 'Anthropic'
    WHEN feature = 'code_completion' THEN 'Google'
    WHEN feature = 'code_generation' THEN 'Anthropic' -- https://gitlab.com/gitlab-org/quality/engineering-analytics/finops/finops-analysis/-/issues/140#note_1867359025
    ELSE model
  END                              AS provider,
  CASE WHEN feature = 'code_completion' THEN 'Codey for Code Completion'
    WHEN feature = 'code_generation' THEN 'Gitlab::Llm::Anthropic' ELSE model
  END                              AS model,
  feature                          AS feature,
  gsc_is_gitlab_team_member        AS is_gitlab_team_member,
  event_action                     AS prompt_response,
  SUM(token_amount)                AS tokens_tracked,
  SUM(token_amount) * 4            AS characters_tracked
FROM source
GROUP BY 1, 2, 3, 4, 5, 6
