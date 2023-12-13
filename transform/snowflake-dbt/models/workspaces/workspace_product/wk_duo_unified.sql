
{{
  config(
    materialized='incremental',
    unique_key='request_id',
    tags=["mnpi_exception", "product"]
  )
}}

WITH
prep_limit AS 
(

SELECT
*
FROM {{ ref('mart_behavior_structured_event') }} e 
WHERE 
(
e.event_label = 'chat'
OR
e.event_label = 'gitlab_duo_chat_answer'

)
AND
e.behavior_date > '2023-03-01'
), mapper AS 
(
        SELECT
        e.event_property AS request_id,
        MAX(CASE WHEN e.event_action = 'submit_gitlab_duo_question' THEN e.page_url_path ELSE '' END) AS url,
        MAX(CASE WHEN e.event_action = 'process_gitlab_duo_question' AND e.event_value = 1 THEN 'Successful' ELSE 'Failure' END) AS successful_process_submit_event
        --,MAX(CASE WHEN e.event_action = 'process_gitlab_duo_question' AND e.event_label IS NOT NULL THEN e.event_label ELSE NULL END) AS process_gl_q_tool
        FROM {{ ref('mart_behavior_structured_event') }} e 
        WHERE
        (
        e.event_action = 'process_gitlab_duo_question' 
        OR
        e.event_action = 'submit_gitlab_duo_question'
        )
        AND
        e.behavior_date > '2023-03-01'
        GROUP BY 1
), prep AS
        (
        SELECT 
        event_property AS request_id,
        MAX(CASE WHEN e.event_label = 'gitlab_duo_chat_answer' AND e.event_action = 'error_answer' THEN 1 ELSE 0 END) AS error_answer,
        SUM(CASE WHEN e.event_label = 'gitlab_duo_chat_answer' AND e.event_action = 'error_answer' THEN 1 ELSE 0 END) AS error_answer_sum,
        MAX(e.gsc_pseudonymized_user_id) AS user_id,
        MAX(CASE WHEN (PARSE_JSON(e.contexts:data[0]:data)['is_gitlab_team_member'] = 'e08c592bd39b012f7c83bbc0247311b238ee1caa61be28ccfd412497290f896a'
            OR 
            PARSE_JSON(e.contexts:data[0]:data)['is_gitlab_team_member'] = FALSE
            OR
            PARSE_JSON(e.contexts:data[0]:data)['is_gitlab_team_member'] = 'false'
            ) THEN 0 ELSE 1 END) AS internal_gitlab,
        SUM(CASE WHEN e.event_action = 'tokens_per_user_request_prompt' THEN e.event_value ELSE 0 END) AS tokens_per_prompt,
        SUM(CASE WHEN e.event_action = 'tokens_per_user_request_response' THEN e.event_value ELSE 0 END) AS tokens_per_response,
        MIN(e.behavior_at) AS first_event,
        MAX(e.behavior_at) AS last_event

        FROM
        prep_limit e
        WHERE 
        event_action != 'execute_llm_method'
        GROUP BY 
        1
), final AS (
        SELECT
        p.*,
        m.url,
        m.successful_process_submit_event,
        DATEDIFF(SECOND,p.first_event,p.last_event) AS seconds_between_events
        FROM
        prep p 
        LEFT JOIN 
        mapper m ON m.request_id = p.request_id 
        {% if is_incremental() %}
        LEFT JOIN 
        {{ this }} t ON t.request_id = p.request_id
        WHERE
        t.request_id IS NULL
    
        {% endif %}

        )

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-12-13",
    updated_date="2023-12-13"
) }}
