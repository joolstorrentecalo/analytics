WITH source AS (

    SELECT *
    FROM {{ source('thanos', 'periodic_queries') }}

), parsed AS (

    SELECT
        pq_2.value['metric']['product_stage']::VARCHAR                  AS metric_product_stage,
        pq_2.value['metric']['stage_group']::VARCHAR                    AS metric_stage_group,
        parse_json(pq_2.value:value[0]::DOUBLE)::int::timestamp_ntz     AS metric_created_at,
        NULLIF(pq_2.value:value[1],'NaN')::DOUBLE                       AS metric_value,
        pq_1.value['data']['resultType']::VARCHAR                       AS result_type,
        pq_1.value['status']:: VARCHAR                                  AS status_type,
        pq_1.this['message']:: VARCHAR                                  AS message_type,
        pq_1.this['status_code']:: VARCHAR                              AS status_code,
        pq_1.this['success']:: VARCHAR                                  AS success_status
  FROM
    source pq
  , lateral flatten(input => pq.jsontext['stage_group_error_budget_seconds_remaining']) pq_1
  , lateral flatten(input => pq.jsontext['stage_group_error_budget_seconds_remaining']['body']['data']['result'],outer => true) pq_2
WHERE result_type iS NOT NULL AND status_type IS NOT NULL

)
SELECT * FROM
parsed
