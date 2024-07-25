WITH gdpr_delete_logs AS (

    SELECT *
    FROM {{ ref('dbt_gdpr_logs_source') }}

), results AS (

    SELECT
      time_stamp,
      info_msg,
      invocation_id
    FROM gdpr_delete_logs

)

SELECT *
FROM results
