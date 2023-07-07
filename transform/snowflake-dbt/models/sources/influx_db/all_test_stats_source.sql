WITH source AS (
  
   SELECT *
   FROM {{ source('influx_db','all_test_stats') }}
 
), final AS (
 
    SELECT   
      id::TEXT                                      AS id,
      testcase::TEXT                                AS testcase,
      file_path::TEXT                               AS file_path,
      name::TEXT                                    AS name,
      product_group::TEXT                           AS product_group,
      stage::TEXT                                   AS stage,
      job_id::NUMBER                                AS job_id,
      job_name::TEXT                                AS job_name,
      job_url::TEXT                                 AS job_url,
      pipeline_id::NUMBER                           AS pipeline_id,
      pipeline_url::TEXT                            AS pipeline_url,
      merge_request::BOOLEAN                        AS merge_request,
      merge_request_iid::NUMBER                     AS merge_request_iid,	
      smoke::BOOLEAN                                AS smoke,
      reliable::BOOLEAN                             AS reliable,
      quarantined::BOOLEAN	                        AS quarantined,
      retried::BOOLEAN	                            AS retried,
      retry_attempts::NUMBER                        AS retry_attempts,
      run_time::NUMBER                              AS run_time,	
      run_type::TEXT                                AS run_type,
      status::TEXT                                  AS status,
      ui_fabrication::NUMBER                        AS ui_fabrication,
      api_fabrication::NUMBER                       AS api_fabrication,
      total_fabrication::NUMBER                     AS pipeline_total_fabrication
    FROM source
)

SELECT *
FROM final
