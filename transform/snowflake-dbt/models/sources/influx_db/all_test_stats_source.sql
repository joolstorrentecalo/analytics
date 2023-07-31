WITH source AS (
  
   SELECT *
   FROM {{ source('influx_db','all_test_stats') }}
 
), final AS (
 
    SELECT   
      id::VARCHAR                                      AS all_test_stats_id,
      testcase::VARCHAR                                AS all_test_stats_testcase,
      file_path::VARCHAR                               AS all_test_stats_file_path,
      name::VARCHAR                                    AS all_test_stats_name,
      product_group::VARCHAR                           AS all_test_stats_product_group,
      stage::VARCHAR                                   AS all_test_stats_stage,
      job_id::NUMBER                                   AS job_id,
      job_name::VARCHAR                                AS job_name,
      job_url::VARCHAR                                 AS job_url,
      pipeline_id::NUMBER                              AS pipeline_id,
      pipeline_url::VARCHAR                            AS pipeline_url,
      merge_request::BOOLEAN                           AS is_merge_request,
      merge_request_iid::NUMBER                        AS merge_request_iid,	
      smoke::BOOLEAN                                   AS is_smoke,
      reliable::BOOLEAN                                AS is_reliable,
      quarantined::BOOLEAN	                           AS is_quarantined,
      retried::BOOLEAN	                               AS has_retried,
      retry_attempts::NUMBER                           AS retry_attempts,
      run_time::NUMBER                                 AS run_time,	
      run_type::VARCHAR                                AS run_type,
      status::VARCHAR                                  AS status,
      ui_fabrication::NUMBER                           AS ui_fabrication,
      api_fabrication::NUMBER                          AS api_fabrication,
      total_fabrication::NUMBER                        AS pipeline_total_fabrication
    FROM source
)

SELECT *
FROM final
