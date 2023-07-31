WITH source AS (
  
   SELECT *
   FROM {{ source('influx_db','all_fabrication_stats') }}
 
), final AS (
 
    SELECT   
      timestamp::TIMESTAMP                  AS timestamp,
      resource::VARCHAR                     AS resource,
      fabrication_method::VARCHAR           AS fabrication_method,
      http_method::VARCHAR                  AS http_method,
      run_type::VARCHAR                     AS run_type,
      merge_request::VARCHAR                AS merge_request,
      fabrication_time::FLOAT               AS fabrication_time,
      info::VARCHAR                         AS info,
      job_url::VARCHAR                      AS job_url
    FROM source
)

SELECT *
FROM final
