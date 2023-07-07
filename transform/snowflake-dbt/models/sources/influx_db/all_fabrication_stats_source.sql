WITH source AS (
  
   SELECT *
   FROM {{ source('influx_db','all_fabrication_stats') }}
 
), final AS (
 
    SELECT   
      custKey::NUMBER                    AS custKey,
      resource::TEXT                     AS resource,
      fabrication_method::TEXT           AS fabrication_method,
      http_method::TEXT                  AS http_method,
      run_type::TEXT                     AS run_type,
      merge_request::TEXT                AS merge_request,
      fabrication_time::FLOAT            AS fabrication_time,
      info::TEXT                         AS info,
      job_url::TEXT                      AS job_url
    FROM source
)

SELECT *
FROM final
