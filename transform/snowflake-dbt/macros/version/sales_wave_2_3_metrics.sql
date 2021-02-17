{% macro sales_wave_2_3_metrics() %} 

    -- usage ping data - devops metrics
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['events']") }}                                        AS umau_28_days_user,        
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['action_monthly_active_users_project_repo']") }}      AS action_monthly_active_users_project_repo_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['merge_requests']") }}                                AS merge_requests_28_days_user,           
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['projects_with_repositories_enabled']") }} AS projects_with_repositories_enabled_28_days_user,       
    {{ null_negative_numbers("raw_usage_data_payload['counts']['commit_comment']") }}                                                                   AS commit_comment_all_time_event,    
    {{ null_negative_numbers("raw_usage_data_payload['counts']['source_code_pushes']") }}                                                               AS source_code_pushes_all_time_event,     
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipelines']") }}                                  AS ci_pipelines_28_days_user,           
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_internal_pipelines']") }}                         AS ci_internal_pipelines_28_days_user,      
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['ci_builds']") }}                                     AS ci_builds_28_days_user,            
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['create']['ci_builds']") }}                                             AS ci_builds_all_time_user,                            
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_builds']") }}                                                                        AS ci_builds_all_time_event,         
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_runners']") }}                                                                       AS ci_runners_all_time_event,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['counts']['auto_devops_enabled']") }}                                                   AS auto_devops_enable_all_time_event,                  
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['gitlab_shared_runners_enabled']") }}                                                   AS gitlab_shared_runners_enabled_instance_setting,     
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['container_registry_enabled']") }}                                                      AS container_registry_enabled_instance_setting,   
    {{ null_negative_numbers("raw_usage_data_payload['counts']['template_repositories']") }}                                                            AS template_repositories_all_time_event,     
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['ci_pipeline_config_repository']") }}                 AS ci_pipeline_config_repository_28_days_user,         
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_unique_users_all_secure_scanners']") }}         AS user_unique_users_all_secure_scanners_28_days_user,     
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_sast_jobs']") }}                                AS user_sast_jobs_28_days_user,       
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dast_jobs']") }}                                AS user_dast_jobs_28_days_user,   
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dependency_scanning_jobs']") }}                 AS user_dependency_scanning_jobs_28_days_user,       
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_license_management_jobs']") }}                  AS user_license_management_jobs_28_days_user,         
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_secret_detection_jobs']") }}                    AS user_secret_detection_jobs_28_days_user,          
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_container_scanning_jobs']") }}                  AS user_container_scanning_job_28_days_user,       
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['object_store']['packages']['enabled']") }}                                             AS object_store_packages_enabled_instance_setting,       
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_with_packages']") }}                                                           AS projects_with_packages_all_time_event,             
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['package']['projects_with_packages']") }}                       AS projects_with_packages_28_days_user,  
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['deployments']") }}                                  AS deployments_28_days_user,       
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['releases']") }}                                     AS releases_28_days_user,              
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['epics']") }}                                           AS epics_28_days_user,   
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['issues']") }}                                          AS issues_28_days_user          

{%- endmacro -%}
