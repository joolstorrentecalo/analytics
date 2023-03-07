{{ config({
    "alias": "dim_crm_account"
}) }}

WITH final AS (

    SELECT 
      --primary key
      prep_crm_account.dim_crm_account_id,

      --surrogate keys
      prep_crm_account.dim_parent_crm_account_id,
      prep_crm_account.dim_crm_user_id,
      prep_crm_account.merged_to_account_id,
      prep_crm_account.record_type_id,
      prep_crm_account.master_record_id,
      prep_crm_account.dim_crm_person_primary_contact_id,

      --account people
      prep_crm_account.crm_account_owner,
      prep_crm_account.proposed_crm_account_owner,
      prep_crm_account.account_owner,
      prep_crm_account.technical_account_manager,
      prep_crm_account.owner_role,
      prep_crm_account.user_role_type,

      ----ultimate parent crm account info
      prep_crm_account.parent_crm_account_name,
      prep_crm_account.parent_crm_account_sales_segment,
      prep_crm_account.parent_crm_account_billing_country,
      prep_crm_account.parent_crm_account_billing_country_code,
      prep_crm_account.parent_crm_account_industry,
      prep_crm_account.parent_crm_account_sub_industry,
      prep_crm_account.parent_crm_account_industry_hierarchy,
      prep_crm_account.parent_crm_account_owner_team,
      prep_crm_account.parent_crm_account_sales_territory,
      prep_crm_account.parent_crm_account_tsp_region,
      prep_crm_account.parent_crm_account_tsp_sub_region,
      prep_crm_account.parent_crm_account_tsp_area,
      prep_crm_account.parent_crm_account_gtm_strategy,
      prep_crm_account.parent_crm_account_focus_account,
      prep_crm_account.parent_crm_account_tsp_account_employees,
      prep_crm_account.parent_crm_account_tsp_max_family_employees,
      prep_crm_account.parent_crm_account_employee_count_band,
      prep_crm_account.parent_crm_account_created_date,
      prep_crm_account.parent_crm_account_zi_technologies,
      prep_crm_account.parent_crm_account_zoom_info_website,
      prep_crm_account.parent_crm_account_zoom_info_company_other_domains,
      prep_crm_account.parent_crm_account_zoom_info_dozisf_zi_id,
      prep_crm_account.parent_crm_account_zoom_info_parent_company_zi_id,
      prep_crm_account.parent_crm_account_zoom_info_parent_company_name,
      prep_crm_account.parent_crm_account_zoom_info_ultimate_parent_company_zi_id,
      prep_crm_account.parent_crm_account_zoom_info_ultimate_parent_company_name,
      prep_crm_account.parent_crm_account_demographics_business_unit,
      prep_crm_account.parent_crm_account_demographics_geo,
      prep_crm_account.parent_crm_account_demographics_region,
      prep_crm_account.parent_crm_account_demographics_sales_segment,
      prep_crm_account.parent_crm_account_demographics_area,
      prep_crm_account.parent_crm_account_demographics_territory,
      prep_crm_account.parent_crm_account_demographics_role_type,
      prep_crm_account.parent_crm_account_demographics_max_family_employee,
      prep_crm_account.parent_crm_account_demographics_upa_country,
      prep_crm_account.parent_crm_account_demographics_upa_state,
      prep_crm_account.parent_crm_account_demographics_upa_city,
      prep_crm_account.parent_crm_account_demographics_upa_street,
      prep_crm_account.parent_crm_account_demographics_upa_postal_code,

      --descriptive attributes
      prep_crm_account.crm_account_name,
      prep_crm_account.crm_account_demographics_employee_count,
      prep_crm_account.crm_account_gtm_strategy,
      prep_crm_account.crm_account_focus_account,
      prep_crm_account.crm_account_owner_user_segment,
      prep_crm_account.crm_account_tsp_account_employees,
      prep_crm_account.crm_account_tsp_max_family_employees,
      prep_crm_account.crm_account_billing_country,
      prep_crm_account.crm_account_billing_country_code,
      prep_crm_account.crm_account_type,
      prep_crm_account.crm_account_industry,
      prep_crm_account.crm_account_sub_industry,
      prep_crm_account.crm_account_owner_team,
      prep_crm_account.crm_account_sales_territory,
      prep_crm_account.crm_account_tsp_region,
      prep_crm_account.crm_account_tsp_sub_region,
      prep_crm_account.crm_account_tsp_area,
      prep_crm_account.tsp_max_hierarchy_sales_segment,
      prep_crm_account.crm_account_employee_count_band,
      prep_crm_account.tsp_account_employees,
      prep_crm_account.tsp_max_family_employees,
      prep_crm_account.partner_vat_tax_id,
      prep_crm_account.account_manager,
      prep_crm_account.business_development_rep,
      prep_crm_account.dedicated_service_engineer,
      prep_crm_account.account_tier,
      prep_crm_account.account_tier_notes,
      prep_crm_account.license_utilization,
      prep_crm_account.support_level,
      prep_crm_account.named_account,
      prep_crm_account.billing_postal_code,
      prep_crm_account.partner_type,
      prep_crm_account.partner_status,
      prep_crm_account.gitlab_customer_success_project,
      prep_crm_account.demandbase_account_list,
      prep_crm_account.demandbase_intent,
      prep_crm_account.demandbase_page_views,
      prep_crm_account.demandbase_score,
      prep_crm_account.demandbase_sessions,
      prep_crm_account.demandbase_trending_offsite_intent,
      prep_crm_account.demandbase_trending_onsite_engagement,
      prep_crm_account.is_locally_managed_account,
      prep_crm_account.is_strategic_account,
      prep_crm_account.partner_track,
      prep_crm_account.partners_partner_type,
      prep_crm_account.gitlab_partner_program,
      prep_crm_account.zoom_info_company_name,
      prep_crm_account.zoom_info_company_revenue,
      prep_crm_account.zoom_info_company_employee_count,
      prep_crm_account.zoom_info_company_industry,
      prep_crm_account.zoom_info_company_city,
      prep_crm_account.zoom_info_company_state_province,
      prep_crm_account.zoom_info_company_country,
      prep_crm_account.account_phone,
      prep_crm_account.zoominfo_account_phone,
      prep_crm_account.abm_tier,
      prep_crm_account.health_score,
      prep_crm_account.health_number,
      prep_crm_account.health_score_color,
      prep_crm_account.partner_account_iban_number,
      prep_crm_account.federal_account,
      prep_crm_account.fy22_new_logo_target_list,
      prep_crm_account.gitlab_com_user,
      prep_crm_account.crm_account_zi_technologies,
      prep_crm_account.crm_account_zoom_info_website,
      prep_crm_account.crm_account_zoom_info_company_other_domains,
      prep_crm_account.crm_account_zoom_info_dozisf_zi_id,
      prep_crm_account.crm_account_zoom_info_parent_company_zi_id,
      prep_crm_account.crm_account_zoom_info_parent_company_name,
      prep_crm_account.crm_account_zoom_info_ultimate_parent_company_zi_id,
      prep_crm_account.crm_account_zoom_info_ultimate_parent_company_name,
      prep_crm_account.forbes_2000_rank,
      prep_crm_account.parent_account_industry_hierarchy,
      prep_crm_account.sales_development_rep,
      prep_crm_account.admin_manual_source_number_of_employees,
      prep_crm_account.admin_manual_source_account_address,
      prep_crm_account.eoa_sentiment,
      prep_crm_account.gs_health_user_engagement,
      prep_crm_account.gs_health_cd,
      prep_crm_account.gs_health_devsecops,
      prep_crm_account.gs_health_ci,
      prep_crm_account.gs_health_scm,

      --measures (maintain for now to not break reporting)
      prep_crm_account.parent_crm_account_lam,
      prep_crm_account.parent_crm_account_lam_dev_count,
      prep_crm_account.carr_account_family,
      prep_crm_account.carr_this_account,
      prep_crm_account.potential_arr_lam,

      --degenerative dimensions
      prep_crm_account.is_sdr_target_account,
      prep_crm_account.is_key_account,
      prep_crm_account.is_reseller,
      prep_crm_account.is_jihu_account,
      prep_crm_account.is_first_order_available,
      prep_crm_account.is_zi_jenkins_present,
      prep_crm_account.is_zi_svn_present,
      prep_crm_account.is_zi_tortoise_svn_present,
      prep_crm_account.is_zi_gcp_present,
      prep_crm_account.is_zi_atlassian_present,
      prep_crm_account.is_zi_github_present,
      prep_crm_account.is_zi_github_enterprise_present,
      prep_crm_account.is_zi_aws_present,
      prep_crm_account.is_zi_kubernetes_present,
      prep_crm_account.is_zi_apache_subversion_present,
      prep_crm_account.is_zi_apache_subversion_svn_present,
      prep_crm_account.is_zi_hashicorp_present,
      prep_crm_account.is_zi_aws_cloud_trail_present,
      prep_crm_account.is_zi_circle_ci_present,
      prep_crm_account.is_zi_bit_bucket_present,
      prep_crm_account.is_excluded_from_zoom_info_enrich,

      --dates
      prep_crm_account.crm_account_created_date,
      prep_crm_account.abm_tier_1_date,
      prep_crm_account.abm_tier_2_date,
      prep_crm_account.abm_tier_3_date,
      prep_crm_account.gtm_acceleration_date,
      prep_crm_account.gtm_account_based_date,
      prep_crm_account.gtm_account_centric_date,
      prep_crm_account.partners_signed_contract_date,
      prep_crm_account.technical_account_manager_date,
      prep_crm_account.customer_since_date,
      prep_crm_account.next_renewal_date,
      prep_crm_account.gs_first_value_date,
      prep_crm_account.gs_last_csm_activity_date,

      --metadata
      prep_crm_account.created_by_name,
      prep_crm_account.last_modified_by_name,
      prep_crm_account.last_modified_date,
      prep_crm_account.last_activity_date,
      prep_crm_account.is_deleted,
      prep_crm_account.pte_score,
      prep_crm_account.pte_decile,
      prep_crm_account.pte_score_group,
      prep_crm_account.ptc_score,
      prep_crm_account.ptc_decile,
      prep_crm_account.ptc_score_group
    FROM {{ ref('prep_crm_account') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@michellecooper",
    created_date="2020-06-01",
    updated_date="2023-02-21"
) }}


