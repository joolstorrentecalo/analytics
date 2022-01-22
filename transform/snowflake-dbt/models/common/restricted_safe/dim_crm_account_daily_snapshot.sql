WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), sfdc_account_base AS (

    SELECT *
    FROM {{ ref('sfdc_account_snapshots_source') }}
    WHERE account_id IS NOT NULL

), sfdc_account AS (

    SELECT
      {{ dbt_utils.surrogate_key(['sfdc_account_base.account_id','snapshot_dates.date_id'])}}     AS crm_account_snapshot_id,
      snapshot_dates.date_id                                                                      AS snapshot_date_id,
      sfdc_account_base.*
    FROM sfdc_account_base
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= sfdc_account_base.dbt_valid_from
      AND snapshot_dates.date_actual < COALESCE(sfdc_account_base.dbt_valid_to, '9999-12-31'::TIMESTAMP)

), sfdc_users_base AS (

    SELECT *
    FROM {{ ref('sfdc_user_snapshots_source') }}

), sfdc_users AS (

    SELECT
      {{ dbt_utils.surrogate_key(['sfdc_users_base.user_id','snapshot_dates.date_id'])}}          AS crm_user_snapshot_id,
      snapshot_dates.date_id                                                                      AS snapshot_date_id,
      sfdc_users_base.*
    FROM sfdc_users_base
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= sfdc_users_base.dbt_valid_from
      AND snapshot_dates.date_actual < COALESCE( sfdc_users_base.dbt_valid_to, '9999-12-31'::TIMESTAMP)

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), ultimate_parent_account AS (

    SELECT
      crm_account_snapshot_id,
      snapshot_date_id,
      account_id,
      account_name,
      billing_country,
      df_industry,
      industry,
      sub_industry,
      account_owner_team,
      tsp_territory,
      tsp_region,
      tsp_sub_region,
      tsp_area,
      gtm_strategy,
      tsp_account_employees,
      tsp_max_family_employees,
      account_demographics_sales_segment,
      account_demographics_geo,
      account_demographics_region,
      account_demographics_area,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      created_date,
      zi_technologies,
      zoom_info_website,
      zoom_info_company_other_domains,
      zoom_info_dozisf_zi_id,
      zoom_info_parent_company_zi_id,
      zoom_info_parent_company_name,
      zoom_info_ultimate_parent_company_zi_id,
      zoom_info_ultimate_parent_company_name
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), final AS (

    SELECT
      --crm account informtion
      sfdc_account.crm_account_snapshot_id,
      sfdc_account.snapshot_date_id,
      sfdc_account.owner_id                                               AS dim_crm_user_id,
      sfdc_account.account_id                                             AS dim_crm_account_id,
      sfdc_account.account_name                                           AS crm_account_name,
      sfdc_account.billing_country                                        AS crm_account_billing_country,
      sfdc_account.account_type                                           AS crm_account_type,
      sfdc_account.industry                                               AS crm_account_industry,
      sfdc_account.sub_industry                                           AS crm_account_sub_industry,
      sfdc_account.account_owner                                          AS crm_account_owner,
      sfdc_account.account_owner_team                                     AS crm_account_owner_team,
      sfdc_account.tsp_territory                                          AS crm_account_sales_territory,
      sfdc_account.tsp_region                                             AS crm_account_tsp_region,
      sfdc_account.tsp_sub_region                                         AS crm_account_tsp_sub_region,
      sfdc_account.tsp_area                                               AS crm_account_tsp_area,
      sfdc_account.account_demographics_sales_segment                     AS parent_crm_account_demographics_sales_segment,
      sfdc_account.account_demographics_geo                               AS parent_crm_account_demographics_geo,
      sfdc_account.account_demographics_region                            AS parent_crm_account_demographics_region,
      sfdc_account.account_demographics_area                              AS parent_crm_account_demographics_area,
      sfdc_account.account_demographics_territory                         AS parent_crm_account_demographics_territory,
      sfdc_account.account_demographics_employee_count                    AS crm_account_demographics_employee_count,
      sfdc_account.account_demographics_max_family_employee               AS parent_crm_account_demographics_max_family_employee,
      sfdc_account.account_demographics_upa_country                       AS parent_crm_account_demographics_upa_country,
      sfdc_account.account_demographics_upa_state                         AS parent_crm_account_demographics_upa_state,
      sfdc_account.account_demographics_upa_city                          AS parent_crm_account_demographics_upa_city,
      sfdc_account.account_demographics_upa_street                        AS parent_crm_account_demographics_upa_street,
      sfdc_account.account_demographics_upa_postal_code                   AS parent_crm_account_demographics_upa_postal_code,
      sfdc_account.gtm_strategy                                           AS crm_account_gtm_strategy,
      sfdc_account.account_owner_user_segment                             AS crm_account_owner_user_segment,
      sfdc_account.tsp_account_employees                                  AS crm_account_tsp_account_employees,
      sfdc_account.tsp_max_family_employees                               AS crm_account_tsp_max_family_employees,
      sfdc_account.health_score,
      sfdc_account.health_number,
      sfdc_account.health_score_color,
      sfdc_account.partner_account_iban_number,
      CAST(sfdc_account.partners_signed_contract_date AS date)            AS partners_signed_contract_date,
      sfdc_account.record_type_id                                         AS record_type_id,
      sfdc_account.federal_account                                        AS federal_account,
      sfdc_account.is_jihu_account                                        AS is_jihu_account,
      sfdc_account.carr_this_account,
      sfdc_account.carr_total,
      sfdc_account.potential_arr_lam,
      sfdc_account.fy22_new_logo_target_list,
      sfdc_account.is_first_order_available,
      sfdc_account.gitlab_com_user,
      sfdc_account.tsp_account_employees,
      sfdc_account.tsp_max_family_employees,
      account_owner.name                                                  AS account_owner,
      sfdc_users.name                                                     AS technical_account_manager,
      sfdc_account.is_deleted                                             AS is_deleted,
      map_merged_crm_account.dim_crm_account_id                           AS merged_to_account_id,
      sfdc_account.created_date                                           AS crm_account_created_date,
      sfdc_account.zi_technologies                                        AS crm_account_zi_technologies,
      sfdc_account.technical_account_manager_date,
      sfdc_account.zoom_info_website                                      AS crm_account_zoom_info_website,
      sfdc_account.zoom_info_company_other_domains                        AS crm_account_zoom_info_company_other_domains,
      sfdc_account.zoom_info_dozisf_zi_id                                 AS crm_account_zoom_info_dozisf_zi_id,
      sfdc_account.zoom_info_parent_company_zi_id                         AS crm_account_zoom_info_parent_company_zi_id,
      sfdc_account.zoom_info_parent_company_name                          AS crm_account_zoom_info_parent_company_name,
      sfdc_account.zoom_info_ultimate_parent_company_zi_id                AS crm_account_zoom_info_ultimate_parent_company_zi_id,
      sfdc_account.zoom_info_ultimate_parent_company_name                 AS crm_account_zoom_info_ultimate_parent_company_name,
  
      ----ultimate parent crm account info
      ultimate_parent_account.account_id                                  AS dim_parent_crm_account_id,
      ultimate_parent_account.account_name                                AS parent_crm_account_name,
      {{ sales_segment_cleaning('sfdc_account.ultimate_parent_sales_segment') }}
                                                                          AS parent_crm_account_sales_segment,
      ultimate_parent_account.billing_country                             AS parent_crm_account_billing_country,
      ultimate_parent_account.industry                                    AS parent_crm_account_industry,
      ultimate_parent_account.sub_industry                                AS parent_crm_account_sub_industry,
      ultimate_parent_account.account_owner_team                          AS parent_crm_account_owner_team,
      ultimate_parent_account.tsp_territory                               AS parent_crm_account_sales_territory,
      ultimate_parent_account.tsp_region                                  AS parent_crm_account_tsp_region,
      ultimate_parent_account.tsp_sub_region                              AS parent_crm_account_tsp_sub_region,
      ultimate_parent_account.tsp_area                                    AS parent_crm_account_tsp_area,
      ultimate_parent_account.gtm_strategy                                AS parent_crm_account_gtm_strategy,
      ultimate_parent_account.tsp_account_employees                       AS parent_crm_account_tsp_account_employees,
      ultimate_parent_account.tsp_max_family_employees                    AS parent_crm_account_tsp_max_family_employees,
      ultimate_parent_account.created_date                                AS parent_crm_account_created_date,
      ultimate_parent_account.zi_technologies                             AS parent_crm_account_zi_technologies,
      ultimate_parent_account.zoom_info_website                           AS parent_crm_account_zoom_info_website,
      ultimate_parent_account.zoom_info_company_other_domains             AS parent_crm_account_zoom_info_company_other_domains,
      ultimate_parent_account.zoom_info_dozisf_zi_id                      AS parent_crm_account_zoom_info_dozisf_zi_id,
      ultimate_parent_account.zoom_info_parent_company_zi_id              AS parent_crm_account_zoom_info_parent_company_zi_id,
      ultimate_parent_account.zoom_info_parent_company_name               AS parent_crm_account_zoom_info_parent_company_name,
      ultimate_parent_account.zoom_info_ultimate_parent_company_zi_id     AS parent_crm_account_zoom_info_ultimate_parent_company_zi_id,
      ultimate_parent_account.zoom_info_ultimate_parent_company_name      AS parent_crm_account_zoom_info_ultimate_parent_company_name,
      {{ sfdc_account_fields() }}
  
    FROM sfdc_account
    LEFT JOIN map_merged_crm_account
      ON sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN ultimate_parent_account
      ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id
        AND sfdc_account.snapshot_date_id = ultimate_parent_account.snapshot_date_id
    LEFT OUTER JOIN sfdc_users
      ON sfdc_account.technical_account_manager_id = sfdc_users.user_id
        AND sfdc_account.snapshot_date_id = sfdc_users.snapshot_date_id
    LEFT JOIN sfdc_users AS account_owner
      ON account_owner.user_id = sfdc_account.owner_id
        AND account_owner.snapshot_date_id = sfdc_account.snapshot_date_id
    LEFT JOIN sfdc_record_type
      ON sfdc_account.record_type_id = sfdc_record_type.record_type_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2022-01-20"
) }}
