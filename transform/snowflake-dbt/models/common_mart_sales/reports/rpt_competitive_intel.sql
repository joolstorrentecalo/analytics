SELECT
    opportunity.dim_crm_opportunity_id,
    opportunity.opportunity_name,
    opportunity.competitors,
    coalesce((opportunity.competitors_github_enterprise_flag + opportunity.competitors_github_flag) > 0, FALSE)                                                                                  AS competitors_github_flag,
    opportunity.deal_path_name,
    coalesce((opportunity.competitors_bitbucket_flag + opportunity.competitors_bitbucket_server_flag) > 0, FALSE)                                                                                AS competitors_bitbucket_all_flag,
    opportunity.competitors_jenkins_flag,
    coalesce((opportunity.competitors_azure_devops_flag + opportunity.competitors_azure_flag + opportunity.competitors_github_enterprise_flag + opportunity.competitors_github_flag) > 0, FALSE) AS competitors_microsoft_flag,
    opportunity.competitors_atlassian_flag,
    coalesce(
        opportunity.competitors_atlassian_flag = 0
        AND opportunity.competitors_microsoft_flag = 0
        AND opportunity.competitors IS NOT NULL
        AND opportunity.competitors <> 'None', FALSE
    )                                                                                                                                                                                            AS competition_no_micro_atl,
    coalesce(
        opportunity.competitors IS NOT NULL
        AND opportunity.competitors <> 'None', FALSE
    )                                                                                                                                                                                            AS all_competitive_opps,
    coalesce(
        opportunity.competitors IS NULL
        OR opportunity.competitors = 'None', FALSE
    )                                                                                                                                                                                            AS no_comp_opps,
    opportunity.net_arr,
    opportunity.growth_type,
    opportunity.stage_name,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_area_stamped ELSE opportunity.crm_account_user_area
    END                                                                                                                                                                                          AS area,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_geo_stamped ELSE opportunity.crm_account_user_geo
    END                                                                                                                                                                                          AS geo,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_region_stamped ELSE opportunity.crm_account_user_region
    END                                                                                                                                                                                          AS region,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_sales_segment_stamped ELSE opportunity.crm_account_user_sales_segment
    END                                                                                                                                                                                          AS segment,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_role_level_1 ELSE opportunity.crm_account_user_role_level_1
    END                                                                                                                                                                                          AS role_level_1,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_role_level_2 ELSE opportunity.crm_account_user_role_level_2
    END                                                                                                                                                                                          AS role_level_2,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_role_level_3 ELSE opportunity.crm_account_user_role_level_3
    END                                                                                                                                                                                          AS role_level_3,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_role_level_4 ELSE opportunity.crm_account_user_role_level_4
    END                                                                                                                                                                                          AS role_level_4,
    CASE
        WHEN opportunity.close_fiscal_year = close.current_fiscal_year THEN opportunity.crm_opp_owner_role_level_5 ELSE opportunity.crm_account_user_role_level_5
    END                                                                                                                                                                                          AS role_level_5,
    opportunity.order_type,
    opportunity.order_type_grouped,
    opportunity.created_date,
    opportunity.close_date,
    opportunity.close_fiscal_quarter_name,
    opportunity.close_fiscal_year,
    opportunity.sales_qualified_source_name,
    opportunity.sales_qualified_source_grouped,
    CASE
        WHEN opportunity.net_arr < 0
            THEN 'Churn/Contraction'
        WHEN opportunity.net_arr >= 0 AND opportunity.net_arr <= 25000
            THEN '$0-$25K'
        WHEN opportunity.net_arr > 25000 AND opportunity.net_arr <= 100000
            THEN '$25K-$100K'
        WHEN opportunity.net_arr > 100000 AND opportunity.net_arr <= 1000000
            THEN '$100K-$1M'
        ELSE '>$1M'
    END                                                                                                                                                                                          AS deal_size_group,
    opportunity.reason_for_loss,
    CASE
        WHEN
            opportunity.order_type IN ('4. Contraction', '5. Churn - Partial', '6. Churn - Final')
            AND opportunity.military_invasion_risk_scale IS NOT NULL THEN 'Military Invasion'
        WHEN opportunity.order_type IN ('4. Contraction', '5. Churn - Partial') THEN 'Contraction'
        WHEN opportunity.order_type = '6. Churn - Final' THEN 'Churn'
    END                                                                                                                                                                                          AS loss_type,
    CASE
        WHEN opportunity.loss_type = 'Military Invasion' THEN 'Military Invasion'
        WHEN opportunity.reason_for_loss = 'Corporate Decision' THEN 'Top Down Executive Decision'
        WHEN opportunity.reason_for_loss IN ('Lack of Engagement / Sponsor', 'Evangelist Left', 'Went Silent') THEN 'Lack of Customer Engagement or Sponsor'
        WHEN opportunity.reason_for_loss IN ('Budget/Value Unperceived', 'Product Value / Gaps') THEN 'Product Features / Value Gaps'
        WHEN opportunity.reason_for_loss = 'Other' THEN 'Unknown'
        WHEN opportunity.reason_for_loss IN ('Insuficient funds', 'Loss of Budget') THEN 'Lack of Budget'
        WHEN opportunity.reason_for_loss = 'Product quality/availability' THEN 'Product Quality / Availability'
        WHEN opportunity.order_type IN ('4. Contraction', '5. Churn - Partial', '6. Churn - Final') AND opportunity.reason_for_loss IS NULL THEN 'Unknown'
        ELSE opportunity.reason_for_loss
    END                                                                                                                                                                                          AS reason_for_loss_mapped_to_new_codes,
    opportunity.calculated_deal_count,
    CASE WHEN opportunity.is_won = TRUE THEN opportunity.calculated_deal_count END                                                                                                               AS wondeals_dealsum,
    opportunity.net_arr * opportunity.calculated_deal_count                                                                                                                                      AS net_arr_final,
    datediff(QUARTER, close.current_first_day_of_fiscal_quarter, opportunity.close_fiscal_quarter_date)                                                                                          AS relative_quarter
FROM "PROD"."RESTRICTED_SAFE_COMMON_MART_SALES"."MART_CRM_OPPORTUNITY" opportunity
LEFT JOIN common.dim_date close
    ON opportunity.close_date = close.date_actual
WHERE
    opportunity.close_date BETWEEN '2022-02-01' AND current_date
    AND opportunity.is_closed = TRUE
    AND opportunity.is_edu_oss = FALSE
    AND opportunity.is_jihu_account = FALSE
    AND coalesce(opportunity.reason_for_loss, 'null') <> 'Merged into another opportunity'
    AND opportunity.sales_qualified_source_name <> 'Web Direct Generated'
    AND opportunity.parent_crm_account_geo <> 'JIHU'
