SELECT
    o.dim_crm_opportunity_id,
    o.opportunity_name,
    o.competitors,
    CASE
        WHEN (o.competitors_github_enterprise_flag + o.competitors_github_flag) > 0
            THEN 1
        ELSE 0
    END                                                                                   AS competitors_github_flag,
    o.deal_path_name,
    CASE
        WHEN (o.competitors_bitbucket_flag + o.competitors_bitbucket_server_flag) > 0
            THEN 1
        ELSE 0
    END                                                                                   AS competitors_bitbucket_all_flag,
    o.competitors_jenkins_flag,
    CASE
        WHEN (competitors_azure_devops_flag + competitors_azure_flag + competitors_github_enterprise_flag + competitors_github_flag) > 0 THEN 1
        ELSE 0
    END                                                                                   AS competitors_microsoft_flag,
    competitors_atlassian_flag,
    CASE
        WHEN
            competitors_atlassian_flag = 0
            AND competitors_microsoft_flag = 0
            AND competitors IS NOT NULL
            AND competitors <> 'None'
            THEN 1
        ELSE 0
    END                                                                                   AS competition_no_micro_atl,
    CASE
        WHEN
            competitors IS NOT NULL
            AND competitors <> 'None'
            THEN 1
        ELSE 0
    END                                                                                   AS all_competitive_opps,
    CASE
        WHEN
            competitors IS NULL
            OR competitors = 'None'
            THEN 1
        ELSE 0
    END                                                                                   AS no_comp_opps,
    o.net_arr,
    o.growth_type,
    o.stage_name,
    h.crm_current_account_set_area,
    h.crm_current_account_set_geo,
    h.crm_current_account_set_region,
    h.crm_current_account_set_sales_segment,
    h.crm_current_account_set_role_level_1,
    h.crm_current_account_set_role_level_2,
    h.crm_current_account_set_role_level_3,
    h.crm_current_account_set_role_level_4,
    h.crm_current_account_set_role_level_5,
    o.order_type,
    o.order_type_grouped,
    o.created_date,
    o.close_date,
    o.close_fiscal_quarter_name,
    o.close_fiscal_year,
    o.sales_qualified_source_name,
    o.sales_qualified_source_grouped,
    CASE
        WHEN o.net_arr < 0
            THEN 'Churn/Contraction'
        WHEN o.net_arr >= 0 AND o.net_arr <= 25000
            THEN '$0-$25K'
        WHEN o.net_arr > 25000 AND o.net_arr <= 100000
            THEN '$25K-$100K'
        WHEN o.net_arr > 100000 AND o.net_arr <= 1000000
            THEN '$100K-$1M'
        ELSE '>$1M'
    END                                                                                   AS deal_size_group,
    o.reason_for_loss,
    CASE
        WHEN
            order_type IN ('4. Contraction', '5. Churn - Partial', '6. Churn - Final')
            AND military_invasion_risk_scale IS NOT NULL THEN 'Military Invasion'
        WHEN order_type IN ('4. Contraction', '5. Churn - Partial') THEN 'Contraction'
        WHEN order_type = '6. Churn - Final' THEN 'Churn'
    END                                                                                   AS loss_type,
    CASE
        WHEN loss_type = 'Military Invasion' THEN 'Military Invasion'
        WHEN reason_for_loss = 'Corporate Decision' THEN 'Top Down Executive Decision'
        WHEN reason_for_loss IN ('Lack of Engagement / Sponsor', 'Evangelist Left', 'Went Silent') THEN 'Lack of Customer Engagement or Sponsor'
        WHEN reason_for_loss IN ('Budget/Value Unperceived', 'Product Value / Gaps') THEN 'Product Features / Value Gaps'
        WHEN reason_for_loss = 'Other' THEN 'Unknown'
        WHEN reason_for_loss IN ('Insuficient funds', 'Loss of Budget') THEN 'Lack of Budget'
        WHEN reason_for_loss = 'Product quality/availability' THEN 'Product Quality / Availability'
        WHEN order_type IN ('4. Contraction', '5. Churn - Partial', '6. Churn - Final') AND reason_for_loss IS NULL THEN 'Unknown'
        ELSE reason_for_loss
    END                                                                                   AS reason_for_loss_mapped_to_new_codes,
    o.calculated_deal_count,
    CASE WHEN o.is_won = TRUE THEN o.calculated_deal_count END                            AS wondeals_dealsum,
    o.net_arr * o.calculated_deal_count                                                   AS net_arr_final,
    datediff(QUARTER, c.current_first_day_of_fiscal_quarter, o.close_fiscal_quarter_date) AS relative_quarter
FROM "PROD"."RESTRICTED_SAFE_COMMON_MART_SALES"."MART_CRM_OPPORTUNITY" o
LEFT JOIN restricted_safe_workspace_sales.wk_fct_crm_opportunity h
    ON o.dim_crm_opportunity_id = h.dim_crm_opportunity_id
LEFT JOIN common.dim_date c
    ON o.close_date = c.date_actual
WHERE
    o.close_date BETWEEN '2022-02-01' AND current_date
    AND o.is_closed = TRUE
    AND o.is_edu_oss = FALSE
    AND o.is_jihu_account = FALSE
    AND coalesce(o.reason_for_loss, 'null') <> 'Merged into another opportunity'
    AND o.sales_qualified_source_name <> 'Web Direct Generated'
    AND o.parent_crm_account_geo <> 'JIHU'
