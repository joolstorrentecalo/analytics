WITH base AS(
    SELECT 
    dim_crm_opportunity_id,
    dim_parent_crm_account_id,
    report_role_level_1,
    report_role_level_2,
    report_role_level_3,
    CASE
        WHEN report_role_level_1 = 'APJ' THEN 'APJ'
        WHEN report_role_level_1 = 'SMB' THEN 'SMB'
        WHEN report_role_level_1 = 'PUBSEC' THEN 'PUBSEC'
        WHEN report_role_level_2 = 'AMER_COMM' THEN 'AMER COMM'
        WHEN report_role_level_1 = 'AMER' THEN 'AMER ENT'
        WHEN report_role_level_2 = 'EMEA_COMM' THEN 'EMEA COMM'
        WHEN report_role_level_2 = 'EMEA_NEUR' THEN 'EMEA NEUR'
        WHEN report_role_level_2 = 'EMEA_DACH' THEN 'EMEA DACH'
        WHEN report_role_level_2 = 'EMEA_SEUR' THEN 'EMEA SEUR'
        WHEN report_role_level_2 = 'EMEA_META' THEN 'EMEA META'
        WHEN report_role_level_2 = 'EMEA_TELCO' THEN 'EMEA TELCO'
    END AS pipe_council_grouping,                                   -- replace with upstream column
    sales_type,
    intended_product_tier,
    order_type,
    sales_qualified_source_name,
    close_date,
    close_fiscal_quarter_name,
    stage_name,
    CASE
        WHEN net_arr<50000 THEN 'Run-Rate Net ARR(<$50K)'
        WHEN net_arr>=50000 AND net_arr<250000 THEN 'Mid Size Net ARR ($50K-$250K)'
        WHEN net_arr>=250000 AND net_arr<500000 THEN 'Fat-Middle Net ARR ($250K-$500K)'
        WHEN net_arr>=500000 AND net_arr<1000000 THEN 'Big Deal Net ARR ($500K-$1M)'
        ELSE 'Jumbo Deal Net ARR (>$1M)'
    END AS deal_size_grouping,                                      -- replace with upstream column
    SUM(CASE WHEN stage_name IN ('1-Discovery', '2-Scoping','3-Technical Evaluation','4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing') 
        THEN net_arr END) AS open_pipe_1_plus,
    SUM(CASE WHEN stage_name IN ('3-Technical Evaluation','4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing') 
        THEN net_arr END) AS open_pipe_3_plus,
    SUM(CASE WHEN stage_name='Closed Won' 
        THEN net_arr END) AS total_won_net_arr,
    COUNT(CASE WHEN stage_name='Closed Won' 
        THEN dim_crm_opportunity_id END) AS total_won_deals,
    SUM(CASE WHEN stage_name='8-Closed Lost' 
        THEN net_arr END) AS lost_pipe,
    SUM(CASE WHEN stage_name NOT LIKE '%Closed%' 
        THEN COALESCE(proserv_amount,professional_services_value) END) AS ps_amnt_open,
    SUM(CASE WHEN stage_name='Closed Won' 
        THEN COALESCE(proserv_amount,professional_services_value) END) AS ps_amnt_won,
    SUM(CASE WHEN stage_name NOT LIKE '%Closed%'
        THEN net_arr END) AS total_open_net_arr,
    COUNT(CASE WHEN stage_name NOT LIKE '%Closed%'
        THEN dim_crm_opportunity_id END) AS total_open_deals,
    FROM prod.restricted_safe_common_mart_sales.mart_crm_opportunity
    WHERE close_fiscal_quarter_date='2024-08-01'
        AND sales_qualified_source_name<>'Web Direct Generated'
        AND net_arr>0
        AND opportunity_category NOT IN ('Decommission','Internal Correction')
        AND lower(opportunity_name) NOT LIKE '%rebook%'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
),

current_qtr_targets AS(
    SELECT crm_user_role_level_3,
    CASE
        WHEN prep_crm_user_hierarchy.crm_user_role_level_1 = 'APJ' THEN 'APJ'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_1 = 'SMB' THEN 'SMB'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_1 = 'PUBSEC' THEN 'PUBSEC'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'AMER_COMM' THEN 'AMER COMM'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_1 = 'AMER' THEN 'AMER ENT'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_COMM' THEN 'EMEA COMM'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_NEUR' THEN 'EMEA NEUR'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_DACH' THEN 'EMEA DACH'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_SEUR' THEN 'EMEA SEUR'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_META' THEN 'EMEA META'
        WHEN prep_crm_user_hierarchy.crm_user_role_level_2 = 'EMEA_TELCO' THEN 'EMEA TELCO'
    END AS pipe_council_grouping,                                     -- replace with upstream column
    SUM(CASE WHEN fct_sales_funnel_target.kpi_name = 'Net ARR'
        THEN fct_sales_funnel_target.allocated_target END) AS net_arr_target,
    FROM prod.restricted_safe_common_mart_sales.mart_sales_funnel_target
    LEFT JOIN prod.common.dim_date 
        ON restricted_safe_common_mart_sales.mart_sales_funnel_target.target_month = dim_date.date_actual
    LEFT JOIN prod.restricted_safe_common.fct_sales_funnel_target
        ON restricted_safe_common_mart_sales.mart_sales_funnel_target.sales_funnel_target_id = fct_sales_funnel_target.sales_funnel_target_id
    LEFT JOIN prod.common_prep.prep_crm_user_hierarchy
        ON fct_sales_funnel_target.dim_crm_user_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    WHERE dim_date.day_of_month = 1
        --AND pipe_council_grouping IS NOT NULL
        AND mart_sales_funnel_target.kpi_name='Net ARR' 
        AND fiscal_quarter_name_fy='FY25-Q3'
        AND sales_qualified_source_name<>'Web Direct Generated'
    GROUP BY 1,2
)

SELECT DISTINCT
b.*,c.net_arr_target,
a.parent_crm_account_industry,
a.parent_crm_account_sales_segment,
a.parent_crm_account_upa_country_name
FROM base b
LEFT JOIN current_qtr_targets c
    ON c.pipe_council_grouping=b.pipe_council_grouping AND c.crm_user_role_level_3=b.report_role_level_3
LEFT JOIN prod.restricted_safe_common_mart_sales.mart_crm_account a
    ON a.dim_parent_crm_account_id=b.dim_parent_crm_account_id
