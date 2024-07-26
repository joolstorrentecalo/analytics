{{ config(
    materialized='table'
) }}

WITH l2r_base AS (

    SELECT DISTINCT
        dim_crm_person_id,
        sfdc_record_id,
        email_hash,
        sfdc_record_type,
        person_first_country,
        source_buckets,
        status AS person_status,
        lead_source,
        account_demographics_sales_segment,
        account_demographics_region,
        account_demographics_geo,
        account_demographics_area,
        account_demographics_upa_country,
        account_demographics_territory,
        person_order_type,
        is_mql,
        dim_crm_opportunity_id,
        opp_order_type,
        sales_qualified_source_name,
        sdr_or_bdr,
        is_won,
        valid_deal_count,
        is_sao,
        net_arr,
        is_net_arr_closed_deal,
        is_net_arr_pipeline_created,
        is_eligible_age_analysis,
        report_segment,
        report_geo,
        report_region,
        report_area,
        report_role_name,
        report_role_level_1,
        report_role_level_2,
        report_role_level_3,
        report_role_level_4,
        report_role_level_5,
        opp_account_demographics_sales_segment,
        opp_account_demographics_region,
        opp_account_demographics_geo,
        opp_account_demographics_territory,
        opp_account_demographics_area,
        parent_crm_account_upa_country,
        true_inquiry_date,
        mql_date_latest_pt,
        opp_created_date,
        sales_accepted_date,
        pipeline_created_date,
        close_date
    FROM {{ref('rpt_lead_to_revenue')}}
    WHERE dim_crm_person_id IS NOT NULL
        OR dim_crm_opportunity_id IS NOT NULL

), target_prep AS (

    SELECT
        target_date,
        kpi_name,
        crm_user_sales_segment,
        crm_user_geo,
        crm_user_region,
        order_type_name,
        sales_qualified_source_name,
        daily_allocated_target
     FROM {{ref('mart_sales_funnel_target_daily')}}

), inquiry_prep AS (
    
    SELECT DISTINCT
        CASE
            WHEN person_order_type = '1. New - First Order'
                THEN TRUE 
            ELSE FALSE        
        END                                       AS is_first_order,
        account_demographics_sales_segment        AS segment,
        account_demographics_region               AS region,
        account_demographics_geo                  AS geo,
        person_first_country                      AS country,
        true_inquiry_date                         AS metric_date,
        'Inquiry'                                 AS metric,
        CASE 
            WHEN true_inquiry_date IS NOT NULL 
                THEN email_hash
            ELSE NULL
        END                                       AS metric_value,
        NULL                                      AS target_value
    FROM l2r_base
    WHERE (l2r_base.true_inquiry_date <= l2r_base.mql_date_latest_pt
        OR l2r_base.mql_date_latest_pt IS NULL)

), mql_prep AS (

    SELECT DISTINCT
        CASE
            WHEN person_order_type = '1. New - First Order'
                THEN TRUE 
            ELSE FALSE        
        END                                       AS is_first_order,
        account_demographics_sales_segment        AS segment,
        account_demographics_region               AS region,
        account_demographics_geo                  AS geo,
        person_first_country                      AS country,
        mql_date_latest_pt                        AS metric_date,
        'MQL'                                     AS metric,
        CASE 
            WHEN is_mql = 1 
                THEN email_hash
            ELSE NULL
        END                                       AS metric_value,
        daily_allocated_target                    AS target_value
    FROM l2r_base
    LEFT JOIN target_prep
        ON l2r_base.mql_date_latest_pt=target_prep.target_date
            AND l2r_base.account_demographics_sales_segment=target_prep.crm_user_sales_segment
            AND l2r_base.account_demographics_geo=target_prep.crm_user_geo
            AND l2r_base.account_demographics_region=target_prep.crm_user_region
            AND l2r_base.person_order_type=target_prep.order_type_name
    WHERE (l2r_base.mql_date_latest_pt <= l2r_base.sales_accepted_date
        OR l2r_base.sales_accepted_date IS NULL)
        AND target_prep.kpi_name = 'MQL'

), sao_prep AS (

    SELECT DISTINCT
        CASE
            WHEN opp_order_type = '1. New - First Order'
                THEN TRUE 
            ELSE FALSE        
        END                                       AS is_first_order,
        opp_account_demographics_sales_segment    AS segment,
        opp_account_demographics_region           AS region,
        opp_account_demographics_geo              AS geo,
        parent_crm_account_upa_country            AS country,
        l2r_base.sales_qualified_source_name,
        sales_accepted_date                       AS metric_date,
        'SAO'                                     AS metric,
        CASE 
            WHEN is_sao
                THEN dim_crm_opportunity_id
            ELSE NULL
        END                                       AS metric_value,
        daily_allocated_target                    AS target_value
    FROM l2r_base
    LEFT JOIN target_prep
        ON l2r_base.sales_accepted_date=target_prep.target_date
            AND l2r_base.opp_account_demographics_sales_segment=target_prep.crm_user_sales_segment
            AND l2r_base.opp_account_demographics_geo=target_prep.crm_user_geo
            AND l2r_base.opp_account_demographics_region=target_prep.crm_user_region
            AND l2r_base.opp_order_type=target_prep.order_type_name
            AND l2r_base.sales_qualified_source_name=target_prep.sales_qualified_source_name
    WHERE (l2r_base.sales_accepted_date <= l2r_base.close_date
        OR l2r_base.close_date IS NULL)
        AND target_prep.kpi_name = 'SAO'

), pipeline_prep AS (

    SELECT DISTINCT
        CASE
            WHEN opp_order_type = '1. New - First Order'
                THEN TRUE 
            ELSE FALSE        
        END                                       AS is_first_order,
        opp_account_demographics_sales_segment    AS segment,
        opp_account_demographics_region           AS region,
        opp_account_demographics_geo              AS geo,
        parent_crm_account_upa_country            AS country,
        l2r_base.sales_qualified_source_name,
        pipeline_created_date                     AS metric_date,
        'Pipeline'                                AS metric,
        CASE 
            WHEN is_net_arr_pipeline_created = 1
                THEN net_arr
            ELSE NULL
        END                                       AS metric_value,
        daily_allocated_target                    AS target_value
    FROM l2r_base
    LEFT JOIN target_prep
        ON l2r_base.true_inquiry_date=target_prep.target_date
            AND l2r_base.opp_account_demographics_sales_segment=target_prep.crm_user_sales_segment
            AND l2r_base.opp_account_demographics_geo=target_prep.crm_user_geo
            AND l2r_base.opp_account_demographics_region=target_prep.crm_user_region
            AND l2r_base.opp_order_type=target_prep.order_type_name
            AND l2r_base.sales_qualified_source_name=target_prep.sales_qualified_source_name
    WHERE (l2r_base.pipeline_created_date <= l2r_base.close_date
        OR l2r_base.close_date IS NULL)
        AND target_prep.kpi_name = 'Net ARR Pipeline Created'

), closed_won_prep AS (

    SELECT DISTINCT
        CASE
            WHEN opp_order_type = '1. New - First Order'
                THEN TRUE 
            ELSE FALSE        
        END                                       AS is_first_order,
        opp_account_demographics_sales_segment    AS segment,
        opp_account_demographics_region           AS region,
        opp_account_demographics_geo              AS geo,
        parent_crm_account_upa_country            AS country,
        l2r_base.sales_qualified_source_name,
        pipeline_created_date                     AS metric_date,
        'Closed Won'                              AS metric,
        dim_crm_opportunity_id                    AS metric_value,
        daily_allocated_target                    AS target_value
    FROM l2r_base
    LEFT JOIN target_prep
        ON l2r_base.true_inquiry_date=target_prep.target_date
            AND l2r_base.opp_account_demographics_sales_segment=target_prep.crm_user_sales_segment
            AND l2r_base.opp_account_demographics_geo=target_prep.crm_user_geo
            AND l2r_base.opp_account_demographics_region=target_prep.crm_user_region
            AND l2r_base.opp_order_type=target_prep.order_type_name
            AND l2r_base.sales_qualified_source_name=target_prep.sales_qualified_source_name
    WHERE is_won
        AND target_prep.kpi_name = 'Deals'

), final AS (

    SELECT
        metric_date,
        segment,
        region,
        geo,
        country,
        is_first_order,
        NULL AS sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS metric_value
    FROM inquiry_prep
    {{dbt_utils.group_by(n=9)}}
    UNION ALL 
    SELECT
        metric_date,
        segment,
        region,
        geo,
        country,
        is_first_order,
        NULL AS sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS metric_value
    FROM mql_prep
    {{dbt_utils.group_by(n=9)}}
    UNION ALL 
    SELECT
        metric_date,
        segment,
        region,
        geo,
        country,
        is_first_order,
        sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS metric_value
    FROM sao_prep
    {{dbt_utils.group_by(n=9)}}
    UNION ALL 
    SELECT
        metric_date,
        segment,
        region,
        geo,
        country,
        is_first_order,
        sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS metric_value
    FROM pipeline_prep
    {{dbt_utils.group_by(n=9)}}
    UNION ALL 
    SELECT
        metric_date,
        segment,
        region,
        geo,
        country,
        is_first_order,
        sales_qualified_source_name,
        metric,
        target_value,
        COUNT(DISTINCT metric_value) AS metric_value
    FROM closed_won_prep
    {{dbt_utils.group_by(n=9)}} 

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-07-26",
    updated_date="2024-07-26",
) }}