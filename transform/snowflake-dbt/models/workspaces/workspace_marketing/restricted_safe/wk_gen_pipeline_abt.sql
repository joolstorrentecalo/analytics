{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'),
    ('snowplow_sessions_all', 'snowplow_sessions_all'),
	('rpt_namespace_onboarding', 'rpt_namespace_onboarding'),
	('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('dim_date', 'dim_date')
    ])
}},


-- https://gitlab.com/gitlab-data/data-science-projects/attribution-modeling/-/merge_requests/1#note_1935997603 

l2r_base AS (

    SELECT
    -- CORE IDS
        lead_to_revenue_id,
        dim_crm_account_id,
        dim_crm_opportunity_id,
        dim_crm_person_id,
        sfdc_record_id,
        dim_crm_touchpoint_id,
        dim_crm_btp_touchpoint_id,
        dim_crm_batp_touchpoint_id,
        CASE 
            WHEN dim_crm_batp_touchpoint_id IS NOT NULL THEN 'Attributed Touchpoint'
            WHEN dim_crm_touchpoint_id IS NULL THEN 'No Touchpoint'
            ELSE 'Standard Touchpoint'
        END AS attributed_tp_label,
        dim_campaign_id,

    -- Geo and Segment fields
        -- Account level 
        inferred_geo,
        inferred_employee_segment,
        -- Person level 
        account_demographics_geo,
        account_demographics_sales_segment,
        --Opp level
        crm_opp_owner_geo_stamped,
        crm_opp_owner_sales_segment_stamped,

    --    Customer / Order Type
    -- Person level
        is_first_order_available,
        CASE 
            WHEN  person_order_type = '3. Growth' THEN '3. Growth or Other'
            ELSE person_order_type
        END as person_order_type_label,
        -- Opp level
        opp_order_type, 

        CASE WHEN opp_order_type IN ('1. New - First Order', '1. First Order') THEN 'New' 
             WHEN opp_order_type IN ('2. New Connected', '3. Growth') THEN 'Growth' -- Could add new connected to New too
             WHEN opp_order_type IN ('4. Contraction', '5. Churn - Partial', '6. Churn - Final') THEN 'Contraction or Churn'
             WHEN dim_crm_opportunity_id IS NULL THEN 'None'
             ELSE opp_order_type
        END AS opp_order_type_label, 

    -- Other demographic fields
        -- Account level
        parent_crm_account_lam_dev_count, -- could be useful for size segmentation along with deal size 
        -- industry fields, tech stack and more available in mart_crm_account
        -- Person level
        email_domain_type,

    -- Source from sales POV
    -- Person level
        source_buckets,
        lead_source,
        --Opp level
        sales_qualified_source_name,
        opp_lead_source,
        opp_source_buckets,
        coalesce(lead_source, opp_lead_source) AS lead_source_combined,
        coalesce(source_buckets, opp_source_buckets) AS source_buckets_combined,

        -- Marketing Channel  
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        opp_bizible_marketing_channel,
        opp_bizible_marketing_channel_path,
        --    bizible_touchpoint_source,
        coalesce(bizible_marketing_channel, opp_bizible_marketing_channel) AS marketing_channel_combined,
        coalesce(bizible_marketing_channel_path, opp_bizible_marketing_channel_path) AS marketing_channel_path_combined,

        -- Other Touchpoint fields
        bizible_touchpoint_position,
        bizible_touchpoint_type,

        -- New fields to surface channel just before MQL
        -- bizible_mql_marketing_channel,
        -- bizible_mql_marketing_channel_path

    --    bizible_medium,
        touchpoint_offer_type_grouped,
        touchpoint_offer_type,

    --    bizible_form_page_utm_budget,
    --    bizible_landing_page_utm_budget,

    -- Status fields - Account Type / Order Type, lead lifecycle fields
    -- Account level
    -- crm_account_type (Customer or Prospect) -> not in L2R yet - added from snapshot table
        sfdc_record_type, -- lead or contact
        status AS lead_status,
        lead_score_classification,

    -- Flags
    -- Account level
    -- Person level
        is_inquiry,
        is_mql,
        -- Opp level
        is_sao,
        is_net_arr_pipeline_created,
        is_eligible_age_analysis, 
    -- swap mart_crm_opportunity cte back to L2R when new pipeline fields added 

    --    Closed Won flags - leave for future iteration
    --    is_won,
    --    valid_deal_count,
    --    is_net_arr_closed_deal,
    --    close_date,
        CASE 
        WHEN sales_qualified_source_name != 'Web Direct Generated' AND is_net_arr_pipeline_created = true THEN 'Sales-Assisted Pipeline'
        WHEN sales_qualified_source_name != 'Web Direct Generated' AND is_sao = true THEN 'Sales-Assisted SAO, no pipeline' 
        WHEN sales_qualified_source_name != 'Web Direct Generated' AND dim_crm_opportunity_id IS NOT NULL THEN 'Sales-Assisted Opp, no SAO or Pipeline'
        WHEN sales_qualified_source_name = 'Web Direct Generated' THEN 'Web Direct' --AND status != 'Ineligible'
        WHEN dim_crm_opportunity_id IS NULL THEN 'No Opp Created'
        ELSE 'Error' 
        END AS conversion_outcome, 

        CASE 
        WHEN sales_qualified_source_name != 'Web Direct Generated' AND is_net_arr_pipeline_created = true THEN 'Sales-Assisted Pipeline'
        WHEN sales_qualified_source_name = 'Web Direct Generated' THEN 'Web Direct' 
        WHEN dim_crm_opportunity_id IS NULL OR is_net_arr_pipeline_created = false THEN 'No Pipeline Opp'
        ELSE 'Error' 
        END AS conversion_outcome_grouped, 

    -- Dates 
    -- Account level
            -- Customer Since Date
    -- Person level
        true_inquiry_date,
        mql_date_first_pt,
        mql_date_latest_pt,
        -- Opp level
        sales_accepted_date,
        opp_created_date,
        -- pipeline_created_date - To be added to L2R by RK
        -- Touchpoint level
        bizible_touchpoint_date,

            -- Opportunity fields
        opportunity_name,
        net_arr AS net_arr_l2r,
        CASE WHEN is_net_arr_pipeline_created = 1 THEN net_arr ELSE 0 END AS created_arr_l2r,

        --    touchpoint_sales_stage, -- field not in L2R yet

        -- Bizible attributed numbers to benchmark
        bizible_weight_custom_model,

        custom_opp_created,
        linear_opp_created,

        pipeline_custom_net_arr,
        pipeline_linear_net_arr,
        custom_sao,
        linear_sao--,

    --    won_custom_net_arr,
    --    won_linear_net_arr,
    --    won_custom,
    --    won_linear
        , COUNT(*) OVER (PARTITION BY dim_crm_account_id, dim_crm_opportunity_id, bizible_touchpoint_date, bizible_marketing_channel) AS num_touchpoints 
    -- Max() should work here too
    FROM rpt_lead_to_revenue
    WHERE 1=1 
        AND (dim_crm_btp_touchpoint_id IS NOT NULL OR dim_crm_batp_touchpoint_id IS NOT NULL) 
        AND dim_crm_account_id IS NOT NULL-- Focusing on just those touchpoints associcated with a SFDC account
        -- Start with opps from last FY (only 4 months of opps so far). Final model likely to have at least start of FY24
        AND ( LEAST(opp_created_date, sales_accepted_date) BETWEEN '2024-02-01' AND '2024-06-01' OR dim_crm_opportunity_id IS NULL)
        --    AND bizible_touchpoint_date BETWEEN '2022-05-01' AND '2024-04-30' -- Only look at touchpoints from last two years; give an extra month to see if an oppty gets created
        -- fix touchpoints in a window - start a year before opp creation as large deals can take > than a year to develop
        AND (bizible_touchpoint_date >= LEAST(opp_created_date, sales_accepted_date) - 365 OR dim_crm_opportunity_id IS NULL)
        -- For 2 years of opps
        -- 730 DAYS = 668,553 rows -> 365 days = 522,736 rows
        -- need pipeline created date in L2R - > have moved second half of TP filter down to final CTE
        --    AND (bizible_touchpoint_date <= LEAST(opp_created_date, sales_accepted_date) OR dim_crm_opportunity_id IS NULL) -- Older line to only look at touchpoints before oppty was created 

        -- Exclude rows that can be reasonably known as ineligble at point of opp creation
        AND (lower(crm_opp_owner_geo_stamped) != 'jihu' OR lower(account_demographics_geo) != 'jihu' OR lower(account_demographics_sales_segment) != 'jihu' OR lower(opp_account_demographics_geo) != 'jihu')
                -- No extra impact on rows from any of JIHU exclusions
        AND status NOT IN ('Ineligible')
        --QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_crm_account_id, dim_crm_opportunity_id, dim_crm_touchpoint_id ORDER BY bizible_touchpoint_date) = 1 -- limit to just one unique touchpoint_id per opp_id. Could further limit to one touchpoint per day per channel if needed/desired
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_crm_account_id, dim_crm_opportunity_id, bizible_touchpoint_date, bizible_marketing_channel  ORDER BY bizible_marketing_channel) = 1 -- limit to just one unique channel touchpoint per day per opp id
            -- JA: Without this condition the row numbers balloon to 39.5 Million 
            -- Longer term we will revist this and see if we can find a more nuanced way to select the touchpoint per day that has impact, plus find a way to keep attribution for different budget holders 

    ), l2r_plus_opps AS (

        SELECT 
            l2r_base.*,
            is_renewal,
            is_eligible_open_pipeline,
            pipeline_created_date,
            -- is_eligible_age_analysis,
            opportunity_category,
            stage_name,
            -- is_jihu_account,
            is_edu_oss,
            stage_1_discovery_date, 
            net_arr,
            -- created_arr -- not present in mart_crm_opportunity, is present in snapshot tables

            -- flags to compare vs. L2R (REMOVED USE OF SNAPSHOT TABLE)
            -- mart_crm_opportunity_daily_snapshot.order_type AS snapshot_order_type,
            -- mart_crm_opportunity_daily_snapshot.order_type_grouped AS snapshot_order_type_grouped,
            -- mart_crm_opportunity_daily_snapshot.order_type_live AS snapshot_order_type_live,
            -- mart_crm_opportunity_daily_snapshot.crm_opp_owner_geo_stamped AS snapshot_crm_opp_owner_geo_stamped,
            -- mart_crm_opportunity_daily_snapshot.crm_opp_owner_sales_segment_stamped AS snapshot_crm_opp_owner_sales_segment_stamped,

        -- Closed fields not needed yet 
            -- is_booked_net_arr,
            -- is_win_rate_calc,
            -- calculated_deal_count,
            -- is_closed,
        FROM l2r_base
        LEFT JOIN PROD.restricted_safe_common_mart_sales.mart_crm_opportunity
            ON l2r_base.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
            -- AND LEAST(l2r_base.opp_created_date, l2r_base.sales_accepted_date) = mart_crm_opportunity_daily_snapshot.snapshot_date
            -- Removed snapshot table and logic as for these flags we are ok having the live version rather than needing to pin them to opp info at time of pipeline creation
        WHERE 1=1 
            AND is_renewal = 0 OR is_renewal IS NULL

    ), l2r_opps_accounts AS (

        SELECT 
            l2r_plus_opps.*, 
            prep_crm_account_daily_snapshot.crm_account_type AS snapshot_crm_account_type,-- sot field for customer or prospect account label
            mart_crm_account.crm_account_type,
            coalesce(prep_crm_account_daily_snapshot.crm_account_type, mart_crm_account.crm_account_type) AS crm_account_type_combined,
            mart_crm_account.customer_since_date
        FROM l2r_plus_opps
        LEFT JOIN PROD.restricted_safe_common_prep.prep_crm_account_daily_snapshot
            ON l2r_plus_opps.dim_crm_account_id = prep_crm_account_daily_snapshot.dim_crm_account_id
        -- AND LEAST(l2r_plus_opps.opp_created_date, l2r_plus_opps.sales_accepted_date) = prep_crm_account_daily_snapshot.snapshot_date
            AND l2r_plus_opps.pipeline_created_date = prep_crm_account_daily_snapshot.snapshot_date 
            -- snapshot used to ensure our account type label matches the status at the time in question
        LEFT JOIN PROD.restricted_safe_common_mart_sales.mart_crm_account
            ON l2r_plus_opps.dim_crm_account_id = mart_crm_account.dim_crm_account_id
        WHERE 1=1
        AND mart_crm_account.is_jihu_account = 0  OR mart_crm_account.is_jihu_account IS NULL

    ), final AS (

        SELECT 
            *
            , ROW_NUMBER() OVER (PARTITION BY dim_crm_account_id, dim_crm_opportunity_id ORDER BY bizible_touchpoint_date ASC, dim_crm_touchpoint_id ASC) AS touchpoint_sequence
            , DENSE_RANK() OVER (PARTITION BY dim_crm_account_id ORDER BY sales_accepted_date ASC, dim_crm_opportunity_id ASC) AS opportunity_sequence
            -- edge case -> whats expected behaviour when there are no opps on the account? At the moment all these TPs get labelled as 1. This could be interpreted as the first (potential) purchase path
        FROM l2r_opps_accounts
        WHERE 1=1 
            -- Brought TP filter down to remove TPs after pipeline created date
            AND bizible_touchpoint_date <= pipeline_created_date OR dim_crm_opportunity_id IS NULL
            -- 509K to 299K rows 
            -- Leave SMB in for now
            -- AND crm_opp_owner_sales_segment_stamped != 'SMB' OR inferred_employee_segment != 'SMB' OR account_demographics_sales_segment != 'SMB' OR inferred_geo != 'SMB' OR account_demographics_geo != 'SMB'
        ORDER BY dim_crm_account_id, dim_crm_opportunity_id, bizible_touchpoint_date ASC, dim_crm_btp_touchpoint_id ASC, dim_crm_batp_touchpoint_id ASC
        
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jahye",
    updated_by="@jahye",
    created_date="2024-06-19",
    updated_date="2024-06-19"
) }}
