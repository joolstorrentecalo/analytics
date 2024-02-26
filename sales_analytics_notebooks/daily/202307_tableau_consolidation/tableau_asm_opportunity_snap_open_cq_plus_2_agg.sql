/*

Open Quarter Plus 2 Historical AGGREGATED

*/

WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

),

sfdc_account_xf AS (

    SELECT
        account_id,
        industry
    FROM prod.restricted_safe_workspace_sales.sfdc_accounts_xf


),

sfdc_opportunity_snapshot_history_xf AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf

),

current_quarter_date AS (

    SELECT
        fiscal_year                      AS current_fiscal_year,
        date_actual                      AS current_calendar_date,
        fiscal_quarter_name_fy           AS current_fiscal_quarter_name,
        first_day_of_fiscal_quarter      AS current_fiscal_quarter_date,
        day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalized
    FROM date_details
    WHERE date_actual = (
        SELECT MAX(snapshot_date)
        FROM sfdc_opportunity_snapshot_history_xf
    )
),

opty_base AS (

    SELECT

        opty.*,

        calculated_deal_size AS deal_size_bin
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf AS opty
    CROSS JOIN current_quarter_date
    INNER JOIN sfdc_account_xf AS account
        ON account.account_id = opty.account_id
    -- only capture current fiscal quarter
    WHERE
        opty.close_fiscal_quarter_date = DATEADD(MONTH, 6, current_quarter_date.current_fiscal_quarter_date)
        AND opty.is_eligible_open_pipeline_flag = 1
),

snap_base AS (

    SELECT
        snap.*,

        calculated_deal_size AS deal_size_bin

    FROM sfdc_opportunity_snapshot_history_xf AS snap
    INNER JOIN current_quarter_date
        ON snap.snapshot_day_of_fiscal_quarter_normalised = current_day_of_fiscal_quarter_normalized

    INNER JOIN sfdc_account_xf AS account
        ON account.account_id = snap.account_id
    -- Exclude the current quarter so we could leverage the opportunity object
    -- the idea is to have the current quarter match and do not create confusion due
    -- timing issues.
    WHERE
        snap.snapshot_fiscal_quarter_date >= DATEADD(MONTH, -18, CURRENT_DATE)
        AND snap.snapshot_fiscal_quarter_date != current_quarter_date.current_fiscal_quarter_date
        AND snap.close_fiscal_quarter_date = DATEADD(MONTH, 6, snap.snapshot_fiscal_quarter_date)
        AND snap.is_eligible_open_pipeline_flag = 1
),

opty_aggregated AS (
    SELECT

        -------
        -------
        -- DIMENSIONS

        owner_id,
        opportunity_owner,

        account_id,
        account_name,

        report_opportunity_user_business_unit,
        report_opportunity_user_sub_business_unit,
        report_opportunity_user_division,
        report_opportunity_user_asm,
        COALESCE(report_opportunity_user_role_type, 'NA')   AS report_opportunity_user_role_type,

        COALESCE(deal_size_bin, 'NA')                       AS deal_size_bin,
        COALESCE(age_bin, 'NA')                             AS age_bin,
        COALESCE(partner_category, 'NA')                    AS partner_category,
        COALESCE(sales_qualified_source, 'NA')              AS sales_qualified_source,
        COALESCE(stage_name, 'NA')                          AS stage_name,
        COALESCE(order_type_stamped, 'NA')                  AS order_type_stamped,
        COALESCE(deal_group, 'NA')                          AS deal_group,
        COALESCE(sales_type, 'NA')                          AS sales_type,
        COALESCE(forecast_category_name, 'NA')              AS forecast_category_name,
        COALESCE(product_category_tier, 'NA')               AS product_category_tier,
        COALESCE(product_category_deployment, 'NA')         AS product_category_deployment,
        COALESCE(industry, 'NA')                            AS industry,
        COALESCE(lam_dev_count_bin, 'NA')                   AS lam_dev_count_bin,
        COALESCE(current_stage_age_bin, 'NA')               AS current_stage_age_bin,

        COALESCE(parent_crm_account_upa_country_name, 'NA') AS parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_eligible_open_pipeline_flag,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        -----------------------------------------------
        close_fiscal_quarter_date                           AS close_date,
        CURRENT_DATE                                        AS snapshot_date,
        created_fiscal_quarter_date                         AS created_date,
        pipeline_created_fiscal_quarter_date                AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                                        AS net_arr,
        SUM(booked_net_arr)                                 AS booked_net_arr,
        SUM(open_1plus_net_arr)                             AS open_1plus_net_arr,

        SUM(calculated_deal_count)                          AS deal_count,
        SUM(booked_deal_count)                              AS booked_deal_count,
        AVG(cycle_time_in_days)                             AS age_in_days

    FROM opty_base
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34

),

snap_aggregated AS (
    SELECT

        -------
        -------
        -- DIMENSIONS

        owner_id,
        opportunity_owner,

        account_id,
        account_name,

        report_opportunity_user_business_unit,
        report_opportunity_user_sub_business_unit,
        report_opportunity_user_division,
        report_opportunity_user_asm,
        COALESCE(report_opportunity_user_role_type, 'NA')   AS report_opportunity_user_role_type,

        COALESCE(deal_size_bin, 'NA')                       AS deal_size_bin,
        COALESCE(age_bin, 'NA')                             AS age_bin,
        COALESCE(partner_category, 'NA')                    AS partner_category,
        COALESCE(sales_qualified_source, 'NA')              AS sales_qualified_source,
        COALESCE(stage_name, 'NA')                          AS stage_name,
        COALESCE(order_type_stamped, 'NA')                  AS order_type_stamped,
        COALESCE(deal_group, 'NA')                          AS deal_group,
        COALESCE(sales_type, 'NA')                          AS sales_type,
        COALESCE(forecast_category_name, 'NA')              AS forecast_category_name,
        COALESCE(product_category_tier, 'NA')               AS product_category_tier,
        COALESCE(product_category_deployment, 'NA')         AS product_category_deployment,
        COALESCE(industry, 'NA')                            AS industry,
        COALESCE(lam_dev_count_bin, 'NA')                   AS lam_dev_count_bin,
        COALESCE(current_stage_age_bin, 'NA')               AS current_stage_age_bin,

        COALESCE(parent_crm_account_upa_country_name, 'NA') AS parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_eligible_open_pipeline_flag,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        -----------------------------------------------
        close_fiscal_quarter_date                           AS close_date,
        snapshot_fiscal_quarter_date                        AS snapshot_date,
        created_fiscal_quarter_date                         AS created_date,
        pipeline_created_fiscal_quarter_date                AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                                        AS net_arr,
        SUM(booked_net_arr)                                 AS booked_net_arr,
        SUM(open_1plus_net_arr)                             AS open_1plus_net_arr,

        SUM(calculated_deal_count)                          AS deal_count,
        SUM(booked_deal_count)                              AS booked_deal_count,
        AVG(cycle_time_in_days)                             AS age_in_days

    FROM snap_base
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31, 32, 33, 34
),

aggregated AS (

    SELECT *
    FROM snap_aggregated
    UNION
    SELECT *
    FROM opty_aggregated

),

final AS (

    SELECT
        aggregated.*,

        COALESCE(close_date.fiscal_year = current_quarter_date.current_fiscal_year, FALSE)      AS is_cfy_flag,

        COALESCE(aggregated.close_date = current_fiscal_quarter_date, FALSE)                    AS is_cfq_flag,

        COALESCE(aggregated.close_date = DATEADD(MONTH, 3, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_1_flag,

        COALESCE(aggregated.close_date = DATEADD(MONTH, 6, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_2_flag,

        COALESCE(
            aggregated.close_date >= current_fiscal_quarter_date
            AND aggregated.close_date <= DATEADD(MONTH, 15, current_fiscal_quarter_date),
            FALSE
        )                                                                                       AS is_open_pipeline_range_flag,
        COALESCE(
            aggregated.close_date <= current_fiscal_quarter_date
            AND aggregated.close_date >= DATEADD(MONTH, -15, current_fiscal_quarter_date),
            FALSE
        )                                                                                       AS is_bookings_range_flag,

        COALESCE(
            aggregated.is_open = TRUE
            AND aggregated.is_stage_1_plus = 1,
            FALSE
        )                                                                                       AS is_open_stage_1_plus,

        COALESCE(
            aggregated.is_open = TRUE
            AND aggregated.is_stage_3_plus = 1,
            FALSE
        )                                                                                       AS is_open_stage_3_plus,

        close_date.fiscal_year                                                                  AS close_fiscal_year,
        close_date.fiscal_quarter_name_fy                                                       AS close_fiscal_quarter_name,

        LOWER(
            CONCAT(
                aggregated.report_opportunity_user_business_unit,
                '_', aggregated.report_opportunity_user_sub_business_unit,
                '_', aggregated.report_opportunity_user_division,
                '_', aggregated.report_opportunity_user_asm,
                '_', aggregated.sales_qualified_source,
                '_', aggregated.deal_group
            )
        )                                                                                       AS key_bu_subbu_division_asm_sqs_ot,

        LOWER(
            CONCAT(
                aggregated.report_opportunity_user_business_unit,
                '_', aggregated.report_opportunity_user_sub_business_unit
            )
        )                                                                                       AS key_bu_subbu


    FROM aggregated
    CROSS JOIN current_quarter_date
    LEFT JOIN date_details AS close_date
        ON close_date.date_actual = aggregated.close_date
    WHERE (
        net_arr != 0
        OR booked_net_arr != 0
    )
)

SELECT *
FROM final
