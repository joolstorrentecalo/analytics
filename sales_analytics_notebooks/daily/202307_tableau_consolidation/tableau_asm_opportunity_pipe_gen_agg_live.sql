WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

),

report_date AS (
    SELECT
        fiscal_year                      AS current_fiscal_year,
        date_actual                      AS current_calendar_date,
        fiscal_quarter_name_fy           AS current_fiscal_quarter_name,
        first_day_of_fiscal_quarter      AS current_fiscal_quarter_date,
        day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalized
    FROM date_details
    WHERE date_actual = CURRENT_DATE

),

sfdc_opportunity_xf AS (

    SELECT
        report_date.*,
        opty.*,

        calculated_deal_size AS deal_size_bin

    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf AS opty
    LEFT JOIN prod.restricted_safe_workspace_sales.sfdc_accounts_xf AS account
        ON account.account_id = opty.account_id
    CROSS JOIN report_date
    WHERE
        opty.is_edu_oss = 0
        AND opty.is_deleted = 0
        --AND opty.key_bu_subbu_division NOT LIKE '%other%'
        AND opty.is_jihu_account = 0
        --AND opty.net_arr != 0

),

aggregated_base AS (
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
        COALESCE(pipeline_landing_quarter, 'NA')            AS pipeline_landing_quarter,
        COALESCE(current_stage_age_bin, 'NA')               AS current_stage_age_bin,

        COALESCE(parent_crm_account_upa_country_name, 'NA') AS parent_crm_account_upa_country_name,

        is_web_portal_purchase,
        is_open,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,
        is_eligible_created_pipeline_flag,

        -----------------------------------------------
        -- Date dimensions Aggregated
        pipeline_created_fiscal_quarter_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        SUM(net_arr)                                        AS net_arr,
        SUM(booked_net_arr)                                 AS booked_net_arr,
        SUM(open_1plus_net_arr)                             AS open_1plus_net_arr,

        SUM(calculated_deal_count)                          AS deal_count,
        SUM(booked_deal_count)                              AS booked_deal_count,
        AVG(cycle_time_in_days)                             AS age_in_days,

        SUM(total_professional_services_value)              AS total_professional_services_value,
        SUM(total_book_professional_services_value)         AS total_book_professional_services_value,
        SUM(total_lost_professional_services_value)         AS total_lost_professional_services_value,
        SUM(total_open_professional_services_value)         AS total_open_professional_services_value

    FROM sfdc_opportunity_xf
    WHERE
        pipeline_created_fiscal_quarter_date IS NOT NULL
        AND is_eligible_created_pipeline_flag = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
),

eligible_dates AS (
    SELECT DISTINCT first_day_of_fiscal_quarter AS report_date
    FROM date_details
    CROSS JOIN report_date
    WHERE
        date_details.fiscal_year >= report_date.current_fiscal_year - 1
        AND date_details.fiscal_year <= report_date.current_fiscal_year + 1
),

base_key AS (
    SELECT DISTINCT
        a.owner_id,
        a.opportunity_owner,

        a.account_id,
        a.account_name,

        a.report_opportunity_user_business_unit,
        a.report_opportunity_user_sub_business_unit,
        a.report_opportunity_user_division,
        a.report_opportunity_user_asm,
        a.report_opportunity_user_role_type,

        a.deal_size_bin,
        a.age_bin,
        a.partner_category,
        a.sales_qualified_source,
        a.stage_name,
        a.order_type_stamped,
        a.deal_group,
        a.sales_type,
        a.forecast_category_name,
        a.product_category_tier,
        a.product_category_deployment,
        a.industry,
        a.lam_dev_count_bin,
        a.pipeline_landing_quarter,
        a.current_stage_age_bin,

        a.parent_crm_account_upa_country_name,

        a.is_web_portal_purchase,
        a.is_open,
        a.is_stage_1_plus,
        a.is_stage_3_plus,
        a.fpa_master_bookings_flag,

        -----------------------------------------------
        -- Date dimensions
        b.report_date
    FROM aggregated_base AS a
    CROSS JOIN eligible_dates AS b
    WHERE a.net_arr != 0

),

consolidated AS (

    SELECT
        base_key.owner_id,
        base_key.opportunity_owner,

        base_key.account_id,
        base_key.account_name,

        base_key.report_opportunity_user_business_unit,
        base_key.report_opportunity_user_sub_business_unit,
        base_key.report_opportunity_user_division,
        base_key.report_opportunity_user_asm,
        base_key.report_opportunity_user_role_type,

        base_key.deal_size_bin,
        base_key.age_bin,
        base_key.partner_category,
        base_key.sales_qualified_source,
        base_key.stage_name,
        base_key.order_type_stamped,
        base_key.deal_group,
        base_key.sales_type,
        base_key.forecast_category_name,
        base_key.product_category_tier,
        base_key.product_category_deployment,
        base_key.industry,
        base_key.lam_dev_count_bin,
        base_key.pipeline_landing_quarter,
        base_key.current_stage_age_bin,

        base_key.parent_crm_account_upa_country_name,

        base_key.is_web_portal_purchase,
        base_key.is_open,
        base_key.is_stage_1_plus,
        base_key.is_stage_3_plus,
        base_key.fpa_master_bookings_flag,

        base_key.report_date               AS pipeline_created_date,

        -----------------------------------------------
        -- Dimensions for Detail / Aggregated

        aggregated_base.net_arr,
        aggregated_base.booked_net_arr,
        aggregated_base.open_1plus_net_arr,

        aggregated_base.deal_count,
        aggregated_base.booked_deal_count,

        -----------------------------------------------
        -- Dimensions for Aggregated
        previous_quarter.net_arr           AS prev_quarter_net_arr,
        previous_quarter.booked_net_arr    AS prev_quarter_booked_net_arr,
        previous_quarter.booked_deal_count AS prev_quarter_booked_deal_count,
        previous_year.net_arr              AS prev_year_net_arr,
        previous_year.booked_net_arr       AS prev_year_booked_net_arr,
        previous_year.booked_deal_count    AS prev_year_booked_deal_count

    FROM base_key
    LEFT JOIN aggregated_base
        ON
            base_key.owner_id = aggregated_base.owner_id
            AND base_key.account_id = aggregated_base.account_id
            AND base_key.report_opportunity_user_business_unit = aggregated_base.report_opportunity_user_business_unit
            AND base_key.report_opportunity_user_sub_business_unit = aggregated_base.report_opportunity_user_sub_business_unit
            AND base_key.report_opportunity_user_division = aggregated_base.report_opportunity_user_division
            AND base_key.report_opportunity_user_asm = aggregated_base.report_opportunity_user_asm
            AND base_key.report_opportunity_user_role_type = aggregated_base.report_opportunity_user_role_type
            AND base_key.deal_size_bin = aggregated_base.deal_size_bin
            AND base_key.age_bin = aggregated_base.age_bin
            AND base_key.partner_category = aggregated_base.partner_category
            AND base_key.sales_qualified_source = aggregated_base.sales_qualified_source
            AND base_key.stage_name = aggregated_base.stage_name
            AND base_key.order_type_stamped = aggregated_base.order_type_stamped
            AND base_key.sales_type = aggregated_base.sales_type
            AND base_key.forecast_category_name = aggregated_base.forecast_category_name
            AND base_key.product_category_tier = aggregated_base.product_category_tier
            AND base_key.product_category_deployment = aggregated_base.product_category_deployment
            AND base_key.parent_crm_account_upa_country_name = aggregated_base.parent_crm_account_upa_country_name
            AND base_key.is_web_portal_purchase = aggregated_base.is_web_portal_purchase
            AND base_key.fpa_master_bookings_flag = aggregated_base.fpa_master_bookings_flag
            AND base_key.report_date = aggregated_base.pipeline_created_fiscal_quarter_date
            AND base_key.industry = aggregated_base.industry
            AND base_key.pipeline_landing_quarter = aggregated_base.pipeline_landing_quarter
            AND base_key.lam_dev_count_bin = aggregated_base.lam_dev_count_bin
            AND base_key.current_stage_age_bin = aggregated_base.current_stage_age_bin
    LEFT JOIN aggregated_base AS previous_quarter
        ON
            base_key.owner_id = previous_quarter.owner_id
            AND base_key.account_id = previous_quarter.account_id
            AND base_key.report_opportunity_user_business_unit = previous_quarter.report_opportunity_user_business_unit
            AND base_key.report_opportunity_user_sub_business_unit = previous_quarter.report_opportunity_user_sub_business_unit
            AND base_key.report_opportunity_user_division = previous_quarter.report_opportunity_user_division
            AND base_key.report_opportunity_user_asm = previous_quarter.report_opportunity_user_asm
            AND base_key.report_opportunity_user_role_type = previous_quarter.report_opportunity_user_role_type
            AND base_key.deal_size_bin = previous_quarter.deal_size_bin
            AND base_key.age_bin = previous_quarter.age_bin
            AND base_key.partner_category = previous_quarter.partner_category
            AND base_key.sales_qualified_source = previous_quarter.sales_qualified_source
            AND base_key.stage_name = previous_quarter.stage_name
            AND base_key.order_type_stamped = previous_quarter.order_type_stamped
            AND base_key.sales_type = previous_quarter.sales_type
            AND base_key.forecast_category_name = previous_quarter.forecast_category_name
            AND base_key.product_category_tier = previous_quarter.product_category_tier
            AND base_key.product_category_deployment = previous_quarter.product_category_deployment
            AND base_key.parent_crm_account_upa_country_name = previous_quarter.parent_crm_account_upa_country_name
            AND base_key.is_web_portal_purchase = previous_quarter.is_web_portal_purchase
            AND base_key.fpa_master_bookings_flag = previous_quarter.fpa_master_bookings_flag
            AND base_key.report_date = DATEADD(MONTH, 3, previous_quarter.pipeline_created_fiscal_quarter_date)
            AND base_key.industry = previous_quarter.industry
            AND base_key.pipeline_landing_quarter = previous_quarter.pipeline_landing_quarter
            AND base_key.lam_dev_count_bin = previous_quarter.lam_dev_count_bin
            AND base_key.current_stage_age_bin = previous_quarter.current_stage_age_bin
    LEFT JOIN aggregated_base AS previous_year
        ON
            base_key.owner_id = previous_year.owner_id
            AND base_key.account_id = previous_year.account_id
            AND base_key.report_opportunity_user_business_unit = previous_year.report_opportunity_user_business_unit
            AND base_key.report_opportunity_user_sub_business_unit = previous_year.report_opportunity_user_sub_business_unit
            AND base_key.report_opportunity_user_division = previous_year.report_opportunity_user_division
            AND base_key.report_opportunity_user_asm = previous_year.report_opportunity_user_asm
            AND base_key.report_opportunity_user_role_type = previous_year.report_opportunity_user_role_type
            AND base_key.deal_size_bin = previous_year.deal_size_bin
            AND base_key.age_bin = previous_year.age_bin
            AND base_key.partner_category = previous_year.partner_category
            AND base_key.sales_qualified_source = previous_year.sales_qualified_source
            AND base_key.stage_name = previous_year.stage_name
            AND base_key.order_type_stamped = previous_year.order_type_stamped
            AND base_key.sales_type = previous_year.sales_type
            AND base_key.forecast_category_name = previous_year.forecast_category_name
            AND base_key.product_category_tier = previous_year.product_category_tier
            AND base_key.product_category_deployment = previous_year.product_category_deployment
            AND base_key.parent_crm_account_upa_country_name = previous_year.parent_crm_account_upa_country_name
            AND base_key.is_web_portal_purchase = previous_year.is_web_portal_purchase
            AND base_key.fpa_master_bookings_flag = previous_year.fpa_master_bookings_flag
            AND base_key.report_date = DATEADD(MONTH, 12, previous_year.pipeline_created_fiscal_quarter_date)
            AND base_key.industry = previous_year.industry
            AND base_key.pipeline_landing_quarter = previous_year.pipeline_landing_quarter
            AND base_key.lam_dev_count_bin = previous_year.lam_dev_count_bin
            AND base_key.current_stage_age_bin = previous_year.current_stage_age_bin

),

final AS (

    SELECT
        final.*,

        COALESCE(pipe_gen_date.fiscal_year = report_date.current_fiscal_year, FALSE)                  AS is_cfy_flag,
        COALESCE(final.pipeline_created_date = current_fiscal_quarter_date, FALSE)                    AS is_cfq_flag,

        COALESCE(final.pipeline_created_date = DATEADD(MONTH, 3, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_1_flag,

        COALESCE(final.pipeline_created_date = DATEADD(MONTH, 6, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_2_flag,

        COALESCE(
            pipeline_created_date <= current_fiscal_quarter_date
            AND pipeline_created_date >= DATEADD(MONTH, -15, current_fiscal_quarter_date), FALSE
        )                                                                                             AS is_pipe_gen_range_flag,

        COALESCE(
            is_open = TRUE
            AND is_stage_1_plus = 1, FALSE
        )                                                                                             AS is_open_stage_1_plus,

        COALESCE(
            is_open = TRUE
            AND is_stage_3_plus = 1, FALSE
        )                                                                                             AS is_open_stage_3_plus,

        fiscal_year                                                                                   AS pipeline_created_fiscal_year,
        fiscal_quarter_name_fy                                                                        AS pipeline_created_fiscal_quarter_name,

        LOWER(
            CONCAT(
                report_opportunity_user_business_unit,
                '_', report_opportunity_user_sub_business_unit,
                '_', report_opportunity_user_division,
                '_', report_opportunity_user_asm,
                '_', sales_qualified_source,
                '_', deal_group
            )
        )                                                                                             AS key_bu_subbu_division_asm_sqs_ot,

        LOWER(
            CONCAT(
                report_opportunity_user_business_unit,
                '_', report_opportunity_user_sub_business_unit
            )
        )                                                                                             AS key_bu_subbu
    FROM consolidated AS final
    CROSS JOIN report_date
    LEFT JOIN date_details AS pipe_gen_date
        ON pipe_gen_date.date_actual = final.pipeline_created_date
    WHERE (
        net_arr != 0
        OR prev_quarter_net_arr != 0
        OR prev_year_net_arr != 0
    )
    AND pipeline_created_date <= report_date.current_fiscal_quarter_date
)

SELECT *
FROM final
