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
        AND close_fiscal_year >= current_fiscal_year
),

detail AS (
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
        COALESCE(is_open = 1, FALSE)                        AS is_open,
        is_stage_1_plus,
        is_stage_3_plus,
        fpa_master_bookings_flag,

        -----------------------------------------------
        -- Dimensions for Detail

        is_eligible_created_pipeline_flag,
        opportunity_id,
        opportunity_name,

        -----------------------------------------------
        -- Date dimensions Detail
        close_date,
        close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        close_fiscal_year,
        created_date,
        created_fiscal_quarter_name,
        created_fiscal_year,
        pipeline_created_date,
        pipeline_created_fiscal_quarter_name,
        pipeline_created_fiscal_year,
        -----------------------------------------------
        -- Measures for Detail / Aggregated

        net_arr,
        open_1plus_net_arr,
        booked_net_arr,
        booked_churned_contraction_net_arr,

        calculated_deal_count                               AS deal_count,
        booked_deal_count,
        booked_churned_contraction_deal_count,
        cycle_time_in_days                                  AS age_in_days,



        total_professional_services_value,
        total_book_professional_services_value,
        total_lost_professional_services_value,
        total_open_professional_services_value,

        lam_dev_count




    FROM sfdc_opportunity_xf


),

final AS (

    SELECT
        final.*,

        COALESCE(close_date.fiscal_year = report_date.current_fiscal_year, FALSE)                   AS is_cfy_flag,
        COALESCE(close_fiscal_quarter_date = current_fiscal_quarter_date, FALSE)                    AS is_cfq_flag,

        COALESCE(close_fiscal_quarter_date = DATEADD(MONTH, 3, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_1_flag,

        COALESCE(close_fiscal_quarter_date = DATEADD(MONTH, 6, current_fiscal_quarter_date), FALSE) AS is_cfq_plus_2_flag,

        COALESCE(
            close_date >= current_fiscal_quarter_date
            AND close_date <= DATEADD(MONTH, 15, current_fiscal_quarter_date), FALSE
        )                                                                                           AS is_open_pipeline_range_flag,
        COALESCE(
            close_date <= current_fiscal_quarter_date
            AND close_date >= DATEADD(MONTH, -15, current_fiscal_quarter_date), FALSE
        )                                                                                           AS is_bookings_range_flag,

        COALESCE(
            is_open = TRUE
            AND is_stage_1_plus = 1, FALSE
        )                                                                                           AS is_open_stage_1_plus,

        COALESCE(
            is_open = TRUE
            AND is_stage_3_plus = 1, FALSE
        )                                                                                           AS is_open_stage_3_plus,

        LOWER(
            CONCAT(
                report_opportunity_user_business_unit,
                '_', report_opportunity_user_sub_business_unit,
                '_', report_opportunity_user_division,
                '_', report_opportunity_user_asm,
                '_', sales_qualified_source,
                '_', deal_group
            )
        )                                                                                           AS key_bu_subbu_division_asm_sqs_ot,

        LOWER(
            CONCAT(
                report_opportunity_user_business_unit,
                '_', report_opportunity_user_sub_business_unit
            )
        )                                                                                           AS key_bu_subbu
    FROM detail AS final
    CROSS JOIN report_date
    LEFT JOIN date_details AS close_date
        ON close_date.date_actual = final.close_date
    WHERE (
        net_arr != 0
        OR booked_net_arr != 0
        OR total_professional_services_value != 0
    )
)

SELECT *
FROM final
