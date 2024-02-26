WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

),

target_date AS (

    SELECT
        day_of_fiscal_quarter_normalised AS current_day_of_fiscal_quarter_normalised,
        first_day_of_fiscal_quarter      AS current_fiscal_quarter_date,
        fiscal_year                      AS current_fiscal_year
    FROM date_details
    WHERE date_actual = CURRENT_DATE

),

dim_users AS (

    SELECT
        user.*,
        user.role_type                                                    AS user_role_type,
        LOWER(user.business_unit) || '_' || LOWER(user.sub_business_unit) AS user_segment_region
    FROM prod.workspace_sales.sfdc_users_xf AS user
    WHERE
        user.is_active = 1
        AND (
            LOWER(user.title) LIKE '%strategic account%'
            OR LOWER(user.title) LIKE '%account executive%'
            OR LOWER(user.title) LIKE '%country manager%'
            OR LOWER(user.title) LIKE '%public sector channel manager%'
        )

),

sfdc_opportunity_xf AS (

    SELECT
        opp.*,
        acc.is_key_account
    FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf AS opp
    INNER JOIN prod.restricted_safe_common_mart_sales.mart_crm_account AS acc
        ON acc.dim_crm_account_id = opp.account_id
    WHERE
        opp.opportunity_id NOT IN ('0064M00000XTr9zQAD') -- EXCLUDED DEAL FROM APAC, IN FY22-Q1 it would show under Tae Ho Hyun, when this deal should not be considered for his performance

),

current_quarter AS (

    SELECT
        --------------------------
        -- keys
        opp.owner_id,
        --------------------------
        -- reported quarter
        SUM(opp.booked_deal_count)                    AS qtd_booked_deal_count,
        SUM(opp.open_1plus_deal_count)                AS qtd_open_1plus_deal_count,
        SUM(opp.open_3plus_deal_count)                AS qtd_open_3plus_deal_count,
        SUM(opp.open_4plus_deal_count)                AS qtd_open_4plus_deal_count,

        ------------------------------
        -- Net ARR
        -- Use Net ARR instead
        -- created and closed

        -- reported quarter
        SUM(opp.booked_net_arr)                       AS qtd_booked_net_arr,
        SUM(opp.open_1plus_net_arr)                   AS qtd_open_1plus_net_arr,
        SUM(opp.open_3plus_net_arr)                   AS qtd_open_3plus_net_arr,
        SUM(opp.open_4plus_net_arr)                   AS qtd_open_4plus_net_arr,

        SUM(opp.created_and_won_same_quarter_net_arr) AS qtd_created_and_won_same_quarter_net_arr


    FROM sfdc_opportunity_xf AS opp
    INNER JOIN target_date
        ON opp.close_fiscal_quarter_date = target_date.current_fiscal_quarter_date
    GROUP BY 1

),

rq_plus_1 AS (

    SELECT
        --------------------------
        -- keys
        opp.owner_id,
        --------------------------
        SUM(opp.open_1plus_deal_count) AS rq_plus_1_open_1plus_deal_count,
        SUM(opp.open_3plus_deal_count) AS rq_plus_1_open_3plus_deal_count,
        SUM(opp.open_4plus_deal_count) AS rq_plus_1_open_4plus_deal_count,
        SUM(opp.open_1plus_net_arr)    AS rq_plus_1_open_1plus_net_arr,
        SUM(opp.open_3plus_net_arr)    AS rq_plus_1_open_3plus_net_arr,
        SUM(opp.open_4plus_net_arr)    AS rq_plus_1_open_4plus_net_arr
    FROM sfdc_opportunity_xf AS opp
    INNER JOIN target_date
        ON opp.close_fiscal_quarter_date = DATEADD(MONTH, 3, current_fiscal_quarter_date)
    GROUP BY 1


),

rq_plus_2 AS (

    SELECT
        --------------------------
        -- keys
        opp.owner_id,
        --------------------------
        SUM(opp.open_1plus_deal_count) AS rq_plus_2_open_1plus_deal_count,
        SUM(opp.open_3plus_deal_count) AS rq_plus_2_open_3plus_deal_count,
        SUM(opp.open_4plus_deal_count) AS rq_plus_2_open_4plus_deal_count,
        SUM(opp.open_1plus_net_arr)    AS rq_plus_2_open_1plus_net_arr,
        SUM(opp.open_3plus_net_arr)    AS rq_plus_2_open_3plus_net_arr,
        SUM(opp.open_4plus_net_arr)    AS rq_plus_2_open_4plus_net_arr
    FROM sfdc_opportunity_xf AS opp
    INNER JOIN target_date
        ON opp.close_fiscal_quarter_date = DATEADD(MONTH, 6, current_fiscal_quarter_date)
    GROUP BY 1

),

ytd_metrics AS (

    SELECT
        owner_id,
        SUM(booked_net_arr)        AS ytd_booked_net_arr,
        SUM(open_1plus_net_arr)    AS fy_open_1plus_net_arr,
        SUM(open_3plus_net_arr)    AS fy_open_3plus_net_arr,
        SUM(open_4plus_net_arr)    AS fy_open_4plus_net_arr,

        SUM(booked_deal_count)     AS ytd_booked_deal_count,
        SUM(open_1plus_deal_count) AS fy_open_1plus_deal_count,
        SUM(open_3plus_deal_count) AS fy_open_3plus_deal_count,
        SUM(open_4plus_deal_count) AS fy_open_4plus_deal_count,

        -- NF: 20220615 Extended Metrics requested by EMEA Ent.
        -- Open deals with more than 1M$ net arr
        SUM(CASE
            WHEN open_1plus_net_arr >= 1000000
                THEN open_1plus_net_arr
            ELSE 0
        END)                       AS fy_open_1plus_over_1m_net_arr,

        SUM(CASE
            WHEN open_1plus_net_arr >= 1000000
                THEN open_1plus_deal_count
            ELSE 0
        END)                       AS fy_open_1plus_over_1m_deal_count,


        -- Open dealsbetween 500k and 1M$ net arr
        SUM(CASE
            WHEN
                open_1plus_net_arr < 1000000
                AND open_1plus_net_arr >= 500000
                THEN open_1plus_net_arr
            ELSE 0
        END)                       AS fy_open_1plus_between_500k_1m_net_arr,

        SUM(CASE
            WHEN
                open_1plus_net_arr < 1000000
                AND open_1plus_net_arr >= 500000
                THEN open_1plus_deal_count
            ELSE 0
        END)                       AS fy_open_1plus_between_500k_1m_deal_count,


        SUM(CASE
            WHEN
                open_1plus_net_arr < 500000
                AND open_1plus_net_arr >= 100000
                THEN open_1plus_net_arr
            ELSE 0
        END)                       AS fy_open_1plus_between_100k_500k_net_arr,

        SUM(CASE
            WHEN
                open_1plus_net_arr < 500000
                AND open_1plus_net_arr >= 100000
                THEN open_1plus_deal_count
            ELSE 0
        END)                       AS fy_open_1plus_between_100k_500k_deal_count,


        -- open net arr with accounts flagged as key accounts
        SUM(CASE
            WHEN is_key_account = 1
                THEN open_1plus_net_arr
            ELSE 0
        END)                       AS fy_open_1plus_on_key_account_net_arr


    FROM sfdc_opportunity_xf AS opp
    INNER JOIN target_date
        ON opp.close_fiscal_year = target_date.current_fiscal_year
    GROUP BY 1

),

pipe_gen_metrics AS (

    SELECT
        owner_id,
        SUM(net_arr)               AS qtd_pipeline_generated_net_arr,
        SUM(CASE
            WHEN close_fiscal_quarter_date = pipeline_created_fiscal_quarter_date
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_cq_to_close_on_cq_net_arr,

        -- Created to Close after CQ
        SUM(CASE
            WHEN close_fiscal_quarter_date >= DATEADD(MONTH, 3, pipeline_created_fiscal_quarter_date)
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_cq_to_close_on_rq_plus_1_plus_net_arr,


        SUM(CASE
            WHEN close_fiscal_quarter_date = DATEADD(MONTH, 3, pipeline_created_fiscal_quarter_date)
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_cq_to_close_on_rq_plus_1_net_arr,
        SUM(CASE
            WHEN close_fiscal_quarter_date = DATEADD(MONTH, 6, pipeline_created_fiscal_quarter_date)
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_cq_to_close_on_rq_plus_2_net_arr,
        SUM(CASE
            WHEN close_fiscal_quarter_date >= DATEADD(MONTH, 9, pipeline_created_fiscal_quarter_date)
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_cq_to_close_on_rq_plus_3_plus_net_arr,
        SUM(CASE
            WHEN LOWER(sales_qualified_source) LIKE '%ae%'
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_by_ae_generated_net_arr,
        SUM(CASE
            WHEN LOWER(sales_qualified_source) LIKE '%sdr%'
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_by_sdr_generated_net_arr,
        SUM(CASE
            WHEN (LOWER(sales_qualified_source) LIKE '%channel%' OR LOWER(sales_qualified_source) LIKE '%partner%')
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_by_channel_generated_net_arr,
        SUM(CASE
            WHEN LOWER(sales_qualified_source) LIKE '%web%'
                THEN net_arr
            ELSE 0
        END)                       AS qtd_pipeline_created_by_web_generated_net_arr,

        SUM(calculated_deal_count) AS qtd_pipeline_generated_count


    FROM sfdc_opportunity_xf
    WHERE
        is_eligible_created_pipeline_flag = 1
        AND pipeline_created_fiscal_quarter_date = (
            SELECT first_day_of_fiscal_quarter
            FROM date_details
            WHERE date_actual = CURRENT_DATE
            GROUP BY 1
        )
    GROUP BY 1

),

duplicated_key_fields AS (
    -- NF: Not sure why this UNION is behaving as  UNION ALL (leaving duplicates)

    SELECT owner_id
    FROM current_quarter
    UNION
    SELECT owner_id
    FROM rq_plus_1
    UNION
    SELECT owner_id
    FROM rq_plus_2
    UNION
    SELECT owner_id
    FROM pipe_gen_metrics
    UNION
    SELECT owner_id
    FROM ytd_metrics

),

unique_key_fields AS (

    SELECT DISTINCT owner_id
    FROM duplicated_key_fields

),

base_fields AS (

    SELECT DISTINCT
        key_fields.*,
        close_date.fiscal_quarter_name_fy                    AS close_fiscal_quarter_name,
        close_date.first_day_of_fiscal_quarter               AS close_fiscal_quarter_date,
        close_date.fiscal_year                               AS close_fiscal_year,
        target_date.current_day_of_fiscal_quarter_normalised AS close_day_of_fiscal_quarter_normalised,
        rq_plus_1.first_day_of_fiscal_quarter                AS rq_plus_1_close_fiscal_quarter_date,
        rq_plus_1.fiscal_quarter_name_fy                     AS rq_plus_1_close_fiscal_quarter_name,
        rq_plus_2.first_day_of_fiscal_quarter                AS rq_plus_2_close_fiscal_quarter_date,
        rq_plus_2.fiscal_quarter_name_fy                     AS rq_plus_2_close_fiscal_quarter_name
    FROM unique_key_fields AS key_fields
    CROSS JOIN target_date
    INNER JOIN date_details AS close_date
        ON close_date.first_day_of_fiscal_quarter = target_date.current_fiscal_quarter_date
    LEFT JOIN date_details AS rq_plus_1
        ON rq_plus_1.date_actual = DATEADD(MONTH, 3, target_date.current_fiscal_quarter_date)
    LEFT JOIN date_details AS rq_plus_2
        ON rq_plus_2.date_actual = DATEADD(MONTH, 6, target_date.current_fiscal_quarter_date)

),

final AS (

    -----------------------------------------------------
    -----------------------------------------------------
    SELECT
        --------------------------
        -- ORDER IS CRITICAL FOR REPORT TO WORK
        --------------------------

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- keys
        user.key_sal_heatmap                                                                     AS key_owner_name,
        user.employee_number,
        user.key_bu_subbu,
        user.user_role_type,
        base_fields.close_fiscal_quarter_name,
        base_fields.close_fiscal_quarter_date,
        --------------------------

        base_fields.close_day_of_fiscal_quarter_normalised,
        base_fields.rq_plus_1_close_fiscal_quarter_name,
        base_fields.rq_plus_2_close_fiscal_quarter_name,
        COALESCE(opp.qtd_booked_net_arr, 0)                                                      AS qtd_booked_net_arr,
        COALESCE(opp.qtd_open_1plus_net_arr, 0)                                                  AS qtd_open_1plus_net_arr,

        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- Net ARR
        COALESCE(opp.qtd_open_3plus_net_arr, 0)                                                  AS qtd_open_3plus_net_arr,
        COALESCE(opp.qtd_open_4plus_net_arr, 0)                                                  AS qtd_open_4plus_net_arr,
        COALESCE(rq_plus_1.rq_plus_1_open_1plus_net_arr, 0)                                      AS rq_plus_1_open_1plus_net_arr,
        COALESCE(rq_plus_1.rq_plus_1_open_3plus_net_arr, 0)                                      AS rq_plus_1_open_3plus_net_arr,

        -- next quarter
        COALESCE(rq_plus_2.rq_plus_2_open_1plus_net_arr, 0)                                      AS rq_plus_2_open_1plus_net_arr,
        COALESCE(ytd_metrics.ytd_booked_net_arr, 0)                                              AS ytd_booked_net_arr,
        --rq_plus_1.rq_plus_1_open_4plus_net_arr,

        -- quarter plus 2
        COALESCE(ytd_metrics.fy_open_1plus_net_arr, 0)                                           AS fy_open_1plus_net_arr,
        --rq_plus_2.rq_plus_2_open_3plus_net_arr,
        --rq_plus_2.rq_plus_2_open_4plus_net_arr,

        -- fiscal year metrics
        COALESCE(ytd_metrics.fy_open_3plus_net_arr, 0)                                           AS fy_open_3plus_net_arr,
        COALESCE(ytd_metrics.fy_open_1plus_on_key_account_net_arr, 0)                            AS fy_open_1plus_on_key_account_net_arr,
        COALESCE(ytd_metrics.fy_open_1plus_over_1m_net_arr, 0)                                   AS fy_open_1plus_over_1m_net_arr,
        --ytd_metrics.fy_open_4plus_net_arr,

        COALESCE(ytd_metrics.fy_open_1plus_between_500k_1m_net_arr, 0)                           AS fy_open_1plus_between_500k_1m_net_arr,

        COALESCE(opp.qtd_created_and_won_same_quarter_net_arr, 0)                                AS qtd_created_and_won_same_quarter_net_arr,
        COALESCE(pipeline_created.qtd_pipeline_generated_net_arr, 0)                             AS qtd_pipeline_generated_net_arr,

        COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_cq_net_arr, 0)             AS qtd_pipeline_created_cq_to_close_on_cq_net_arr,

        -- generated pipeline in quarter
        COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_1_net_arr, 0)      AS qtd_pipeline_created_cq_to_close_on_rq_plus_1_net_arr,

        -- extended pipeline metrics
        COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_2_net_arr, 0)      AS qtd_pipeline_created_cq_to_close_on_rq_plus_2_net_arr,
        COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_3_plus_net_arr, 0) AS qtd_pipeline_created_cq_to_close_on_rq_plus_3_plus_net_arr,
        COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_1_plus_net_arr, 0) AS qtd_pipeline_created_cq_to_close_on_rq_plus_1_plus_net_arr,
        COALESCE(opp.qtd_booked_deal_count, 0)                                                   AS qtd_booked_deal_count,
        COALESCE(opp.qtd_open_1plus_deal_count, 0)                                               AS qtd_open_1plus_deal_count,

        COALESCE(opp.qtd_open_3plus_deal_count, 0)                                               AS qtd_open_3plus_deal_count,
        COALESCE(opp.qtd_open_4plus_deal_count, 0)                                               AS qtd_open_4plus_deal_count,
        COALESCE(rq_plus_1.rq_plus_1_open_1plus_deal_count, 0)                                   AS rq_plus_1_open_1plus_deal_count,
        COALESCE(rq_plus_1.rq_plus_1_open_3plus_deal_count, 0)                                   AS rq_plus_1_open_3plus_deal_count,


        -- calculate % of created pipeline that lands on which quarter
        COALESCE(rq_plus_2.rq_plus_2_open_1plus_deal_count, 0)                                   AS rq_plus_2_open_1plus_deal_count,
        COALESCE(pipeline_created.qtd_pipeline_generated_count, 0)                               AS qtd_pipeline_generated_count,
        COALESCE(ytd_metrics.ytd_booked_deal_count, 0)                                           AS ytd_booked_deal_count,
        COALESCE(ytd_metrics.fy_open_1plus_deal_count, 0)                                        AS fy_open_1plus_deal_count,

        -- calculate the % of created pipeline that was generated by SQS
        COALESCE(ytd_metrics.fy_open_3plus_deal_count, 0)                                        AS fy_open_3plus_deal_count,
        COALESCE(ytd_metrics.fy_open_1plus_over_1m_deal_count, 0)                                AS fy_open_1plus_over_1m_deal_count,
        COALESCE(ytd_metrics.fy_open_1plus_between_500k_1m_deal_count, 0)                        AS fy_open_1plus_between_500k_1m_deal_count,


        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- Count of deals

        -- current quarter at snapshot day
        LOWER(user.business_unit)                                                                AS user_business_unit,
        LOWER(user.user_area)                                                                    AS user_area,
        COALESCE(pipeline_created.qtd_pipeline_created_by_ae_generated_net_arr, 0)               AS qtd_pipeline_created_by_ae_generated_net_arr,

        -- next quarter
        COALESCE(pipeline_created.qtd_pipeline_created_by_sdr_generated_net_arr, 0)              AS qtd_pipeline_created_by_sdr_generated_net_arr,
        COALESCE(pipeline_created.qtd_pipeline_created_by_channel_generated_net_arr, 0)          AS qtd_pipeline_created_by_channel_generated_net_arr,
        --rq_plus_1.rq_plus_1_open_4plus_deal_count,

        -- quarter plus 2
        COALESCE(pipeline_created.qtd_pipeline_created_by_web_generated_net_arr, 0)              AS qtd_pipeline_created_by_web_generated_net_arr,
        --rq_plus_2.rq_plus_2_open_3plus_deal_count,
        --rq_plus_2.rq_plus_2_open_4plus_deal_count,

        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_cq_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_cq_to_close_on_cq_perc,

        -- fiscal year metrics
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_1_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_cq_to_close_on_rq_plus_1_perc,
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_2_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_cq_to_close_on_rq_plus_2_perc,
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_cq_to_close_on_rq_plus_3_plus_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_cq_to_close_on_rq_plus_3_plus_perc,
        --ytd_metrics.fy_open_4plus_deal_count,
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_by_ae_generated_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_by_ae_generated_perc,
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_by_sdr_generated_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_by_sdr_generated_perc,
        CASE
            WHEN pipeline_created.qtd_pipeline_generated_net_arr > 0
                THEN COALESCE(pipeline_created.qtd_pipeline_created_by_channel_generated_net_arr, 0) / pipeline_created.qtd_pipeline_generated_net_arr
            ELSE 0
        END                                                                                      AS qtd_pipeline_created_by_channel_generated_perc,

        -- last updated timestamp
        CURRENT_TIMESTAMP                                                                        AS last_updated_at

    FROM base_fields
    INNER JOIN dim_users AS user
        ON user.user_id = base_fields.owner_id
    LEFT JOIN current_quarter AS opp
        ON base_fields.owner_id = opp.owner_id
    LEFT JOIN pipe_gen_metrics AS pipeline_created
        ON base_fields.owner_id = pipeline_created.owner_id
    LEFT JOIN rq_plus_1
        ON base_fields.owner_id = rq_plus_1.owner_id
    LEFT JOIN rq_plus_2
        ON base_fields.owner_id = rq_plus_2.owner_id
    LEFT JOIN ytd_metrics
        ON base_fields.owner_id = ytd_metrics.owner_id

    WHERE
        user.is_active = 1
        --    AND user.user_segment in ('Large', 'PubSec')
        AND user.employee_number IS NOT NULL
-- AND user.user_region IN ('EMEA','APAC')

)

SELECT final.*
FROM final
--WHERE key_owner_name = 'entg_emea_neur_norben_str_12878'
