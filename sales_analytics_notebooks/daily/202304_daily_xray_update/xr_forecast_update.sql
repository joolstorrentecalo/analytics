WITH RECURSIVE date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

),

current_quarter AS (

    SELECT
        first_day_of_fiscal_quarter                                           AS current_fiscal_quarter_date,
        fiscal_quarter_name_fy                                                AS current_fiscal_quarter_name,
        CAST(fiscal_year AS STRING) || '-Q' || CAST(fiscal_quarter AS STRING) AS current_adjusted_fiscal_quarter_name
    FROM date_details
    WHERE date_actual = CURRENT_DATE

),

sfdc_user AS (

    SELECT *
    FROM prod.workspace_sales.sfdc_users_xf

),

sales_hierarchy AS (

    SELECT
        0                                       AS level_depth,
        CAST(level_depth AS STRING) || ' Level' AS level_name,
        manager_id,
        manager_name,
        user_id,
        name,
        title,
        role_name,
        key_bu,
        key_bu_subbu,
        key_bu_subbu_division,
        key_bu_subbu_division_asm
    FROM sfdc_user
    WHERE
        role_name = 'CRO'
        AND is_active = true
    UNION ALL
    SELECT
        manager.level_depth + 1                     AS level_depth_add,
        CAST(level_depth_add AS STRING) || ' Level' AS level_name,
        users.manager_id,
        users.manager_name,
        users.user_id,
        users.name,
        users.title,
        users.role_name,
        users.key_bu,
        users.key_bu_subbu,
        users.key_bu_subbu_division,
        users.key_bu_subbu_division_asm
    FROM sfdc_user AS users
    INNER JOIN sales_hierarchy AS manager
        ON manager.user_id = users.manager_id
    WHERE
        is_active = true
        AND users.key_bu != 'other'

),

clari_forecast AS (

    SELECT *
    FROM "PROD"."RESTRICTED_SAFE_WORKSPACE_SALES"."WK_SALES_CLARI_NET_ARR_FORECAST"

),

max_report_week_per_quarter AS (

    SELECT
        fiscal_quarter,
        crm_user_id,
        field_name,
        MAX(week_number) AS week_number
    FROM clari_forecast
    GROUP BY 1, 2, 3

),

current_most_likely_forecast AS (

    SELECT forecast.*
    FROM clari_forecast AS forecast
    INNER JOIN max_report_week_per_quarter AS max_week
        ON
            max_week.field_name = forecast.field_name
            AND max_week.crm_user_id = forecast.crm_user_id
            AND max_week.fiscal_quarter = forecast.fiscal_quarter
            AND max_week.week_number = forecast.week_number
    INNER JOIN current_quarter
        ON current_quarter.current_adjusted_fiscal_quarter_name = forecast.fiscal_quarter
    WHERE forecast.field_name = 'Net Most Likely'

),

forecast_and_hierarchy AS (

    SELECT
        sales_hierarchy.level_depth,
        sales_hierarchy.level_name,
        sales_hierarchy.manager_id,
        sales_hierarchy.manager_name,
        sales_hierarchy.user_id,
        sales_hierarchy.name,
        sales_hierarchy.title,
        sales_hierarchy.role_name,
        sales_hierarchy.key_bu,
        sales_hierarchy.key_bu_subbu,
        sales_hierarchy.key_bu_subbu_division,
        sales_hierarchy.key_bu_subbu_division_asm,
        most_likely.week_number,
        most_likely.forecast_value
    FROM sales_hierarchy
    INNER JOIN current_most_likely_forecast AS most_likely
        ON most_likely.crm_user_id = sales_hierarchy.user_id
    WHERE level_depth != 0
    UNION ALL
    -- as we do not have a call for the cro level we adjust it using its directs
    -- values
    SELECT
        cro.level_depth,
        cro.level_name,
        cro.manager_id,
        cro.manager_name,
        cro.user_id,
        cro.name,
        cro.title,
        cro.role_name,
        cro.key_bu,
        cro.key_bu_subbu,
        cro.key_bu_subbu_division,
        cro.key_bu_subbu_division_asm,
        most_likely.week_number,
        SUM(most_likely.forecast_value) AS forecast_value
    FROM sales_hierarchy
    CROSS JOIN (
        SELECT *
        FROM sales_hierarchy
        WHERE level_depth = 0
    ) AS cro
    INNER JOIN current_most_likely_forecast AS most_likely
        ON most_likely.crm_user_id = sales_hierarchy.user_id
    WHERE sales_hierarchy.level_depth = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

),

extra_keys AS (

    SELECT
        *,
        CASE
            WHEN level_depth = 0
                THEN 'global'
            WHEN
                level_depth = 1
                AND LOWER(key_bu_subbu) NOT IN ('entg_amer')
                THEN key_bu_subbu
            WHEN
                level_depth = 1
                AND LOWER(key_bu_subbu) IN ('entg_amer')
                THEN key_bu_subbu_division
            WHEN
                level_depth = 2
                AND (
                    LOWER(key_bu_subbu) IN ('entg_emea', 'entg_apac')
                    OR LOWER(key_bu_subbu_division) = 'entg_pubsec_sled'
                )
                THEN key_bu_subbu_division
            WHEN
                LOWER(key_bu_subbu_division_asm) NOT LIKE ('%other%')
                AND LOWER(key_bu_subbu_division_asm) NOT LIKE ('%all%')
                THEN key_bu_subbu_division_asm
        END AS key_value
    FROM forecast_and_hierarchy
    WHERE
        role_name NOT LIKE 'AE_%'
        AND role_name NOT LIKE 'TL_%'

)

SELECT *
FROM extra_keys
