{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), promotions AS (

    SELECT *
    FROM {{ ref('bamboohr_promotions_xf') }}

), sheetload_people_budget AS (

    SELECT *
    FROM {{ ref('sheetload_people_budget') }}
  
), budget AS (

    SELECT 
      CASE WHEN division = 'Engineering_Meltano'
            THEN 'Engineering/Meltano'
           WHEN division ='People_CEO'
            THEN 'People Group/CEO'
           WHEN division = 'Marketing'
            THEN 'Marketing - Including SDR'
           ELSE division END                          AS division,
      fiscal_year,
      fiscal_quarter,
      budget,
      excess_from_previous_quarter
    FROM sheetload_people_budget

    UNION ALL

    SELECT 
      'Total - Including SDR'                                   AS division,
      fiscal_year,
      fiscal_quarter, 
      SUM(budget)                                               AS budget,
      SUM(excess_from_previous_quarter)                         AS excess_from_previous_quarter
    FROM sheetload_people_budget
    GROUP BY 1,2,3

    UNION ALL
    
    SELECT 
      'Total - Excluding SDR'                                   AS division,
      fiscal_year,
      fiscal_quarter, 
      SUM(budget)                                               AS budget,
      SUM(excess_from_previous_quarter)                         AS excess_from_previous_quarter
    FROM sheetload_people_budget
    WHERE division != 'Sales Development'
    GROUP BY 1,2,3

), promotions_aggregated AS (

    SELECT
      division_grouping                                         AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                 AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    GROUP BY 1,2,3

    UNION ALL

    SELECT
      'Marketing - Excluding SDR'                               AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                 AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    WHERE division = 'Marketing'
      AND department != 'Sales Development'
    GROUP BY 1,2,3

    UNION ALL

    SELECT
      'Sales Development'                                       AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                 AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    WHERE division = 'Marketing'
      AND department = 'Sales Development'
    GROUP BY 1,2,3

    UNION ALL 

    SELECT
      'Total - Including SDR'                                  AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    GROUP BY 1,2,3

    UNION ALL

    SELECT
      'Total - Excluding SDR'                                  AS division,
      dates.fiscal_year,
      dates.fiscal_quarter,
      SUM(total_change_in_comp)                                AS total_spend
    FROM promotions
    LEFT JOIN dates
      ON promotions.promotion_month = dates.date_actual
    WHERE department != 'Sales Development'
    GROUP BY 1,2,3
  
), final AS (

    SELECT 
      'FY' || budget.fiscal_year || ' - Q' || budget.fiscal_quarter AS fiscal_quarter_name,
      budget.*,
      promotions_aggregated.division                                AS division_promotions_grouping,
      promotions_aggregated.total_spend
    FROM budget
    LEFT JOIN promotions_aggregated
      ON budget.division = promotions_aggregated.division
      AND budget.fiscal_year = promotions_aggregated.fiscal_year
      AND budget.fiscal_quarter = promotions_aggregated.fiscal_quarter

)

SELECT *
FROM final
