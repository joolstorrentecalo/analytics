WITH daily_targets AS (
    
    SELECT * 
    FROM {{ ref('wk_fct_sales_funnel_target_daily') }}
    
),    

quarterly_targets AS ( 

    SELECT 
        dim_order_type_id,
        dim_sales_qualified_source_id,
        dim_crm_user_hierarchy_sk,
        fiscal_quarter_name,
        SUM(daily_allocated_target) AS total_quarter_target
    FROM daily_targets
    WHERE kpi_name = 'Net ARR Company'
    GROUP BY 1,2,3,4

)

SELECT 
    daily_targets.target_date,
    daily_targets.dim_order_type_id,
    daily_targets.dim_sales_qualified_source_id,
    daily_targets.dim_crm_user_hierarchy_sk,
    daily_targets.fiscal_quarter_name,
    quarterly_targets.total_quarter_target,
    daily_targets.qtd_allocated_target
FROM daily_targets
LEFT JOIN quarterly_targets 
    ON daily_targets.fiscal_quarter_name = quarterly_targets.fiscal_quarter_name
        AND daily_targets.dim_order_type_id = quarterly_targets.dim_order_type_id
            AND daily_targets.dim_sales_qualified_source_id = quarterly_targets.dim_sales_qualified_source_id
                AND daily_targets.dim_crm_user_hierarchy_sk = quarterly_targets.dim_crm_user_hierarchy_sk
WHERE kpi_name = 'Net ARR Company'
