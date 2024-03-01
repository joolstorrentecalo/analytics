with fiscal_quarter_target as
(select 
dim_order_type_id
,dim_sales_qualified_source_id
,dim_crm_user_hierarchy_sk
,fiscal_Quarter_name
,sum(daily_allocated_target) as total_quarter_target
from PROD.RESTRICTED_SAFE_WORKSPACE_SALES.WK_FCT_SALES_FUNNEL_TARGET_DAILY
where kpi_name = 'Net ARR Company'
group by 1,2,3,4)

select 
td.dim_order_type_id
,td.dim_sales_qualified_source_id
,td.dim_crm_user_hierarchy_sk
,td.fiscal_quarter_name 
,td.target_date
,qtd_allocated_target
,total_quarter_target
from
PROD.RESTRICTED_SAFE_WORKSPACE_SALES.WK_FCT_SALES_FUNNEL_TARGET_DAILY td
left join fiscal_quarter_target tq on td.fiscal_quarter_name = tq.fiscal_quarter_name
    and td.dim_order_type_id = tq.dim_order_type_id
    and td.dim_sales_qualified_source_id = tq.dim_sales_qualified_source_id
    and td.dim_crm_user_hierarchy_sk = tq.dim_crm_user_hierarchy_sk
where kpi_name = 'Net ARR Company'
