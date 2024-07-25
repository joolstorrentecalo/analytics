{{ config(
    tags=["mnpi_exception"]
) }}


WITH unioned AS (

  {{ union_tables(
    [
        ref('mart_crm_opportunity_7th_day_weekly_snapshot'),
        ref('mart_crm_opportunity_7th_day_weekly_snapshot_aggregate'),
        ref('mart_targets_actuals_7th_day_weekly_snapshot')
    ],
    filters={
        'mart_crm_opportunity_7th_day_weekly_snapshot': 'is_current_snapshot_quarter = true',
        'mart_crm_opportunity_7th_day_weekly_snapshot_aggregate': 'is_current_snapshot_quarter = false'
    }
) }}

)

SELECT 
  unioned.*,
  NULL AS sao_crm_opp_owner_sales_segment_stamped,
  NULL AS sao_crm_opp_owner_sales_segment_stamped_grouped,
  NULL AS sao_crm_opp_owner_geo_stamped,
  NULL AS sao_crm_opp_owner_region_stamped,
  NULL AS sao_crm_opp_owner_area_stamped,
  NULL AS sao_crm_opp_owner_segment_region_stamped_grouped,
  NULL AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
  NULL AS crm_opp_owner_stamped_name,
  NULL AS crm_account_owner_stamped_name,
  NULL AS crm_opp_owner_sales_segment_stamped,
  NULL AS crm_opp_owner_sales_segment_stamped_grouped,
  NULL AS crm_opp_owner_geo_stamped,
  NULL AS crm_opp_owner_region_stamped,
  NULL AS crm_opp_owner_area_stamped,
  NULL AS crm_opp_owner_business_unit_stamped,
  NULL AS crm_opp_owner_sales_segment_region_stamped_grouped,
  NULL AS crm_opp_owner_sales_segment_geo_region_area_stamped,
  NULL AS crm_opp_owner_user_role_type_stamped,
  NULL AS opp_owner_name,
  NULL AS crm_user_sales_segment,
  NULL AS crm_user_sales_segment_grouped,
  NULL AS crm_user_geo,
  NULL AS crm_user_region,
  NULL AS crm_user_area,
  NULL AS crm_user_business_unit,
  NULL AS crm_user_sales_segment_region_grouped,
  NULL AS account_owner_name,
  NULL AS crm_account_user_sales_segment,
  NULL AS crm_account_user_sales_segment_grouped,
  NULL AS crm_account_user_geo,
  NULL AS crm_account_user_region,
  NULL AS crm_account_user_area,
  NULL AS crm_account_user_sales_segment_region_grouped,
  NULL AS opportunity_owner_user_segment,
  NULL AS opportunity_owner_user_geo,
  NULL AS opportunity_owner_user_region,
  NULL AS opportunity_owner_user_area,
  NULL AS report_opportunity_user_segment,
  NULL AS report_opportunity_user_geo,
  NULL AS report_opportunity_user_region,
  NULL AS report_opportunity_user_area,
  NULL AS report_user_segment_geo_region_area,
  NULL AS report_user_segment_geo_region_area_sqs_ot,
  NULL AS key_segment,
  NULL AS key_sqs,
  NULL AS key_ot,
  NULL AS key_segment_sqs,
  NULL AS key_segment_ot,
  NULL AS key_segment_geo,
  NULL AS key_segment_geo_sqs,
  NULL AS key_segment_geo_ot,
  NULL AS key_segment_geo_region,
  NULL AS key_segment_geo_region_sqs,
  NULL AS key_segment_geo_region_ot,
  NULL AS key_segment_geo_region_area,
  NULL AS key_segment_geo_region_area_sqs,
  NULL AS key_segment_geo_region_area_ot,
  NULL AS key_segment_geo_area,
  NULL AS sales_team_cro_level,
  NULL AS sales_team_rd_asm_level,
  NULL AS sales_team_vp_level,
  NULL AS sales_team_avp_rd_level,
  NULL AS sales_team_asm_level,
  NULL AS account_owner_user_segment,
  NULL AS account_owner_user_geo,
  NULL AS account_owner_user_region,
  NULL AS account_owner_user_area,
  MAX(CASE 
        WHEN source != 'targets_actuals' AND snapshot_date < CURRENT_DATE() THEN snapshot_date 
        ELSE NULL 
    END) OVER ()                                                        AS max_snapshot_date, -- We want to ensure we have the max_snapshot_date that comes from the actuals in every row but excluding the future dates we have in the targets data 
  FLOOR((DATEDIFF(day, current_first_day_of_fiscal_quarter, max_snapshot_date) / 7)) 
                                                                        AS most_recent_snapshot_week
FROM unioned 
WHERE snapshot_fiscal_quarter_date >= DATEADD(QUARTER, -9, CURRENT_DATE())
