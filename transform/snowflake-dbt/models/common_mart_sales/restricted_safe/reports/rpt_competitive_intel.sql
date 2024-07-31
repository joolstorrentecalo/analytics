with opportunity AS (
select *
from {{ ref('mart_crm_opportunity')}}

),close AS (
select *
from {{ref('dim_date')}}

)


SELECT
  opportunity.dim_crm_opportunity_id,
  opportunity.opportunity_name,
  opportunity.competitors,
  opportunity.deal_path_name,
  COALESCE(
    opportunity.competitors_jenkins_flag
    > 0, FALSE)
    AS jenkins_flag,
  COALESCE(
    opportunity.competitors_atlassian_flag
    > 0, FALSE)
    AS atlassian_flag,
  opportunity.net_arr,
  opportunity.growth_type,
  opportunity.stage_name,
  opportunity.order_type,
  opportunity.order_type_grouped,
  opportunity.created_date,
  opportunity.close_date,
  opportunity.close_fiscal_quarter_name,
  opportunity.close_fiscal_year,
  opportunity.sales_qualified_source_name,
  opportunity.sales_qualified_source_grouped,
  opportunity.reason_for_loss,
  opportunity.calculated_deal_count,
  COALESCE((
    opportunity.competitors_github_enterprise_flag
    + opportunity.competitors_github_flag
  )
  > 0, FALSE)
    AS competitors_github_flag,
  COALESCE((
    opportunity.competitors_bitbucket_flag
    + opportunity.competitors_bitbucket_server_flag
  )
  > 0, FALSE)
    AS competitors_bitbucket_all_flag,
  COALESCE((
    opportunity.competitors_azure_devops_flag
    + opportunity.competitors_azure_flag
    + opportunity.competitors_github_enterprise_flag
    + opportunity.competitors_github_flag
  )
  > 0, FALSE)
    AS competitors_microsoft_flag,
  COALESCE(
    opportunity.competitors_atlassian_flag = 0
    AND competitors_microsoft_flag = 0
    AND opportunity.competitors IS NOT NULL
    AND opportunity.competitors != 'None', FALSE)
    AS competition_no_micro_atl,
  COALESCE(
    opportunity.competitors IS NOT NULL
    AND opportunity.competitors != 'None', FALSE)
    AS all_competitive_opps,
  COALESCE(
    opportunity.competitors IS NULL
    OR opportunity.competitors != 'None', FALSE)
    AS no_comp_opps,
  opportunity.report_area                           AS area,
  opportunity.report_geo                            AS geo,
  opportunity.report_region                         AS region,
  opportunity.report_segment                        AS segment,
  opportunity.report_role_level_1                   AS role_level_1,
  opportunity.report_role_level_2                   AS role_level_2,
  opportunity.report_role_level_3                   AS role_level_3,
  opportunity.report_role_level_4                   AS role_level_4,
  CASE
    WHEN opportunity.net_arr < 0
      THEN 'Churn/Contraction'
    WHEN opportunity.net_arr >= 0 AND opportunity.net_arr <= 25000
      THEN '$0-$25K'
    WHEN opportunity.net_arr > 25000 AND opportunity.net_arr <= 100000
      THEN '$25K-$100K'
    WHEN opportunity.net_arr > 100000 AND opportunity.net_arr <= 1000000
      THEN '$100K-$1M'
    ELSE '>$1M'
  END
    AS deal_size_group,
  CASE
    WHEN
      opportunity.order_type IN (
        '4. Contraction', '5. Churn - Partial', '6. Churn - Final'
      )
      AND opportunity.military_invasion_risk_scale IS NOT NULL
      THEN 'Military Invasion'
    WHEN
      opportunity.order_type IN ('4. Contraction', '5. Churn - Partial')
      THEN 'Contraction'
    WHEN opportunity.order_type = '6. Churn - Final' THEN 'Churn'
  END
    AS loss_type,
  CASE
    WHEN
      loss_type = 'Military Invasion'
      THEN 'Military Invasion'
    WHEN
      opportunity.reason_for_loss = 'Corporate Decision'
      THEN 'Top Down Executive Decision'
    WHEN
      opportunity.reason_for_loss IN (
        'Lack of Engagement / Sponsor', 'Evangelist Left', 'Went Silent'
      )
      THEN 'Lack of Customer Engagement or Sponsor'
    WHEN
      opportunity.reason_for_loss IN (
        'Budget/Value Unperceived', 'Product Value / Gaps'
      )
      THEN 'Product Features / Value Gaps'
    WHEN opportunity.reason_for_loss = 'Other' THEN 'Unknown'
    WHEN
      opportunity.reason_for_loss IN (
        'Insuficient funds', 'Loss of Budget'
      )
      THEN 'Lack of Budget'
    WHEN
      opportunity.reason_for_loss = 'Product quality/availability'
      THEN 'Product Quality / Availability'
    WHEN
      opportunity.order_type IN (
        '4. Contraction', '5. Churn - Partial', '6. Churn - Final'
      )
      AND opportunity.reason_for_loss IS NULL
      THEN 'Unknown'
    ELSE opportunity.reason_for_loss
  END
    AS reason_for_loss_mapped_to_new_codes,
  CASE
    WHEN opportunity.is_won = TRUE THEN opportunity.calculated_deal_count
  END                                             AS wondeals_dealsum,
  opportunity.net_arr
  * opportunity.calculated_deal_count             AS net_arr_final,
  DATEDIFF(
    QUARTER,
    close.current_first_day_of_fiscal_quarter,
    opportunity.close_fiscal_quarter_date
  )                                               AS relative_quarter
FROM opportunity
LEFT JOIN  close
    ON opportunity.close_date = close.date_actual
WHERE
    AND opportunity.is_closed = true
    AND opportunity.is_edu_oss = false
    AND opportunity.is_jihu_account = false
    AND coalesce(opportunity.reason_for_loss, 'null')
    != 'Merged into another opportunity'
    AND opportunity.sales_qualified_source_name != 'Web Direct Generated'
    AND opportunity.parent_crm_account_geo != 'JIHU'
    AND opportunity.stage_name != '9-Unqualified'
    AND opportunity.stage_name != '10-Duplicate'

