WITH base AS (
  SELECT --create date and close date
    dim_crm_opportunity_id,
    CASE WHEN stage_name = '7 - Closing' THEN '7-Closing'
      WHEN stage_name = 'Closed Lost' THEN '8-Closed Lost'
      ELSE stage_name
    END                AS stage_name,
    created_date   AS created_date,
    MIN(snapshot_date) AS stage_date
  FROM {{ ref('mart_crm_opportunity_daily_snapshot') }}
  WHERE sales_qualified_source_name != 'Web Direct Generated'
    AND created_date >= '2020-02-01'
    AND sales_type != 'Renewal'
    AND is_web_portal_purchase = FALSE
    AND opportunity_category NOT IN ('Decommission', 'Internal Correction')
    AND LOWER(opportunity_name) NOT LIKE '%rebook%'
    AND net_arr > 0
    AND (is_eligible_open_pipeline = 1 OR LOWER(stage_name) LIKE '%clos%')
  GROUP BY 1, 2, 3
),

stage_base AS (
  SELECT
    dim_crm_opportunity_id,
    CASE WHEN stage_name = '0-Pending Acceptance' THEN 'stage0'
      WHEN stage_name = '1-Discovery' THEN 'stage1'
      WHEN stage_name = '2-Scoping' THEN 'stage2'
      WHEN stage_name = '3-Technical Evaluation' THEN 'stage3'
      WHEN stage_name = '4-Proposal' THEN 'stage4'
      WHEN stage_name = '5-Negotiating' THEN 'stage5'
      WHEN stage_name = '6-Awaiting Signature' THEN 'stage6'
      WHEN stage_name = '7-Closing' THEN 'stage7'
      WHEN stage_name = '8-Closed Lost' THEN 'closed_lost'
      WHEN stage_name = 'Closed Won' THEN 'closed_won'
    END                                                                                                          AS stage_name,
    created_date,
    stage_date
  FROM base
  WHERE stage_name IN ('0-Pending Acceptance', '1-Discovery', '2-Scoping', '3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing', '8-Closed Lost', 'Closed Won')
),

stage_pivot AS (
  SELECT *
  FROM (SELECT
    dim_crm_opportunity_id,
    created_date,
    stage_name,
    stage_date
  FROM stage_base)
  PIVOT (MAX(stage_date) FOR stage_name IN ('stage0', 'stage1', 'stage2', 'stage3', 'stage4', 'stage5', 'stage6', 'stage7', 'closed_lost', 'closed_won'))
),

stage_dates AS (
  SELECT
    dim_crm_opportunity_id,
    CASE WHEN "'closed_lost'" IS NOT NULL THEN 'Lost'
      WHEN "'closed_won'" IS NOT NULL THEN 'Won'
      ELSE 'Open'
    END                                       AS stage_category,
    created_date,
    "'stage0'"                                AS stage0_date,
    "'stage1'"                                AS stage1_date,
    "'stage2'"                                AS stage2_date,
    "'stage3'"                                AS stage3_date,
    "'stage4'"                                AS stage4_date,
    "'stage5'"                                AS stage5_date,
    "'stage6'"                                AS stage6_date,
    "'stage7'"                                AS stage7_date,
    COALESCE("'closed_lost'", "'closed_won'") AS close_date
  FROM stage_pivot
),

dates_adj AS (
  SELECT
    dim_crm_opportunity_id,
    stage_category,
    created_date,
    COALESCE(stage0_date, stage1_date, stage2_date, stage3_date, stage4_date, stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage0_date,
    COALESCE(stage1_date, stage2_date, stage3_date, stage4_date, stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage1_date,
    COALESCE(stage2_date, stage3_date, stage4_date, stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage2_date,
    COALESCE(stage3_date, stage4_date, stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage3_date,
    COALESCE(stage4_date, stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage4_date,
    COALESCE(stage5_date, stage6_date, stage7_date, close_date, created_date) AS stage5_date,
    COALESCE(stage6_date, stage7_date, close_date, created_date) AS stage6_date,
    COALESCE(stage7_date, close_date, created_date) AS stage7_date,
    COALESCE(close_date, created_date) AS close_date,
  FROM stage_dates
),

opp_snap AS (
  SELECT
    dim_crm_opportunity_id,
    stage_category,
    created_date,
    stage0_date - created_date AS create_days,
    stage0_date,
    CASE WHEN stage1_date - stage0_date < 0 THEN 0 ELSE stage1_date - stage0_date END AS stage0_days,
    stage1_date,
    CASE WHEN stage2_date - stage1_date < 0 THEN 0 ELSE stage2_date - stage1_date END AS stage1_days,
    stage2_date,
    CASE WHEN stage3_date - stage2_date < 0 THEN 0 ELSE stage3_date - stage2_date END AS stage2_days,
    stage3_date,
    CASE WHEN stage4_date - stage3_date < 0 THEN 0 ELSE stage4_date - stage3_date END AS stage3_days,
    stage4_date,
    CASE WHEN stage5_date - stage4_date < 0 THEN 0 ELSE stage5_date - stage4_date END AS stage4_days,
    stage5_date,
    CASE WHEN stage6_date - stage5_date < 0 THEN 0 ELSE stage6_date - stage5_date END AS stage5_days,
    stage6_date,
    CASE WHEN stage7_date - stage6_date < 0 THEN 0 ELSE stage7_date - stage6_date END AS stage6_days,
    stage7_date,
    CASE WHEN close_date - stage7_date < 0 THEN 0 ELSE close_date - stage7_date END AS stage7_days,
    close_date,
    CASE 
      WHEN stage_category = 'Open' 
      THEN CURRENT_DATE() - COALESCE(stage7_date, stage6_date, stage5_date, stage4_date, stage3_date, stage2_date, stage1_date, stage0_date, created_date) 
    END AS current_days
  FROM dates_adj
)

SELECT * FROM opp_snap
