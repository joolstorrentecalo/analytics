{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('mart_crm_opportunity_daily_snapshot', 'mart_crm_opportunity_daily_snapshot'),
]) }},

-- This will be a list of only the opps that are eligible open pipeline and closing in the quarter, per quarter.
fourth_day_flag AS (
  SELECT DISTINCT
    dim_crm_opportunity_id       AS starting_opp_id,
    snapshot_fiscal_quarter_date AS starting_snapshot_fiscal_quarter_date
  FROM mart_crm_opportunity_daily_snapshot
  LEFT JOIN dim_date
    ON mart_crm_opportunity_daily_snapshot.snapshot_date = dim_date.date_actual
  WHERE fiscal_year = '2025'  -- just fy25
    AND stage_name NOT IN ('0-Pending Acceptance', '00-Pre Opportunity', '9-Unqualified', '10-Duplicate')  -- filters out unqualified open opps
    AND (is_open = TRUE OR day_of_fiscal_quarter_normalised = 4)  -- is open, or closed day 4
    AND is_eligible_open_pipeline = TRUE  -- is eligible open pipe
    AND snapshot_fiscal_quarter_date = close_fiscal_quarter_date  -- is closing this snapshot quarter on day 4
    AND close_fiscal_quarter_date IS NOT NULL  -- has a close date
    AND snapshot_date = DATEADD('day', 4, snapshot_fiscal_quarter_date)  -- is four days after first day of quarter
),

-- this just filters down mart_crm_opportunity
base AS (
  SELECT
    dim_crm_opportunity_id,
    opportunity_name,
    snapshot_date,
    snapshot_fiscal_quarter_date,
    close_date,
    close_fiscal_quarter_date,
    fiscal_quarter_name_fy                              AS snapshot_fiscal_quarter_name_fy,
    -- flags
    is_open,
    is_closed,
    is_eligible_open_pipeline,
    fpa_master_bookings_flag,
    is_won,
    is_lost,
    reason_for_loss,
    -- measure
    net_arr,
    -- dimensions
    report_area,
    report_geo,
    report_region,
    report_area                                         AS opportunity_owner_user_area,
    report_geo                                          AS opportunity_owner_user_geo,
    report_region                                       AS opportunity_owner_user_region,
    sales_qualified_source_name,
    order_type_grouped,
    stage_name,
    deal_path_name,
    product_category,
    last_day_of_fiscal_quarter,
    report_role_level_1,
    report_role_level_2,
    COALESCE (report_role_level_2, report_role_level_1) AS role_level_2,
    CASE
      WHEN report_role_level_3 IS NULL AND report_role_level_2 IS NULL THEN report_role_level_1
      WHEN report_role_level_3 IS NULL AND report_role_level_2 IS NOT NULL THEN report_role_level_2
      WHEN report_role_level_3 IS NOT NULL AND report_role_level_3 LIKE ('EMEA_COMM_NEUR%') THEN report_role_level_4
      WHEN report_role_level_3 LIKE ('EMEA_META%') THEN 'EMEA_META'
      WHEN report_role_level_3 LIKE ('EMEA_TELCO%') THEN 'EMEA_TELCO'
      WHEN report_role_level_3 LIKE ('APJ_JAPAN%') THEN 'APJ_JAPAN'
      ELSE report_role_level_3
    END                                                 AS role_level_3,
    -- flags the opps that shouldnt be considered pipe opps
    CASE
      WHEN stage_name NOT IN ('0-Pending Acceptance', '00-Pre Opportunity', '9-Unqualified', '10-Duplicate') THEN TRUE
    END                                                 AS in_pipe,
    day_of_fiscal_quarter                               AS snapshot_day_of_fiscal_quarter
  FROM mart_crm_opportunity_daily_snapshot
  LEFT JOIN dim_date
    ON mart_crm_opportunity_daily_snapshot.snapshot_date = dim_date.date_actual
  WHERE fiscal_year = '2025'  -- FISCAL_QUARTER_NAME_FY = 'FY25-Q1'
    AND (
      snapshot_date = last_day_of_fiscal_quarter  -- last day of quarter
      OR day_of_fiscal_quarter_normalised = 4  -- fourth day of quarter
      OR (snapshot_day_of_fiscal_quarter % 5 = 0)
    )  -- 5th day
)

SELECT
  *,
  -- movement categories - since we are only looking at opps that were open and predicted to close this quarter, we can use the predicted close quarter on future days to categorize
  CASE
    WHEN close_fiscal_quarter_date > snapshot_fiscal_quarter_date AND is_open THEN 'push'
    WHEN close_fiscal_quarter_date > snapshot_fiscal_quarter_date AND is_won = TRUE THEN 'push'
    WHEN close_fiscal_quarter_date > snapshot_fiscal_quarter_date AND stage_name = '8-Closed Lost' THEN 'push'
    WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date AND is_open THEN 'open'
    WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date AND is_won = TRUE THEN 'closed won'
    WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date AND is_open = FALSE AND is_won != TRUE THEN 'closed lost'
    ELSE 'other'
  END AS movement_categories
FROM fourth_day_flag
LEFT JOIN base
  ON fourth_day_flag.starting_opp_id = base.dim_crm_opportunity_id
    AND fourth_day_flag.starting_snapshot_fiscal_quarter_date = base.snapshot_fiscal_quarter_date
