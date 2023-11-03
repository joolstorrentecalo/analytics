{{ simple_cte([
 ('dim_date', 'dim_date'),
 ('dim_crm_account', 'dim_crm_account_daily_snapshot'),
 ('rpt_arr', 'rpt_arr_snapshot_combined_8th_calendar_day')
]) }},

finalized_arr_months AS (
  SELECT DISTINCT
    arr_month,
    is_arr_month_finalized
  FROM rpt_arr
),

child_account_arrs AS (
  SELECT
    dim_crm_account_id        AS child_account_id,
    dim_parent_crm_account_id AS parent_account_id,
    arr_month,
    SUM(ZEROIFNULL(mrr))      AS mrr,
    SUM(ZEROIFNULL(arr))      AS arr,
    SUM(ZEROIFNULL(quantity)) AS quantity
  FROM rpt_arr
  {{ dbt_utils.group_by(n=3) }}
),

py_arr_with_cy_parent AS (
  SELECT
    child_account_arrs.arr_month                     AS py_arr_month,
    DATEADD('year', 1, child_account_arrs.arr_month) AS retention_month,
    dim_crm_account.dim_parent_crm_account_id        AS parent_account_id_in_retention_month,
    DATEADD('year', 1, dim_date.snapshot_date_fpa)   AS retention_period_snapshot_date,
    SUM(ZEROIFNULL(child_account_arrs.mrr))          AS py_mrr,
    SUM(ZEROIFNULL(child_account_arrs.arr))          AS py_arr,
    SUM(ZEROIFNULL(child_account_arrs.quantity))     AS py_quantity
  FROM child_account_arrs
  LEFT JOIN dim_date
    ON child_account_arrs.arr_month::DATE = dim_date.date_day
  LEFT JOIN dim_crm_account
    ON child_account_arrs.child_account_id = dim_crm_account.dim_crm_account_id
      AND dim_crm_account.snapshot_date = DATEADD('year', 1, dim_date.snapshot_date_fpa)
  WHERE retention_month > '2020-03-01'
  -- this is when we started doing daily snapshots for dim_crm_account
  {{ dbt_utils.group_by(n=4) }}
),

cy_arr_with_cy_parent AS (
  SELECT
    parent_account_id,
    arr_month,
    SUM(ZEROIFNULL(mrr))      AS retained_mrr,
    SUM(ZEROIFNULL(arr))      AS retained_arr,
    SUM(ZEROIFNULL(quantity)) AS retained_quantity
  FROM child_account_arrs
  {{ dbt_utils.group_by(n=2) }}
),

final AS (
  SELECT
    COALESCE(cy_arr_with_cy_parent.parent_account_id, py_arr_with_cy_parent.parent_account_id_in_retention_month) AS dim_parent_crm_account_id,
    finalized_arr_months.is_arr_month_finalized                                                                   AS is_arr_month_finalized,
    DATEADD('year', 1, py_arr_with_cy_parent.py_arr_month)                                                        AS retention_month,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_quarter, dim_date.fiscal_quarter_name_fy, NULL)             AS retention_fiscal_quarter_name_fy,
    IFF(dim_date.is_first_day_of_last_month_of_fiscal_year, dim_date.fiscal_year, NULL)                           AS retention_fiscal_year,
    NULL                                                                                                          AS parent_crm_account_sales_segment,
    NULL                                                                                                          AS parent_crm_account_sales_segment_grouped,
    NULL                                                                                                          AS parent_crm_account_geo,
    CASE
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 100000 THEN '1. ARR > $100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) <= 100000 AND SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) > 5000 THEN '2. ARR $5K-100K'
      WHEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)) <= 5000 THEN '3. ARR <= $5K'
    END                                                                                                           AS retention_arr_band,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_mrr))                                                                 AS prior_year_mrr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_mrr))                                                           AS net_retention_mrr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr))                                                                 AS prior_year_arr,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr))                                                           AS net_retention_arr,
    CASE
      WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) = 0
        THEN SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr))
      ELSE 0
    END                                                                                                           AS churn_arr,
    CASE WHEN SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)) > 0
        THEN LEAST(SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_arr)), SUM(ZEROIFNULL(py_arr_with_cy_parent.py_arr)))
      ELSE 0 END                                                                                                  AS gross_retention_arr,
    SUM(ZEROIFNULL(py_arr_with_cy_parent.py_quantity))                                                            AS prior_year_quantity,
    SUM(ZEROIFNULL(cy_arr_with_cy_parent.retained_quantity))                                                      AS net_retention_quantity

  FROM py_arr_with_cy_parent
  LEFT JOIN cy_arr_with_cy_parent
    ON py_arr_with_cy_parent.parent_account_id_in_retention_month = cy_arr_with_cy_parent.parent_account_id
      AND py_arr_with_cy_parent.retention_month = cy_arr_with_cy_parent.arr_month
  LEFT JOIN finalized_arr_months
    ON finalized_arr_months.arr_month = py_arr_with_cy_parent.retention_month
  INNER JOIN dim_date
    ON dim_date.date_actual = py_arr_with_cy_parent.retention_month

  {{ dbt_utils.group_by(n=8) }}
)

{{ dbt_audit(
 cte_ref="final",
 created_by="@nmcavinue",
 updated_by="@chrissharp",
 created_date="2023-11-03",
 updated_date="2023-11-03"
) }}
