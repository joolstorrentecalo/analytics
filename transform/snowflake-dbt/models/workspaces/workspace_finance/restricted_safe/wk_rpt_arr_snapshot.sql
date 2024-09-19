{{ config(materialized='table') }}

{{ simple_cte([
 ('dim_date', 'dim_date'),
 ('mart_arr_snapshot_model', 'mart_arr_snapshot_model'),
 ('prep_crm_account_daily_snapshot', 'prep_crm_account_daily_snapshot')
]) }}

, snapshot_dates AS (
    --Use the 5th calendar day to snapshot ARR, Licensed Users, and Customer Count Metrics
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_fpa_fifth
    FROM dim_date
    ORDER BY 1 DESC

), prep_recurring_charge AS (
  SELECT
    *,
    CASE
      WHEN product_tier_name LIKE '%Ultimate%' THEN 3
      WHEN product_tier_name IN (
          'SaaS - Silver',
          'Self-Managed - Premium',
          'SaaS - Premium'
        ) THEN 2
      WHEN product_tier_name IN ('SaaS - Bronze', 'Self-Managed - Starter') THEN 1
      ELSE 0
    END AS product_ranking2
  FROM mart_arr_snapshot_model
  INNER JOIN snapshot_dates
    ON mart_arr_snapshot_model.arr_month = snapshot_dates.first_day_of_month
    AND mart_arr_snapshot_model.snapshot_date = snapshot_dates.snapshot_date_fpa_fifth
  WHERE
    mrr != 0

),

parent_account_arrs AS (
  SELECT
    COALESCE(dim_parent_crm_account_id, dim_crm_account_id) AS dim_parent_crm_account_id,
    dim_crm_account_id,
    arr_month,
    DATEADD('month', 3, arr_month)                     AS retention_month,
    MIN(is_arpu)                                       AS is_arpu,
    SUM(ZEROIFNULL(arr))                               AS arr_total,
    SUM(ZEROIFNULL(quantity))                          AS quantity_total,
    LISTAGG(DISTINCT product_tier_name, '; ')          AS product_category,
    MAX(product_ranking2)                              AS product_ranking
  FROM
    prep_recurring_charge
  GROUP BY
    1,
    2,
    3,
    4
),

retention_subs AS (
  SELECT
    current_arr.dim_parent_crm_account_id,
    current_arr.is_arpu                  AS current_arpu,
    future_arr.is_arpu                   AS future_arpu,
    future_arr.dim_parent_crm_account_id AS future_dim_parent_crm_account_id,
    current_arr.arr_month                AS current_arr_month,
    current_arr.retention_month,
    (
      COALESCE (current_arr.retention_month, future_arr.arr_month)
    )                                    AS future_arr_month,
    current_arr.arr_total                AS current_arr,
    future_arr.arr_total                 AS future_arr,
    current_arr.quantity_total           AS current_quantity,
    future_arr.quantity_total            AS future_quantity,
    current_arr.product_category         AS current_product_category,
    future_arr.product_category          AS future_product_category,
    current_arr.product_ranking          AS current_product_ranking,
    future_arr.product_ranking           AS future_product_ranking
  FROM
    parent_account_arrs AS current_arr
  FULL
  OUTER JOIN parent_account_arrs AS future_arr ON current_arr.dim_crm_account_id = future_arr.dim_crm_account_id
    AND current_arr.retention_month = future_arr.arr_month
),

final AS (
  SELECT
    retention_subs.dim_parent_crm_account_id,
    retention_subs.future_dim_parent_crm_account_id,
    (
      CASE
        WHEN current_arpu = 'TRUE'
          AND future_arpu = 'TRUE' THEN 'Y'
        ELSE 'N'
      END
    )                                 AS is_arpu_combined,
    parent_crm_account_sales_segment,
    prep_crm_account_daily_snapshot.crm_account_name AS parent_crm_account_name,
    retention_month,
    future_arr_month,
    current_arr                       AS prior_year_arr,
    COALESCE(future_arr, 0)           AS net_retention_arr,
    CASE
      WHEN net_retention_arr > 0 THEN LEAST(net_retention_arr, current_arr)
      ELSE 0
    END                               AS gross_retention_arr,
    current_quantity                  AS prior_year_quantity,
    COALESCE(future_quantity, 0)      AS net_retention_quantity,
    CASE
      WHEN prior_year_quantity != net_retention_quantity THEN net_retention_quantity - prior_year_quantity
      ELSE 0
    END                               AS seat_change_quantity,
    future_product_category           AS net_retention_product_category,
    current_product_category          AS prior_year_product_category,
    future_product_ranking            AS net_retention_product_ranking,
    current_product_ranking           AS prior_year_product_ranking,
    CASE
      WHEN retention_subs.dim_parent_crm_account_id IS NULL THEN 'New'
      WHEN net_retention_arr = 0
        AND prior_year_arr > 0 THEN 'Churn'
      WHEN net_retention_arr < prior_year_arr
        AND net_retention_arr > 0 THEN 'Contraction'
      WHEN net_retention_arr > prior_year_arr
        THEN 'Expansion'
      WHEN net_retention_arr = prior_year_arr THEN 'No Impact'
    END                               AS type_of_arr_change
  FROM
    retention_subs
  LEFT JOIN prep_crm_account_daily_snapshot ON prep_crm_account_daily_snapshot.dim_crm_account_id = COALESCE(
      retention_subs.dim_parent_crm_account_id,
      retention_subs.future_dim_parent_crm_account_id
    )
      AND retention_subs.retention_month = prep_crm_account_daily_snapshot.snapshot_date
),

final_1 AS (
  SELECT
    dim_parent_crm_account_id,
    future_dim_parent_crm_account_id,
    is_arpu_combined,
    parent_crm_account_sales_segment,
    parent_crm_account_name,
    retention_month,
    future_arr_month,
    prior_year_arr,
    net_retention_arr,
    gross_retention_arr,
    prior_year_quantity,
    net_retention_quantity,
    seat_change_quantity,
    net_retention_product_category,
    prior_year_product_category,
    net_retention_product_ranking,
    prior_year_product_ranking,
    type_of_arr_change,
    CASE
      WHEN is_arpu_combined = 'Y'
        AND type_of_arr_change IN ('Expansion', 'Contraction')
        AND prior_year_quantity != net_retention_quantity
        AND prior_year_quantity > 0
        THEN ZEROIFNULL(
            prior_year_arr / NULLIF(prior_year_quantity, 0) * (net_retention_quantity - prior_year_quantity)
          )
      WHEN prior_year_quantity != net_retention_quantity
        AND prior_year_quantity = 0 THEN net_retention_arr
      ELSE 0
    END AS seat_change_arr,
    CASE
      WHEN is_arpu_combined = 'N'
        AND type_of_arr_change IN ('Expansion', 'Contraction') THEN COALESCE(net_retention_arr, 0) - COALESCE(prior_year_arr, 0)
      WHEN is_arpu_combined = 'Y'
        AND prior_year_product_category = net_retention_product_category
        THEN net_retention_quantity * (
            net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      WHEN is_arpu_combined = 'Y'
        AND prior_year_product_category != net_retention_product_category
        AND prior_year_product_ranking = net_retention_product_ranking
        THEN net_retention_quantity * (
            net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
          )
      ELSE 0
    END AS price_change_arr,
    CASE
      WHEN is_arpu_combined = 'Y'
        AND prior_year_product_ranking != net_retention_product_ranking
        THEN ZEROIFNULL(
            net_retention_quantity * (
              net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
            )
          )
      ELSE 0
    END AS tier_change_arr,
    (
      CASE
        WHEN is_arpu_combined = 'Y'
          AND prior_year_product_ranking < net_retention_product_ranking
          THEN ZEROIFNULL(
              net_retention_quantity * (
                net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_arr / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS uptier_change_arr,
    (
      CASE
        WHEN is_arpu_combined = 'Y'
          AND prior_year_product_ranking > net_retention_product_ranking
          THEN ZEROIFNULL(
              net_retention_quantity * (
                net_retention_arr / NULLIF(net_retention_quantity, 0) - prior_year_quantity / NULLIF(prior_year_quantity, 0)
              )
            )
        ELSE 0
      END
    )   AS downtier_change_arr,
    (
      CASE
        WHEN type_of_arr_change = 'New' THEN net_retention_arr
      END
    )   AS new_arr,
    (
      CASE
        WHEN type_of_arr_change = 'Churn' THEN prior_year_arr
      END
    )   AS churn_arr
  FROM
    final
),

final_test AS (
  SELECT
    dim_parent_crm_account_id,
    parent_crm_account_name,
    (
      COALESCE (future_arr_month, retention_month)
    )                      AS analysis_period,
    b.fiscal_quarter_name_fy,
    type_of_arr_change,
    (
      COALESCE (parent_crm_account_sales_segment, 'SMB')
    )                      AS segment,
    SUM(net_retention_arr) AS current_arr,
    SUM(prior_year_arr)    AS prior_quarter_arr,
    SUM(
      CASE
        WHEN type_of_arr_change IN ('Expansion', 'Contraction') THEN seat_change_arr
      END
    )                      AS seat_change_arr,
    SUM(
      CASE
        WHEN type_of_arr_change IN ('Expansion', 'Contraction') THEN price_change_arr
      END
    )                      AS price_change_arr,
    SUM(
      CASE
        WHEN type_of_arr_change IN ('Expansion', 'Contraction') THEN tier_change_arr
      END
    )                      AS tier_change_arr,
    SUM(
      CASE
        WHEN type_of_arr_change IN ('Expansion', 'Contraction') THEN uptier_change_arr
      END
    )                      AS uptier_change_arr,
    SUM(
      CASE
        WHEN type_of_arr_change IN ('Expansion', 'Contraction') THEN downtier_change_arr
      END
    )                      AS downtier_change_arr,
    SUM(new_arr)           AS new_arr,
    SUM(churn_arr) * (-1)  AS churn_arr
  FROM
    final_1
  LEFT JOIN dim_date AS b ON analysis_period = b.date_actual
  WHERE
    b.month_of_fiscal_year IN (3, 6, 9, 12)
    AND b.fiscal_quarter_name_fy >= 'FY21-Q1'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
)

SELECT *
FROM parent_account_arrs
