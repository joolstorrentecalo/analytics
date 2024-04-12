WITH credits AS ( --excluding migration credit between march 7th and March 27th

  SELECT
    cr.source_primary_key,
    SUM(cr.credit_amount) AS total_credit
  FROM {{ ref('gcp_billing_export_credits') }} AS cr
  WHERE LOWER(cr.credit_description) NOT LIKE '%1709765302259%'
  GROUP BY cr.source_primary_key

),

daily_costs AS (

  SELECT
    DATE(usage_start_time)                          AS date_day,
    SUM(cost_before_credits) + SUM(cr.total_credit) AS net_cost
  FROM {{ ref('gcp_billing_export_xf') }} AS xf
  LEFT JOIN credits AS cr ON xf.source_primary_key = cr.source_primary_key
  WHERE DATE_TRUNC('month', DATE(usage_start_time)) >= '2023-11-01'
    AND xf.project_id IN ('unreview-poc-390200e5')
  GROUP BY 1
  ORDER BY 1 DESC
)

SELECT
  d.date_day,
  d.net_cost                                                                           AS daily_net_cost,
  SUM(d.net_cost) OVER (ORDER BY d.date_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_net_cost,
  SUM(d.net_cost) OVER (ORDER BY d.date_day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_net_cost
FROM daily_costs d
ORDER BY d.date_day DESC
