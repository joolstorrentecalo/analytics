SELECT
  date_day,
  CASE WHEN gcp_sku_description LIKE 'Codey for Code Completion%' THEN 'Codey for Code Completion'
    WHEN gcp_sku_description LIKE 'Codey for Code Chat%' THEN 'Codey for Code Chat'
    WHEN gcp_sku_description LIKE 'Codey for Code Generation%' THEN 'Codey for Code Generation'
    WHEN gcp_sku_description LIKE 'PaLM Text Bison%' THEN 'PaLM Text Bison'
  END                   AS model,
  CASE WHEN gcp_sku_description like '%Input%' THEN 'tokens_per_user_request_prompt'
  WHEN gcp_sku_description like '%Output%' THEN 'tokens_per_user_request_response'
  END AS prompt_response,
  gcp_sku_description,
  SUM(usage_amount)     AS characters_billed,
  SUM(usage_amount) / 4 AS tokens_billed,
  SUM(net_cost)         AS net_cost
FROM {{ ref ('rpt_gcp_billing_pl_day_ext') }}
WHERE
  gcp_project_id IN ('unreview-poc-390200e5')
  AND gcp_sku_description IN (
    'PaLM Text Bison Input - Predictions',
    'PaLM Text Bison Output - Predictions',
    'Codey for Code Generation Input - Predictions',
    'Codey for Code Generation Output - Predictions',
    'Codey for Code Chat Input - Predictions',
    'Codey for Code Chat Output - Predictions',
    'Codey for Code Completion Output - Predictions',
    'Codey for Code Completion Input - Predictions'
  )
  AND date_day > '2024-01-01'
GROUP BY 1, 2, 3, 4
ORDER BY date_day DESC
