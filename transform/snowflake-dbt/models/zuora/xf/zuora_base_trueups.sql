WITH invoice_details AS (

  SELECT *
  FROM {{ ref('zuora_base_invoice_details') }}
  WHERE LOWER(charge_name) LIKE '%trueup%'

)

, final AS (
  
  SELECT
    country,
    account_number,
    subscription_name,
    subscription_name_slugify,
    oldest_subscription_in_cohort,
    lineage,
    cohort_month,
    cohort_quarter,
    service_month                 AS trueup_month,
    charge_name,
    service_start_date,
    charge_amount,
    charge_amount/12              AS mrr,
    unit_of_measure,
    unit_price

  FROM invoice_details

)

SELECT * 
FROM final
