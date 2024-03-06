{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH opportunity_data AS (

/* Table providing opportunity amounts */

SELECT
DATE_TRUNC('month', close_date) AS opportunity_close_month,
SUM(amount)                     AS booking_amount,
COUNT(amount)                   AS booking_count
FROM prod.restricted_safe_common.fct_crm_opportunity
WHERE is_closed_won = TRUE
GROUP BY opportunity_close_month

),

invoice_data AS (

/* Table providing invoice amounts */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date))           AS invoice_month,
SUM(fct_invoice.amount)                                       AS invoice_amount_with_tax,
SUM(fct_invoice.amount_without_tax)                           AS invoice_amount_without_tax,
SUM(fct_invoice.amount) - SUM(fct_invoice.amount_without_tax) AS invoice_tax_amount,
COUNT(fct_invoice.amount)                                     AS invoice_count
FROM prod.common.dim_invoice
JOIN prod.restricted_safe_common.fct_invoice ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id
WHERE dim_invoice.status = 'Posted'
GROUP BY invoice_month

),

payment_data AS (

/* Table providing payment amounts */

SELECT
DATE(DATE_TRUNC('month', payment_date)) AS payment_month,
SUM(payment_amount)                     AS payment_amount,
COUNT(payment_amount)                   AS payment_count
FROM prod.restricted_safe_workspace_finance.wk_finance_fct_payment
WHERE payment_status = 'Processed'
GROUP BY payment_month

),

final AS (

SELECT
opportunity_data.opportunity_close_month             AS year_month,
COALESCE(opportunity_data.booking_amount, 0)         AS booking_amount,
COALESCE(opportunity_data.booking_count, 0)          AS booking_count,
COALESCE(invoice_data.invoice_amount_with_tax, 0)    AS invoice_amount_with_tax,
COALESCE(invoice_data.invoice_amount_without_tax, 0) AS invoice_amount_without_tax,
COALESCE(invoice_data.invoice_tax_amount, 0)         AS invoice_tax_amount,
COALESCE(invoice_data.invoice_count, 0)              AS invoice_count,
COALESCE(payment_data.payment_amount, 0)             AS payment_amount,
COALESCE(payment_data.payment_count, 0)              AS payment_count
FROM opportunity_data 
LEFT JOIN invoice_data ON invoice_data.invoice_month  = opportunity_data.opportunity_close_month
LEFT JOIN payment_data ON payment_data.payment_month = opportunity_data.opportunity_close_month
WHERE year_month > '2017-06-30'
ORDER BY year_month

)

{{ dbt_audit(
cte_ref="final_cdot_invoices_manual_intervention_monthly",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-06",
updated_date="2024-03-06"
) }}
