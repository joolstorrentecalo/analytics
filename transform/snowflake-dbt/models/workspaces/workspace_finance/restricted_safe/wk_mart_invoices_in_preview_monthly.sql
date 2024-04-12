{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH final AS (

/* Determine the count and amount of invoices to be billed in the future aka invoices in preview or pending invoices */

SELECT 
DATE_TRUNC('month',(DATE(invoice_start_date))) AS billing_period,
SUM(invoice_amount) AS pending_invoice_amount,
COUNT(invoice_amount) AS pending_invoice_count
FROM {{ ref('driveload_pending_invoices_report_source') }} 
GROUP BY billing_period
ORDER BY billing_period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-26",
updated_date="2024-03-26"
) }}

