{{ config(
   materialized="table",
   tags=["mnpi"]
) }}


WITH balance_per_ssp_channel AS (


/* Determine the total balances per SSP channel monthly */


SELECT
DATE(DATE_TRUNC('month',wk_finance_fct_invoice_aging_detail.accounting_period_end_date)) AS accounting_period_month,
dim_billing_account.ssp_channel,
SUM(wk_finance_fct_invoice_aging_detail.account_balance_impact) AS total_balance_per_channel,
COUNT(wk_finance_fct_invoice_aging_detail.account_balance_impact)AS invoice_count_per_channel,
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
LEFT JOIN {{ ref('fct_invoice') }} ON fct_invoice.dim_invoice_id = wk_finance_fct_invoice_aging_detail.invoice_id
LEFT JOIN {{ ref('dim_billing_account') }} ON dim_billing_account.dim_billing_account_id = fct_invoice.dim_billing_account_id
WHERE dim_billing_account.ssp_channel IS NOT NULL
GROUP BY wk_finance_fct_invoice_aging_detail.accounting_period_end_date, dim_billing_account.ssp_channel
ORDER BY wk_finance_fct_invoice_aging_detail.accounting_period_end_date, dim_billing_account.ssp_channel


),


total AS (


/* Determine the total balances for all open invoices monthly */


SELECT
DATE(DATE_TRUNC('month',wk_finance_fct_invoice_aging_detail.accounting_period_end_date)) AS accounting_period_month,
SUM(account_balance_impact) AS total_all_balance,
COUNT(account_balance_impact) AS count_all_open_invoices
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
GROUP BY accounting_period_end_date


),


Final AS (


/* Compare balances and count per SSP channel vs. the total balances for all open invoices monthly */


SELECT
balance_per_ssp_channel.accounting_period_month,
balance_per_ssp_channel.ssp_channel,
balance_per_ssp_channel.total_balance_per_channel,
ROUND((balance_per_ssp_channel.total_balance_per_channel / total.total_all_balance) * 100,2) AS percentage_of_open_balance_per_path,
total.total_all_balance,
balance_per_ssp_channel.invoice_count_per_channel,
ROUND((balance_per_ssp_channel.invoice_count_per_channel / total.count_all_open_invoices) * 100,2) AS percentage_of_open_invoices_count_per_path,
total.count_all_open_invoices
FROM balance_per_ssp_channel
LEFT JOIN total ON total.accounting_period_month = balance_per_ssp_channel.accounting_period_month
ORDER BY balance_per_ssp_channel.accounting_period_month, balance_per_ssp_channel.ssp_channel


)




{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-08",
updated_date="2024-04-08"
) }}


