WITH invoice AS (

    SELECT *
    FROM prep.zuora.zuora_invoice_source
    WHERE is_deleted = 'FALSE'

), opportunity_dimensions AS (

    SELECT *
    FROM prod.common_mapping.map_crm_opportunity

), quote AS (

    SELECT *
    FROM prep.sfdc.sfdc_zqu_quote_source
    WHERE is_deleted = 'FALSE'

), final_quotes AS (

    SELECT

      --ids
      quote.zqu_quote_id                  AS dim_quote_id,
      quote.zqu__account                  AS dim_crm_account_id,
      quote.zqu__zuora_account_id         AS dim_billing_account_id,

      --shared dimension keys
      quote.zqu__opportunity              AS dim_crm_opportunity_id,
      quote.zqu__zuora_subscription_id    AS dim_subscription_id,
      --dim_location_country_id,
      --dim_location_region_id,
      quote.owner_id                      AS dim_crm_sales_rep_id,
      dim_order_type_id,
      dim_opportunity_source_id,
      dim_purchase_channel_id,
      dim_sales_segment_id,
      dim_sales_territory_id,
      dim_industry_id,
      invoice.invoice_id                  AS dim_invoice_id,

      --dates
      quote.created_date,
      quote.quote_end_date,
      quote.zqu__valid_until              AS quote_valid_until,

      --additive fields
      quote.charge_summary_sub_total,
      quote.delta_arr,
      quote.opportunity_amount,
      quote.renewal_mrr,
      quote.professional_services_amount,
      quote.license_amount,
      quote.true_up_amount,
      quote.tcv_including_discount,
      quote.total_partner_discount,
      quote.quote_amendment_count,
      quote.zqu__delta_tcv                AS delta_tcv

    FROM quote
    LEFT JOIN opportunity_dimensions
      ON quote.zqu__opportunity = opportunity_dimensions.dim_crm_opportunity_id
    LEFT JOIN invoice
      ON quote.invoice_number = invoice.invoice_number

)

{{ dbt_audit(
cte_ref="final_quotes",
created_by="@mcooperDD",
updated_by="@mcooperDD",
created_date="2021-01-11",
updated_date="2021-01-11"
) }}
