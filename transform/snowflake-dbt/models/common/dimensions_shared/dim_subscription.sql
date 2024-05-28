{{ config({
    "tags": ["mnpi_exception"],
    "alias": "dim_subscription"
}) }}

WITH prep_amendment AS (

  SELECT *
  FROM {{ ref('prep_amendment') }}

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), subscription_opportunity_mapping AS (

    SELECT *
    FROM {{ ref('map_subscription_opportunity') }}

), subscription_lineage AS (

    SELECT DISTINCT
      subscription_name_slugify,
      subscription_lineage,
      oldest_subscription_in_cohort,
      subscription_cohort_month,
      subscription_cohort_quarter,
      subscription_cohort_year
    FROM {{ ref('map_subscription_lineage') }}

), data_quality_filter_subscription_slugify AS (
    
    /*
    There was a data quality issue where a subscription_name_slugify can be mapped to more than one subscription_name. 
    There are 5 subscription_name_slugifys and 10 subscription_names that this impacts as of 2023-02-20. This CTE is 
    used to filter out these subscriptions from the model. The data quality issue causes a fanout with the subscription 
    lineages that are used to group on in the data model.
    This DQ issue has been fixed and the way subscriptions are named now does not have this problem.
    So this CTE is for cleaning up historical data and future proofing the model in case this DQ issue come again.
    */

    SELECT 
      subscription_name_slugify,
      COUNT(subscription_name) AS nbr_records
    FROM PROD.COMMON.DIM_SUBSCRIPTION
    WHERE subscription_status IN ('Active', 'Cancelled')
    GROUP BY 1
    HAVING nbr_records > 1

), oldest_subscription_in_cohort AS (
  -- oldest subs is being filltered with the join condition
  SELECT 

    -- Oldest subcription cohort keys
    subscription.dim_subscription_id AS dim_oldest_subscription_in_cohort_id,
    subscription.dim_crm_account_id AS dim_oldest_crm_account_in_cohort_id,

    -- Oldest subcription cohort information
    subscription_lineage.subscription_cohort_month AS oldest_subscription_cohort_month
  
  FROM subscription
  LEFT JOIN subscription_lineage
    ON subscription_lineage.oldest_subscription_in_cohort = subscription.subscription_name_slugify
  WHERE subscription.subscription_status IN ('Active', 'Cancelled') -- Is this required?
    AND subscription.subscription_name_slugify NOT IN (SELECT subscription_name_slugify FROM data_quality_filter_subscription_slugify)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_oldest_subscription_in_cohort_id ORDER BY oldest_subscription_cohort_month DESC) = 1 -- Remove duplicates


), final AS (

  SELECT
    --Surrogate Key
    subscription.dim_subscription_id,

    --Natural Key
    subscription.subscription_name,
    subscription.subscription_version,

    --Common Dimension Keys
    subscription.dim_crm_account_id,
    subscription.dim_billing_account_id,
    subscription.dim_billing_account_id_invoice_owner_account,
    subscription.dim_billing_account_id_creator_account,
    CASE
       WHEN subscription.subscription_created_date < '2019-02-01'
         THEN NULL
       ELSE subscription_opportunity_mapping.dim_crm_opportunity_id
    END                                                                             AS dim_crm_opportunity_id,
    subscription.dim_crm_opportunity_id_current_open_renewal,
    subscription.dim_crm_opportunity_id_closed_lost_renewal,
    {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}                        AS dim_amendment_id_subscription,

    -- Oldest subcription cohort keys
    oldest_subscription.dim_oldest_subscription_in_cohort_id,
    oldest_subscription.dim_oldest_crm_account_in_cohort_id,

    --Subscription Information
    subscription.created_by_id,
    subscription.updated_by_id,
    subscription.dim_subscription_id_original,
    subscription.dim_subscription_id_previous,
    subscription.subscription_name_slugify,
    subscription.subscription_status,
    subscription.namespace_id,
    subscription.namespace_name,
    subscription.zuora_renewal_subscription_name,
    subscription.zuora_renewal_subscription_name_slugify,
    subscription.current_term,
    subscription.renewal_term,
    subscription.renewal_term_period_type,
    subscription.eoa_starter_bronze_offer_accepted,
    subscription.subscription_sales_type,
    subscription.auto_renew_native_hist,
    subscription.auto_renew_customerdot_hist,
    subscription.turn_on_cloud_licensing,
    subscription.turn_on_operational_metrics,
    subscription.contract_operational_metrics,
    subscription.contract_auto_renewal,
    subscription.turn_on_auto_renewal,
    subscription.contract_seat_reconciliation,
    subscription.turn_on_seat_reconciliation,
    subscription_opportunity_mapping.is_questionable_opportunity_mapping,
    subscription.invoice_owner_account,
    subscription.creator_account,
    subscription.was_purchased_through_reseller,
    subscription.multi_year_deal_subscription_linkage,

    -- Oldest subcription cohort information
    oldest_subscription.oldest_subscription_cohort_month,

    --Date Information
    subscription.subscription_start_date,
    subscription.subscription_end_date,
    subscription.subscription_start_month,
    subscription.subscription_end_month,
    subscription.subscription_end_fiscal_year,
    subscription.subscription_created_date,
    subscription.subscription_updated_date,
    subscription.term_start_date,
    subscription.term_end_date,
    subscription.term_start_month,
    subscription.term_end_month,
    subscription.term_start_fiscal_year,
    subscription.term_end_fiscal_year,
    subscription.is_single_fiscal_year_term_subscription,
    subscription.second_active_renewal_month,
    subscription.cancelled_date,

    --Lineage and Cohort Information
    subscription_lineage.subscription_lineage,
    subscription_lineage.oldest_subscription_in_cohort,
    subscription_lineage.subscription_cohort_month,
    subscription_lineage.subscription_cohort_quarter,
    subscription_lineage.subscription_cohort_year

  FROM subscription
  LEFT JOIN subscription_lineage
    ON subscription_lineage.subscription_name_slugify = subscription.subscription_name_slugify
  LEFT JOIN oldest_subscription_in_cohort AS oldest_subscription
    ON oldest_subscription.dim_oldest_subscription_in_cohort_id = subscription.dim_subscription_id
  LEFT JOIN prep_amendment
    ON subscription.dim_amendment_id_subscription = prep_amendment.dim_amendment_id
  LEFT JOIN subscription_opportunity_mapping
    ON subscription.dim_subscription_id = subscription_opportunity_mapping.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@utkarsh060",
    created_date="2020-12-16",
    updated_date="2024-05-27"
) }}
