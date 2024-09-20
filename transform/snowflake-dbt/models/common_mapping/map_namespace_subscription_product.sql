{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily')
]) }}

, prep_charge_mrr_daily_latest AS (

/*
To map the products (dim_product_detail_id) associated with the subscription, we need to look at the charges for the latest subscription version of the associated dim_subscription_id.
We have created a mapping table in prep_charge_mrr_daily at the daily grain which expands all of the charges for a subscription_name across the effective dates of the charges.

We want to limit this to the Active/Cancelled version of the subscription since this represents the latest valid version.

*/

  SELECT 
    prep_charge_mrr_daily.*
  FROM prep_charge_mrr_daily
  LEFT JOIN prep_subscription
    ON prep_charge_mrr_daily.dim_subscription_id = prep_subscription.dim_subscription_id
  WHERE prep_subscription.subscription_status IN ('Active', 'Cancelled')

), final AS (

/*

These charges contains a full history of the products associated with a subscription (dim_subscription_id_original/subscription_name) as well as the effective dates of the 
products as they were used by the customer. They are all associated with the most current dim_subscription_id in the subscription_name lineage.

We need to map these charges to the dim_subscription_id at the time the charges were effective otherwise the most recent version will be associated with dates before it was created, based on
how the charges track the history of the subscription. To map between the current dim_subscription_id and the one active at the time of the charges, we join to the subscription object 
between the subscription_created_datetime (adjusted for the first version of a subscription due to backdated effective dates in subscriptions) and the created date of the next subscription version.
*/

  SELECT DISTINCT
    prep_charge_mrr_daily_latest.date_actual,
    prep_subscription.dim_subscription_id,
    prep_subscription.dim_subscription_id_original,
    prep_subscription.namespace_id                  AS dim_namespace_id,
    prep_charge_mrr_daily_latest.dim_crm_account_id,
    prep_subscription.subscription_version,
    prep_charge_mrr_daily_latest.dim_product_detail_id,
    prep_charge_mrr_daily_latest.charge_type,
    {{ dbt_utils.generate_surrogate_key
      (
        [
          'prep_charge_mrr_daily_latest.date_actual',
          'prep_subscription.namespace_id',
          'prep_subscription.dim_subscription_id',
          'prep_charge_mrr_daily_latest.dim_product_detail_id'
        ]
      )
    }}                                        AS primary_key
  FROM prep_charge_mrr_daily_latest
  LEFT JOIN prep_subscription
    ON prep_charge_mrr_daily_latest.subscription_name = prep_subscription.subscription_name
      AND prep_charge_mrr_daily_latest.date_actual BETWEEN prep_subscription.subscription_created_datetime_adjusted AND prep_subscription.next_subscription_created_datetime

)

/*
Filter out any records where dim_namespace_id is missing
*/

SELECT *
FROM final
WHERE dim_namespace_id IS NOT NULL
