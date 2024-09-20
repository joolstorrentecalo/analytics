{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license', 'prep_license'),
    ('prep_date', 'prep_date'),
    ('prep_usage_self_managed_seat_link', 'prep_usage_self_managed_seat_link'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily'),
    ('prep_product_detail', 'prep_product_detail')
])}}

, base AS (

/*
Do the following to begin mapping an installation to a subscription:

1. Map the ping to dim_subscription_id based on the license information sent with the record.
2. Find the next ping date for each Service Ping
*/

  SELECT
    prep_ping_instance.ping_created_at,
    prep_ping_instance.dim_installation_id,
    COALESCE(
      COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id), 
      prep_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT
      )                                                                                   AS dim_subscription_id,
    LEAD(ping_created_at) OVER (
      PARTITION BY dim_installation_id ORDER BY ping_created_at ASC
      )                                                                                   AS next_ping_date
  FROM prep_ping_instance
  LEFT JOIN prep_license AS md5
    ON prep_ping_instance.license_md5 = md5.license_md5
  LEFT JOIN prep_license AS sha256
    ON prep_ping_instance.license_sha256 = sha256.license_sha256
  WHERE COALESCE(
      COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id), 
      prep_ping_instance.raw_usage_data_payload:license_subscription_id::TEXT
      ) IS NOT NULL
  
), ping_daily_mapping AS (

/*
Expand the Sevice Ping records to the day grain.

Assumptions:
1. The dim_installation_id <> dim_subscription_id mapping is valid between the ping_created_at date and the next received ping date for that installation
2. If no other pings have been received for this installation, this mapping is valid until one week the last report created date since this data is received weekly.
*/

  SELECT 
    prep_date.date_actual,
    base.dim_installation_id,
    base.dim_subscription_id
  FROM base
  INNER JOIN prep_date
    ON base.ping_created_at <= prep_date.date_actual
      AND COALESCE(base.next_ping_date, DATEADD('day', 7, base.ping_created_at)) > prep_date.date_actual

), seat_link AS (

/*
Not all installations will send a Service Ping, so we will incorporate the Seat Link records
to have a more complete view of all potential dim_installation_id <> dim_subscription_id mappings.

This CTE finds the next_report_date for each dim_installation_id so we can expand the Seat Link data in a subsequent CTE.
*/

  SELECT 
    prep_usage_self_managed_seat_link.*,
    LEAD(report_date) OVER (PARTITION BY dim_installation_id ORDER BY report_date ASC)  AS next_report_date
  FROM prep_usage_self_managed_seat_link

), seat_link_records AS (

/*
Expand the Seat Link data to the daily grain for each installation - subscription.

Assumptions:
1. The dim_installation_id <> dim_subscription_id mapping is valid between the report_date and the next received report_date for that installation
2. If no other Seat Link records have been received for this installation, this mapping is valid until one day after the report date since this data is received daily.
*/

  SELECT
    prep_date.date_actual,
    seat_link.dim_installation_id,
    seat_link.dim_subscription_id
  FROM seat_link
  INNER JOIN prep_date
    ON seat_link.report_date <= prep_date.date_actual
    AND COALESCE(seat_link.next_report_date, DATEADD('day', 1, seat_link.report_date)) > prep_date.date_actual

), joined AS (

/*
Combine the Seat Link and Service Ping mappings for dim_installation_id <> dim_subscription_id. This will create a source of truth
for all possible mappings across both sources.

We perform a LEFT OUTER JOIN on the two datasets because the set of installations that send Service Ping records and the set of 
installations that send Seat Link data overlaps, but both may contain additional mappings.
*/

  SELECT 
    COALESCE(ping_daily_mapping.date_actual, seat_link_records.date_actual)                   AS date_actual,
    COALESCE(ping_daily_mapping.dim_installation_id, seat_link_records.dim_installation_id)   AS dim_installation_id,
    COALESCE(seat_link_records.dim_subscription_id, ping_daily_mapping.dim_subscription_id)   AS dim_subscription_id,
    prep_subscription.dim_subscription_id_original,
    prep_subscription.subscription_name
  FROM ping_daily_mapping
  LEFT OUTER JOIN seat_link_records
    ON ping_daily_mapping.dim_installation_id = seat_link_records.dim_installation_id
      AND ping_daily_mapping.date_actual = seat_link_records.date_actual
  LEFT JOIN prep_subscription
    ON COALESCE(seat_link_records.dim_subscription_id, ping_daily_mapping.dim_subscription_id) = prep_subscription.dim_subscription_id

), prep_charge_mrr_daily_latest AS (

/*
To map the products (dim_product_detail_id) associated with the subscription, we need to look at the charges for the latest subscription version of the associated dim_subscription_id.
We have created a mapping table in prep_charge_mrr_daily at the daily grain which expands all of the charges for a subscription_name across the effective dates of the charges.

We want to limit this to the Active/Cancelled version of the subscription since this represents the latest valid version.

*/

  SELECT prep_charge_mrr_daily.*
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
    COALESCE(subscriptions_ping.dim_subscription_id, subscriptions_charge.dim_subscription_id)                    AS dim_subscription_id,
    COALESCE(subscriptions_ping.dim_subscription_id_original, subscriptions_charge.dim_subscription_id_original)  AS dim_subscription_id_original,
    joined.dim_installation_id,
    subscriptions_ping.dim_crm_account_id,
    COALESCE(subscriptions_ping.subscription_version, subscriptions_charge.subscription_version)                  AS subscription_version,
    prep_charge_mrr_daily_latest.dim_product_detail_id,
    prep_charge_mrr_daily_latest.charge_type,
    {{ dbt_utils.generate_surrogate_key([
        'prep_charge_mrr_daily_latest.date_actual',
        'joined.dim_installation_id',
        'COALESCE(subscriptions_ping.dim_subscription_id, subscriptions_charge.dim_subscription_id)',
        'prep_charge_mrr_daily_latest.dim_product_detail_id'
      ]) 
    }}                                                                                                            AS primary_key
  FROM prep_charge_mrr_daily_latest
  LEFT JOIN joined
    ON prep_charge_mrr_daily_latest.dim_subscription_id_original = joined.dim_subscription_id_original
      AND prep_charge_mrr_daily_latest.date_actual = joined.date_actual
  LEFT JOIN prep_subscription AS subscriptions_ping
    ON joined.dim_subscription_id = subscriptions_ping.dim_subscription_id
  LEFT JOIN prep_subscription AS subscriptions_charge
    ON prep_charge_mrr_daily_latest.dim_subscription_id_original = subscriptions_charge.dim_subscription_id_original
      AND prep_charge_mrr_daily_latest.date_actual BETWEEN subscriptions_charge.subscription_created_datetime_adjusted AND subscriptions_charge.next_subscription_created_datetime
  LEFT JOIN prep_product_detail
    ON prep_charge_mrr_daily_latest.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE prep_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')

)

/*
Filter out any days in the mapping where dim_installation_id is unavailable.
*/

SELECT *
FROM final
WHERE dim_installation_id IS NOT NULL