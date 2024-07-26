{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('self_managed_instance_activations','customers_db_self_managed_instance_activations'),
    ('dim_subscription', 'dim_subscription')
]) }}

SELECT 
  id                                                                                                     AS dim_self_managed_instance_activation_id,
  {{ dbt_utils.generate_surrogate_key(['instance_activations.id']) }}                                    AS dim_self_managed_instance_activation_sk,
  instance_activations.cloud_activation_id                                                               AS dim_cloud_activation_id,
  instance_activations.self_managed_instance_id                                                          AS dim_self_managed_instance_id,
  instance_activations.subscription_id                                                                   AS dim_subscription_id,
  dim_subscription.dim_crm_account_id                                                                    AS dim_crm_account_id,
  dim_subscription.term_start_date                                                                       AS term_start_date,
  dim_subscription.term_start_month                                                                      AS term_start_month,
  dim_subscription.term_end_month                                                                        AS term_end_month,
  dim_subscription.subscription_version                                                                  AS subscription_version,
  dim_subscription.turn_on_cloud_licensing                                                               AS turn_on_cloud_licensing,
  dim_subscription.contract_auto_renewal                                                                 AS contract_auto_renewal,
  instance_activations.created_at                                                                        AS cloud_activation_created_at,
  instance_activations.activated_at                                                                      AS cloud_activation_activated_at,
  instance_activations.updated_at                                                                        AS cloud_activation_updated_at

FROM self_managed_instance_activations AS instance_activations
LEFT JOIN dim_subscription
  ON instance_activations.subscription_id = dim_subscription.dim_subscription_id
