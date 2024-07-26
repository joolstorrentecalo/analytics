{{ config(
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('self_managed_instance_activations','prep_self_managed_instance_activations')
]) }}

SELECT
  -- Primary Key
  dim_self_managed_instance_activation_id,

  -- Surrogate Key
  dim_self_managed_instance_activation_sk,

  -- Foreign Keys
  dim_subscription_id,
  dim_self_managed_instance_id,
  dim_crm_account_id,

  -- Subscription Date Information
  term_start_date,
  term_start_month,
  term_end_month,

  -- Subscription Information
  subscription_version,
  turn_on_cloud_licensing,
  contract_auto_renewal,

  -- Other Attributes
  cloud_activation_created_at,
  cloud_activation_activated_at,
  cloud_activation_updated_at

FROM self_managed_instance_activations
