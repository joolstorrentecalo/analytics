{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('subscriptions', 'dim_subscription'),
    ('charges', 'prep_charge'),
    ('product_tier', 'dim_product_tier'),
    ('product_detail', 'dim_product_detail'),
    ('dim_date', 'dim_date')
]) }},

joined AS (

  SELECT
    dim_date.date_actual,
    subscriptions.dim_subscription_id,
    subscriptions.dim_subscription_id_original,
    subscriptions.namespace_id                     AS dim_namespace_id,
    subscriptions.dim_crm_account_id,
    subscriptions.subscription_version,
    subscriptions.subscription_created_date,
    product_detail.product_rate_plan_charge_name,
    product_detail.product_deployment_type,
    charges.charge_type,
    IFF(product_tier.product_ranking > 0, 1, 0)    AS is_paid_tier
  FROM subscriptions
  LEFT JOIN charges
    ON subscriptions.dim_subscription_id = charges.dim_subscription_id
  INNER JOIN dim_date
    ON charges.effective_start_date <= dim_date.date_actual
      AND (
        charges.effective_end_date > dim_date.date_actual
        OR charges.effective_end_date IS NULL
     )
  LEFT JOIN product_detail
    ON product_detail.dim_product_detail_id = charges.dim_product_detail_id
  LEFT JOIN product_tier
    ON product_detail.dim_product_tier_id = product_tier.dim_product_tier_id

)

SELECT
  date_actual,
  dim_subscription_id,
  dim_subscription_id_original,
  dim_namespace_id,
  dim_crm_account_id,
  subscription_version,
  product_rate_plan_charge_name,
  charge_type
FROM joined
WHERE product_deployment_type = 'GitLab.com'
  AND is_paid_tier = 1
  AND product_rate_plan_charge_name NOT IN (
    '1,000 CI Minutes',
    'Gitlab Storage 10GB - 1 Year',
    'Premium Support',
    '1,000 Compute Minutes'
  )
  AND charge_type != 'OneTime'
-- picking most recent subscription version
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY dim_namespace_id, date_actual
    ORDER BY subscription_created_date DESC, subscription_version DESC
  ) = 1
