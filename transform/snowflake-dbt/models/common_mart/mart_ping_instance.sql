{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table",
    unique_key = "ping_instance_id"
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_product_tier', 'dim_product_tier'),
    ('dim_date', 'dim_date'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_accounts', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('fct_charge', 'fct_charge'),
    ('dim_license', 'dim_license'),
    ('dim_location', 'dim_location_country'),
    ('fct_ping_instance', 'fct_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

, dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    WHERE (subscription_name_slugify <> zuora_renewal_subscription_name_slugify[0]::TEXT
      OR zuora_renewal_subscription_name_slugify IS NULL)
      AND subscription_status NOT IN ('Draft', 'Expired')

), fct_ping_instance_metric AS  (

    SELECT *
    FROM fct_ping_instance
    {% if is_incremental() %}
                WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
    {% endif %}

), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), license_subscriptions AS (

    SELECT DISTINCT
      dim_date.first_day_of_month                                                 AS reporting_month,
      dim_license.dim_license_id                                                  AS license_id,
      dim_license.license_sha256                                                  AS license_sha256,
      dim_license.license_md5                                                     AS license_md5,
      dim_license.company                                                         AS license_company_name,
      subscription_source.subscription_name_slugify                               AS original_subscription_name_slugify,
      dim_subscription.dim_subscription_id                                        AS latest_subscription_id,
      dim_subscription.subscription_start_date                                    AS subscription_start_date,
      dim_subscription.subscription_end_date                                      AS subscription_end_date,
      dim_subscription.subscription_start_month                                   AS subscription_start_month,
      dim_subscription.subscription_end_month                                     AS subscription_end_month,
      dim_billing_account.dim_billing_account_id                                  AS dim_billing_account_id,
      dim_crm_accounts.crm_account_name                                           AS crm_account_name,
      dim_crm_accounts.dim_parent_crm_account_id                                  AS dim_parent_crm_account_id,
      dim_crm_accounts.parent_crm_account_name                                    AS parent_crm_account_name,
      dim_crm_accounts.parent_crm_account_billing_country                         AS parent_crm_account_billing_country,
      dim_crm_accounts.parent_crm_account_sales_segment                           AS parent_crm_account_sales_segment,
      dim_crm_accounts.parent_crm_account_industry                                AS parent_crm_account_industry,
      dim_crm_accounts.parent_crm_account_owner_team                              AS parent_crm_account_owner_team,
      dim_crm_accounts.parent_crm_account_sales_territory                         AS parent_crm_account_sales_territory,
      dim_crm_accounts.technical_account_manager                                  AS technical_account_manager,
      IFF(MAX(fct_charge.mrr) > 0, TRUE, FALSE)                                   AS is_paid_subscription,
      MAX(IFF(dim_product_detail.product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE)) 
                                                                                  AS is_program_subscription,
      ARRAY_AGG(DISTINCT dim_product_detail.product_tier_name)
        WITHIN GROUP (ORDER BY dim_product_detail.product_tier_name ASC)          AS product_category_array,
      ARRAY_AGG(DISTINCT dim_product_detail.product_rate_plan_name)
        WITHIN GROUP (ORDER BY dim_product_detail.product_rate_plan_name ASC)     AS product_rate_plan_name_array,
      SUM(fct_charge.quantity)                                                    AS quantity,
      SUM(fct_charge.mrr * 12)                                                    AS arr
    FROM dim_license
    INNER JOIN subscription_source
      ON dim_license.dim_subscription_id = subscription_source.subscription_id
    LEFT JOIN dim_subscription
      ON subscription_source.subscription_name_slugify = dim_subscription.subscription_name_slugify
    LEFT JOIN subscription_source AS all_subscriptions
      ON subscription_source.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    INNER JOIN fct_charge
      ON all_subscriptions.subscription_id = fct_charge.dim_subscription_id
        AND fct_charge.charge_type = 'Recurring'
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_charge.dim_product_detail_id
      AND dim_product_detail.product_delivery_type = 'Self-Managed'
      AND dim_product_detail.product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_accounts
      ON dim_billing_account.dim_crm_account_id = dim_crm_accounts.dim_crm_account_id
    INNER JOIN dim_date
      ON fct_charge.effective_start_month <= dim_date.date_day AND fct_charge.effective_end_month > dim_date.date_day
    {{ dbt_utils.group_by(n=21)}}


  ), joined AS (

      SELECT
        fct_ping_instance_metric.ping_instance_id                                                                                       AS ping_instance_id,
        fct_ping_instance_metric.dim_ping_date_id                                                                                       AS dim_ping_date_id,
        fct_ping_instance_metric.dim_license_id                                                                                         AS dim_license_id,
        fct_ping_instance_metric.dim_installation_id                                                                                    AS dim_installation_id,
        fct_ping_instance_metric.dim_ping_instance_id                                                                                   AS dim_ping_instance_id,
        dim_ping_instance.license_sha256                                                                                                AS license_sha256,
        dim_ping_instance.license_md5                                                                                                   AS license_md5,
        dim_ping_instance.is_trial                                                                                                      AS is_trial,
        fct_ping_instance_metric.umau_value                                                                                             AS umau_value,
        COALESCE(sha256.license_id, md5.license_id)                                                                                     AS license_id,
        COALESCE(sha256.license_company_name, md5.license_company_name)                                                                 AS license_company_name,
        COALESCE(sha256.latest_subscription_id, md5.latest_subscription_id)                                                             AS latest_subscription_id,
        COALESCE(sha256.original_subscription_name_slugify, md5.original_subscription_name_slugify)                                     AS original_subscription_name_slugify,
        COALESCE(sha256.product_category_array, md5.product_category_array)                                                             AS product_category_array,
        COALESCE(sha256.product_rate_plan_name_array, md5.product_rate_plan_name_array)                                                 AS product_rate_plan_name_array,
        COALESCE(sha256.subscription_start_month, md5.subscription_start_month)                                                         AS subscription_start_month,
        COALESCE(sha256.subscription_end_month, md5.subscription_end_month)                                                             AS subscription_end_month,
        COALESCE(sha256.dim_billing_account_id, md5.dim_billing_account_id)                                                             AS dim_billing_account_id,
        COALESCE(sha256.crm_account_name, md5.crm_account_name)                                                                         AS crm_account_name,
        COALESCE(sha256.dim_parent_crm_account_id, md5.dim_parent_crm_account_id)                                                       AS dim_parent_crm_account_id,
        COALESCE(sha256.parent_crm_account_name, md5.parent_crm_account_name)                                                           AS parent_crm_account_name,
        COALESCE(sha256.parent_crm_account_billing_country, md5.parent_crm_account_billing_country)                                     AS parent_crm_account_billing_country,
        COALESCE(sha256.parent_crm_account_sales_segment, md5.parent_crm_account_sales_segment)                                         AS parent_crm_account_sales_segment,
        COALESCE(sha256.parent_crm_account_industry, md5.parent_crm_account_industry)                                                   AS parent_crm_account_industry,
        COALESCE(sha256.parent_crm_account_owner_team, md5.parent_crm_account_owner_team)                                               AS parent_crm_account_owner_team,
        COALESCE(sha256.parent_crm_account_sales_territory, md5.parent_crm_account_sales_territory)                                     AS parent_crm_account_sales_territory,
        COALESCE(sha256.technical_account_manager, md5.technical_account_manager)                                                       AS technical_account_manager,
        COALESCE(sha256.is_paid_subscription, md5.is_paid_subscription, FALSE)                                                          AS is_paid_subscription,
        COALESCE(sha256.is_program_subscription, md5.is_program_subscription, FALSE)                                                    AS is_program_subscription,
        dim_ping_instance.ping_delivery_type                                                                                            AS ping_delivery_type,
        dim_ping_instance.ping_edition                                                                                                  AS ping_edition,
        dim_ping_instance.product_tier                                                                                                  AS ping_product_tier,
        dim_ping_instance.ping_edition || ' - ' || dim_ping_instance.product_tier                                                       AS ping_edition_product_tier,
        dim_ping_instance.major_version                                                                                                 AS major_version,
        dim_ping_instance.minor_version                                                                                                 AS minor_version,
        dim_ping_instance.major_minor_version                                                                                           AS major_minor_version,
        dim_ping_instance.major_minor_version_id                                                                                        AS major_minor_version_id,
        dim_ping_instance.version_is_prerelease                                                                                         AS version_is_prerelease,
        dim_ping_instance.is_internal                                                                                                   AS is_internal,
        dim_ping_instance.is_staging                                                                                                    AS is_staging,
        dim_ping_instance.instance_user_count                                                                                           AS instance_user_count,
        dim_ping_instance.ping_created_at                                                                                               AS ping_created_at,
        dim_date.first_day_of_month                                                                                                     AS ping_created_date_month,
        fct_ping_instance_metric.dim_host_id                                                                                            AS dim_host_id,
        fct_ping_instance_metric.dim_instance_id                                                                                        AS dim_instance_id,
        dim_ping_instance.host_name                                                                                                     AS host_name,
        dim_ping_instance.is_last_ping_of_month                                                                                         AS is_last_ping_of_month,
        fct_ping_instance_metric.dim_location_country_id                                                                                AS dim_location_country_id,
        dim_location.country_name                                                                                                       AS country_name,
        dim_location.iso_2_country_code                                                                                                 AS iso_2_country_code,
        dim_ping_instance.collected_data_categories                                                                                     AS collected_data_categories
      FROM fct_ping_instance_metric
      INNER JOIN dim_date
        ON fct_ping_instance_metric.dim_ping_date_id = dim_date.date_id
      LEFT JOIN dim_ping_instance
        ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
      LEFT JOIN license_subscriptions md5
        ON dim_ping_instance.license_md5 = md5.license_md5
        AND dim_date.first_day_of_month = md5.reporting_month
      LEFT JOIN license_subscriptions sha256
        ON dim_ping_instance.license_sha256 = sha256.license_sha256
        AND dim_date.first_day_of_month = sha256.reporting_month
      LEFT JOIN dim_location
        ON fct_ping_instance_metric.dim_location_country_id = dim_location.dim_location_country_id
      WHERE dim_ping_instance.ping_delivery_type = 'Self-Managed'
        OR (dim_ping_instance.ping_delivery_type = 'SaaS' AND fct_ping_instance_metric.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7')

), sorted AS (

    SELECT

      -- Primary Key
      ping_instance_id,
      dim_ping_date_id,
      dim_ping_instance_id,

      --Foreign Key
      dim_instance_id,
      dim_license_id,
      dim_installation_id,
      latest_subscription_id,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      major_minor_version_id,
      dim_host_id,
      host_name,
      dim_location_country_id,

      --Service Ping metadata
      ping_delivery_type,
      ping_edition,
      ping_product_tier,
      ping_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version_is_prerelease,
      is_internal,
      is_staging,
      is_trial,
      umau_value,

      --installation metadata
      instance_user_count,
      collected_data_categories,
      country_name,
      iso_2_country_code,

      --subscription metadata
      original_subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,

      -- account metadata
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      technical_account_manager,

      ping_created_at,
      ping_created_date_month,
      is_last_ping_of_month

    FROM joined

)

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@icooper-acp",
    updated_by="@jpeguero",
    created_date="2022-03-11",
    updated_date="2023-02-01"
) }}
