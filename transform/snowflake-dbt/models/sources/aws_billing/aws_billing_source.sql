{{ config(
    materialized='incremental',
    )
}}

WITH dedicated_legacy_0475 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'dedicated_legacy_0475') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}

),

dedicated_dev_3675 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'dedicated_dev_3675') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}

),

gitlab_marketplace_5127 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'gitlab_marketplace_5127') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}
),

itorg_3027 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'itorg_3027') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}

),

legacy_gitlab_0347 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'legacy_gitlab_0347') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}

),

services_org_6953 AS (

  SELECT * , metadata$FILE_LAST_MODIFIED AS modified_at_ FROM {{ source('aws_billing', 'services_org_6953') }}
  {% if is_incremental() %}

  WHERE metadata$FILE_LAST_MODIFIED  >= (SELECT MAX(modified_at) FROM {{this}})

  {% endif %}

),

all_raw AS (

SELECT * FROM dedicated_legacy_0475
UNION ALL
SELECT * FROM dedicated_dev_3675
UNION ALL
SELECT * FROM gitlab_marketplace_5127
UNION ALL
SELECT * FROM itorg_3027
UNION ALL
SELECT * FROM legacy_gitlab_0347
UNION ALL
SELECT * FROM services_org_6953

)

SELECT     
  value['bill_bill_type']::VARCHAR AS bill_bill_type,
  value['bill_billing_entity']::VARCHAR as bill_billing_entity,
  value['bill_billing_period_end_date']::TIMESTAMP as bill_billing_period_end_date,
  value['bill_billing_period_start_date']::TIMESTAMP as bill_billing_period_start_date,
  value['bill_invoice_id']::VARCHAR as bill_invoice_id,
  value['bill_invoicing_entity']::VARCHAR as bill_invoicing_entity,
  value['bill_payer_account_id']::VARCHAR as bill_payer_account_id,
  value['discount_edp_discount']::FLOAT as discount_edp_discount,
  value['discount_total_discount']::FLOAT as discount_total_discount,
  value['identity_line_item_id']::VARCHAR as identity_line_item_id,
  value['identity_time_interval']::VARCHAR as identity_time_interval,
  value['line_item_availability_zone']::VARCHAR as line_item_availability_zone,
  value['line_item_blended_cost']::FLOAT as line_item_blended_cost,
  value['line_item_blended_rate']::VARCHAR as line_item_blended_rate,
  value['line_item_currency_code']::VARCHAR as line_item_currency_code,
  value['line_item_legal_entity']::VARCHAR as line_item_legal_entity,
  value['line_item_line_item_description']::VARCHAR as line_item_line_item_description,
  value['line_item_line_item_type']::VARCHAR as line_item_line_item_type,
  value['line_item_net_unblended_cost']::FLOAT as line_item_net_unblended_cost,
  value['line_item_net_unblended_rate']::VARCHAR as line_item_net_unblended_rate,
  value['line_item_normalization_factor']::FLOAT as line_item_normalization_factor,
  value['line_item_normalized_usage_amount']::FLOAT as line_item_normalized_usage_amount,
  value['line_item_operation']::VARCHAR as line_item_operation,
  value['line_item_product_code']::VARCHAR as line_item_product_code,
  value['line_item_resource_id']::VARCHAR as line_item_resource_id,
  value['line_item_tax_type']::VARCHAR as line_item_tax_type,
  value['line_item_unblended_cost']::FLOAT as line_item_unblended_cost,
  value['line_item_unblended_rate']::VARCHAR as line_item_unblended_rate,
  value['line_item_usage_account_id']::VARCHAR as line_item_usage_account_id,
  value['line_item_usage_amount']::FLOAT as line_item_usage_amount,
  TRY_CAST(to_varchar(value['line_item_usage_start_date']) AS TIMESTAMP_NTZ) as line_item_usage_start_date,
  TRY_CAST(to_varchar(value['line_item_usage_end_date']) AS TIMESTAMP_NTZ) as line_item_usage_end_date,
  value['line_item_usage_type']::VARCHAR as line_item_usage_type,
  value['pricing_currency']::VARCHAR as pricing_currency,
  value['pricing_public_on_demand_cost']::FLOAT as pricing_public_on_demand_cost,
  value['pricing_public_on_demand_rate']::VARCHAR as pricing_public_on_demand_rate,
  value['pricing_rate_code']::VARCHAR as pricing_rate_code,  
  value['pricing_rate_id']::VARCHAR as pricing_rate_id,
  value['pricing_term']::VARCHAR as pricing_term,
  value['pricing_unit']::VARCHAR as pricing_unit,
  value['product_alarm_type']::VARCHAR as product_alarm_type,
  value['product_availability']::VARCHAR as product_availability,
  value['product_availability_zone']::VARCHAR as product_availability_zone,
  value['product_cache_engine']::VARCHAR as product_cache_engine,
  value['product_capacitystatus']::VARCHAR as product_capacitystatus,
  value['product_classicnetworkingsupport']::VARCHAR as product_classicnetworkingsupport,
  value['product_clock_speed']::VARCHAR as product_clock_speed,
  value['product_content_type']::VARCHAR as product_content_type,
  value['product_current_generation']::VARCHAR as product_current_generation,
  value['product_database_engine']::VARCHAR as product_database_engine,
  value['product_dedicated_ebs_throughput']::VARCHAR as product_dedicated_ebs_throughput,
  value['product_deployment_option']::VARCHAR as product_deployment_option,
  value['product_description']::VARCHAR as product_description,
  value['product_durability']::VARCHAR as product_durability,  
  value['product_ecu']::VARCHAR as product_ecu,
  value['product_engine_code']::VARCHAR as product_engine_code,
  value['product_enhanced_networking_supported']::VARCHAR as product_enhanced_networking_supported,
  value['product_free_tier']::VARCHAR as product_free_tier,
  value['product_from_location']::VARCHAR as product_from_location,
  value['product_from_location_type']::VARCHAR as product_from_location_type,
  value['product_from_region_code']::VARCHAR as product_from_region_code,
  value['product_group']::VARCHAR as product_group,
  value['product_group_description']::VARCHAR as product_group_description,
  value['product_instance_family']::VARCHAR as product_instance_family,
  value['product_instance_type']::VARCHAR as product_instance_type,
  value['product_instance_type_family']::VARCHAR as product_instance_type_family,
  value['product_intel_avx2_available']::VARCHAR as product_intel_avx2_available,
  value['product_intel_avx_available']::VARCHAR as product_intel_avx_available,
  value['product_intel_turbo_available']::VARCHAR as product_intel_turbo_available,
  value['product_license_model']::VARCHAR as product_license_model,
  value['product_location']::VARCHAR as product_location,
  value['product_location_type']::VARCHAR as product_location_type,
  value['product_logs_destination']::VARCHAR as product_logs_destination,
  value['product_mailbox_storage']::VARCHAR as product_mailbox_storage,
  value['product_marketoption']::VARCHAR as product_marketoption,
  value['product_max_iops_burst_performance']::VARCHAR as product_max_iops_burst_performance,
  value['product_max_iopsvolume']::VARCHAR as product_max_iopsvolume,
  value['product_max_throughputvolume']::VARCHAR as product_max_throughputvolume,
  value['product_max_volume_size']::VARCHAR as product_max_volume_size,
  value['product_memory']::VARCHAR as product_memory,
  value['product_memory_gib']::VARCHAR as product_memory_gib,
  value['product_message_delivery_frequency']::VARCHAR as product_message_delivery_frequency,
  value['product_message_delivery_order']::VARCHAR as product_message_delivery_order,
  value['product_min_volume_size']::VARCHAR as product_min_volume_size,
  value['product_network_performance']::VARCHAR as product_network_performance,
  value['product_normalization_size_factor']::VARCHAR as product_normalization_size_factor,
  value['product_operating_system']::VARCHAR as product_operating_system,
  value['product_operation']::VARCHAR as product_operation,
  value['product_origin']::VARCHAR as product_origin,
  value['product_physical_processor']::VARCHAR as product_physical_processor,
  value['product_platopricingtype']::VARCHAR as product_platopricingtype,
  value['product_platousagetype']::VARCHAR as product_platousagetype, 
  value['product_platovolumetype']::VARCHAR as product_platovolumetype,
  value['product_pre_installed_sw']::VARCHAR as product_pre_installed_sw,
  value['product_pricing_unit']::VARCHAR as product_pricing_unit,
  value['product_processor_architecture']::VARCHAR as product_processor_architecture,
  value['product_processor_features']::VARCHAR as product_processor_features,
  value['product_product_family']::VARCHAR as product_product_family,
  value['product_product_name']::VARCHAR as product_product_name,
  value['product_provisioned']::VARCHAR as product_provisioned,
  value['product_queue_type']::VARCHAR as product_queue_type,
  value['product_recipient']::VARCHAR as product_recipient,
  value['product_region']::VARCHAR as product_region,
  value['product_region_code']::VARCHAR as product_region_code,
  value['product_routing_target']::VARCHAR as product_routing_target,
  value['product_routing_type']::VARCHAR as product_routing_type,
  value['product_servicecode']::VARCHAR as product_servicecode,
  value['product_servicename']::VARCHAR as product_servicename,
  value['product_sku']::VARCHAR as product_sku,
  value['product_storage']::VARCHAR as product_storage,
  value['product_storage_class']::VARCHAR as product_storage_class,
  value['product_storage_media']::VARCHAR as product_storage_media,
  value['product_tenancy']::VARCHAR as product_tenancy,
  value['product_tickettype']::VARCHAR as product_tickettype,
  value['product_tiertype']::VARCHAR as product_tiertype,
  value['product_to_location']::VARCHAR as product_to_location,
  value['product_to_location_type']::VARCHAR as product_to_location_type,
  value['product_to_region_code']::VARCHAR as product_to_region_code,
  value['product_transfer_type']::VARCHAR as product_transfer_type,
  value['product_usagetype']::VARCHAR as product_usagetype,
  value['product_vcpu']::VARCHAR as product_vcpu,
  value['product_version']::VARCHAR as product_version,
  value['product_volume_api_name']::VARCHAR as product_volume_api_name,
  value['product_volume_type']::VARCHAR as product_volume_type,
  value['product_vpcnetworkingsupport']::VARCHAR as product_vpcnetworkingsupport,
  value['reservation_amortized_upfront_cost_for_usage']::FLOAT as reservation_amortized_upfront_cost_for_usage,
  value['reservation_amortized_upfront_fee_for_billing_period']::FLOAT as reservation_amortized_upfront_fee_for_billing_period,
  value['reservation_effective_cost']::FLOAT as reservation_effective_cost,
  TRY_CAST(to_varchar(value['reservation_end_time']) AS TIMESTAMP) as reservation_end_time, 
  value['reservation_modification_status']::VARCHAR as reservation_modification_status,
  value['reservation_net_amortized_upfront_cost_for_usage']::FLOAT as reservation_net_amortized_upfront_cost_for_usage,
  value['reservation_net_amortized_upfront_fee_for_billing_period']::FLOAT as reservation_net_amortized_upfront_fee_for_billing_period,
  value['reservation_net_effective_cost']::FLOAT as reservation_net_effective_cost,
  value['reservation_net_recurring_fee_for_usage']::FLOAT as reservation_net_recurring_fee_for_usage,
  value['reservation_net_unused_amortized_upfront_fee_for_billing_period']::FLOAT as reservation_net_unused_amortized_upfront_fee_for_billing_period,
  value['reservation_net_unused_recurring_fee']::FLOAT as reservation_net_unused_recurring_fee,
  value['reservation_net_upfront_value']::FLOAT as reservation_net_upfront_value,
  value['reservation_normalized_units_per_reservation']::VARCHAR as reservation_normalized_units_per_reservation,
  value['reservation_number_of_reservations']::VARCHAR as reservation_number_of_reservations,
  value['reservation_recurring_fee_for_usage']::FLOAT as reservation_recurring_fee_for_usage,
  TRY_CAST(to_varchar(value['reservation_start_time']) AS TIMESTAMP) as reservation_start_time, 
  value['reservation_subscription_id']::VARCHAR as reservation_subscription_id,
  value['reservation_total_reserved_normalized_units']::VARCHAR as reservation_total_reserved_normalized_units,
  value['reservation_total_reserved_units']::VARCHAR as reservation_total_reserved_units,
  value['reservation_units_per_reservation']::VARCHAR as reservation_units_per_reservation,
  value['reservation_unused_amortized_upfront_fee_for_billing_period']::FLOAT as reservation_unused_amortized_upfront_fee_for_billing_period,
  value['reservation_unused_normalized_unit_quantity']::FLOAT as reservation_unused_normalized_unit_quantity,
  value['reservation_unused_quantity']::FLOAT as reservation_unused_quantity,
  value['reservation_unused_recurring_fee']::FLOAT as reservation_unused_recurring_fee,
  value['reservation_upfront_value']::FLOAT as reservation_upfront_value,
  value['savings_plan_amortized_upfront_commitment_for_billing_period']::FLOAT as savings_plan_amortized_upfront_commitment_for_billing_period,
  value['savings_plan_net_amortized_upfront_commitment_for_billing_period']::DECIMAL as savings_plan_net_amortized_upfront_commitment_for_billing_period,
  value['savings_plan_net_recurring_commitment_for_billing_period']::DECIMAL as savings_plan_net_recurring_commitment_for_billing_period,
  value['savings_plan_net_savings_plan_effective_cost']::DECIMAL as savings_plan_net_savings_plan_effective_cost,
  value['savings_plan_recurring_commitment_for_billing_period']::DECIMAL as savings_plan_recurring_commitment_for_billing_period,
  value['savings_plan_savings_plan_a_r_n']::VARCHAR as savings_plan_savings_plan_a_r_n,
  value['savings_plan_savings_plan_effective_cost']::DECIMAL as savings_plan_savings_plan_effective_cost,
  value['savings_plan_savings_plan_rate']::DECIMAL as savings_plan_savings_plan_rate,
  value['savings_plan_total_commitment_to_date']::DECIMAL as savings_plan_total_commitment_to_date,
  value['savings_plan_used_commitment']::DECIMAL as savings_plan_used_commitment,
  modified_at_ as modified_at
FROM all_raw
