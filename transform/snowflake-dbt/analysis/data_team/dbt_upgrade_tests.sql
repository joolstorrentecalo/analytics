{%- set test_db = "15383-UPDATE-TO-DBT-V1-2_" -%}
{%- set test_prod = test_db + "PROD" -%}
{%- set test_prep = test_db + "PREP" -%}

-- PROD.common.dim_date
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "dim_date" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;

-- PROD.common_mart_product.mart_product_usage_paid_user_metrics_monthly
{% set table_db = "PROD" %}
{% set table_schema = "common_mart_product" %}
{% set table_name = "mart_product_usage_paid_user_metrics_monthly" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
        
-- PROD.common.fct_event
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "fct_event" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;


  
-- PROD.restricted_safe_common_mart_sales.mart_crm_opportunity_stamped_hierarchy_hist
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common_mart_sales" %}
{% set table_name = "mart_crm_opportunity_stamped_hierarchy_hist" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["days_since_last_activity","dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_common_mart_sales.mart_arr
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common_mart_sales" %}
{% set table_name = "mart_arr" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;

  
-- PROD.legacy.employee_directory_analysis
{% set table_db = "PROD" %}
{% set table_schema = "legacy" %}
{% set table_name = "employee_directory_analysis" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_common_mart_sales.mart_charge
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common_mart_sales" %}
{% set table_name = "mart_charge" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_common_mart_sales.mart_retention_parent_account
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common_mart_sales" %}
{% set table_name = "mart_retention_parent_account" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;

  
-- PROD.common.fct_ping_instance_metric_monthly
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "fct_ping_instance_metric_monthly" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_common_mart_finance.mart_available_to_renew
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common_mart_finance" %}
{% set table_name = "mart_available_to_renew" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.common_mart_product.rpt_event_xmau_metric_monthly
{% set table_db = "PROD" %}
{% set table_schema = "common_mart_product" %}
{% set table_name = "rpt_event_xmau_metric_monthly" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.common.dim_ping_instance
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "dim_ping_instance" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at","IP_ADDRESS_HASH", "IS_LAST_PING_OF_MONTH", "IS_LAST_PING_OF_WEEK"]
) }}
;
  
-- PROD.common_mart.mart_ping_instance_metric
{% set table_db = "PROD" %}
{% set table_schema = "common_mart" %}
{% set table_name = "mart_ping_instance_metric" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at","IS_LAST_PING_OF_MONTH", "IS_LAST_PING_OF_WEEK"]
) }}
;
  
-- PROD.common.dim_namespace
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "dim_namespace" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.common.dim_user
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "dim_user" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_legacy.customers_db_charges_xf
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_legacy" %}
{% set table_name = "customers_db_charges_xf" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;

-- PROD.legacy.customers_db_trial_histories
{% set table_db = "PROD" %}
{% set table_schema = "legacy" %}
{% set table_name = "customers_db_trial_histories" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.legacy.gitlab_dotcom_members
{% set table_db = "PROD" %}
{% set table_schema = "legacy" %}
{% set table_name = "gitlab_dotcom_members" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.legacy.snowplow_unnested_events_all
{% set table_db = "PROD" %}
{% set table_schema = "legacy" %}
{% set table_name = "snowplow_unnested_events_all" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;
  
-- PROD.restricted_safe_common.dim_crm_account
{% set table_db = "PROD" %}
{% set table_schema = "restricted_safe_common" %}
{% set table_name = "dim_crm_account" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at","LAST_MODIFIED_DATE", "LAST_MODIFIED_BY_NAME"]
) }}
;
  
-- PROD.common.dim_crm_user
{% set table_db = "PROD" %}
{% set table_schema = "common" %}
{% set table_name = "dim_crm_user" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;

  
-- PROD.legacy.gitlab_dotcom_projects_xf
{% set table_db = "PROD" %}
{% set table_schema = "legacy" %}
{% set table_name = "gitlab_dotcom_projects_xf" %}
{%- set old_etl_relation=adapter.get_relation(
      database=table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{%- set new_etl_relation=adapter.get_relation(
      database=test_db + table_db,
      schema=table_schema,
      identifier=table_name
) -%}
{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=new_etl_relation,
    exclude_columns=["dbt_updated_at","dbt_created_at"]
) }}
;