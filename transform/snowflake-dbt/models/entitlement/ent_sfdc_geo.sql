
{{ config(
  materialized='table'
  ) }}

WITH
  target_geos AS (
    SELECT *
    FROM (VALUES
            ('AMER'),
            ('EMEA'),
            ('APJ'),
            ('APAC'),
            ('JAPAN'),
            ('PUBSEC'),
            ('SMB')
      ) AS geos (target_geos)
  ),
  
  target_user_roles AS (
    SELECT *
    FROM (VALUES
            ('CMO', TRUE),
            ('CRO', TRUE),
            ('Director - SA High Velocity', TRUE),
            ('Director CSM', TRUE),
            ('Director of Professional Services', TRUE),
            ('Executive', TRUE),
            ('Field CTO', TRUE),
            ('Field Marketing Director', TRUE),
            ('Field Marketing Manager - Global', TRUE),
            ('Global CSE Director', TRUE),
            ('VP Sales Development', TRUE),
            ('VPSA', TRUE),
            ('Executive - Global Minus Pubsec', FALSE),
            ('Executive - No View All', FALSE),
            ('Implementation Engineers - Global (with/without Pubsec)', FALSE),
            ('Marketing Operations Manager', FALSE),
            ('Marketing Program Manager', FALSE)
      ) AS geos (target_roles, has_pubsec)
  ),
  sdfc_user_source AS (
    SELECT *
    FROM {{ ref('dim_crm_user') }} 
  ),
  tableau_group_source AS (
    SELECT * 
    FROM {{ ref('tableau_cloud_groups_source') }}
  ),
  sfdc_filtered AS (
    SELECT
      sdfc_user_source.dim_crm_user_id,
      sdfc_user_source.user_email,
      sdfc_user_source.crm_user_geo,
      sdfc_user_source.user_role_name,
      IFF(target_geos.target_geos IS NOT NULL, TRUE, FALSE) AS has_user_geo,
      IFF(target_user_roles.target_roles IS NOT NULL, TRUE, FALSE) AS has_global,
      IFF(target_user_roles.target_roles IS NOT NULL AND has_pubsec = TRUE, TRUE, FALSE) AS has_pubsec
    FROM sdfc_user_source
    LEFT JOIN target_geos
      ON lower(sdfc_user_source.crm_user_geo) = lower(target_geos.target_geos)
    LEFT JOIN target_user_roles
      ON lower(sdfc_user_source.user_role_name) = lower(target_user_roles.target_roles)
    WHERE is_active = TRUE
  
  ),
  
  tableau_safe_users AS (
    SELECT
      user_email
    FROM tableau_group_source
    WHERE site_luid = '2ae725b5-5a7b-40ba-a05c-35b7aa9ab731'
      AND group_name = 'General SAFE Access'
      AND group_luid = 'e926984d-06de-4536-b572-6d09075a21be'
  ),
  
  geo_list AS (
    SELECT DISTINCT
      crm_user_geo
    FROM sfdc_filtered
    WHERE crm_user_geo IS NOT NULL
      AND has_user_geo = TRUE
  ),
  
  geo_users AS (
    SELECT
      dim_crm_user_id,
      crm_user_geo,
      user_email,
      'sfdc_user_geo' AS entitlement_basis
    FROM sfdc_filtered
    WHERE has_user_geo
  ),
  
  all_accounts AS (
    SELECT
      geo_list.crm_user_geo,
      sfdc_filtered.user_email,
      'tableau_safe_and_sfdc_role' AS entitlement_basis
    FROM sfdc_filtered
    INNER JOIN tableau_safe_users
      ON sfdc_filtered.user_email = tableau_safe_users.user_email
    LEFT JOIN geo_users
      ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
    CROSS JOIN geo_list
    WHERE geo_users.user_email IS NULL
      AND sfdc_filtered.has_global = TRUE
      AND sfdc_filtered.has_pubsec = TRUE
  ),
  
  non_pubsec AS (
    
    SELECT
      geo_list.crm_user_geo,
      sfdc_filtered.user_email,
      'tableau_safe_and_sfdc_role_less_pubsec' AS entitlement_basis
    FROM sfdc_filtered
    INNER JOIN tableau_safe_users
      ON sfdc_filtered.user_email = tableau_safe_users.user_email
    LEFT JOIN geo_users
      ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
    CROSS JOIN geo_list
    WHERE geo_users.user_email IS NULL
      AND sfdc_filtered.has_pubsec = FALSE
      AND sfdc_filtered.has_global = TRUE
      AND lower(geo_list.crm_user_geo) != lower('PUBSEC')
  ),

combined AS (
SELECT *
FROM non_pubsec

UNION ALL

SELECT *
FROM all_accounts

UNION ALL

SELECT
  geo_users.crm_user_geo,
  geo_users.user_email,
  entitlement_basis
FROM geo_users
)

SELECT *
FROM combined
