{{ config(alias='sfdc_users_xf') }}

WITH source_user AS (

    SELECT 
        sfdc_users.id                   AS user_id,
        sfdc_users.user_segment__c      AS user_segment,
        sfdc_users.user_role_type__c    AS user_role_type,
        sfdc_user_roles_source.name     AS user_role_name,
        COALESCE(CAST(REPLACE(
                REPLACE(sfdc_users.hybrid__c,'Yes','1')
                ,'No','0') 
            AS INTEGER),0)              AS is_hybrid_flag
    FROM {{ source('salesforce', 'user') }} sfdc_users
    LEFT JOIN {{ ref('sfdc_user_roles_source') }} sfdc_user_roles_source
      ON sfdc_users.userroleid = sfdc_user_roles_source.id

),  base AS (
    SELECT
      edm_user.dim_crm_user_id           AS user_id,
      edm_user.user_name                 AS name,
      edm_user.department,
      edm_user.title,
      edm_user.team,
      CASE --only expose GitLab.com email addresses of internal employees
        WHEN edm_user.user_email LIKE '%gitlab.com' THEN edm_user.user_email ELSE NULL
      END                       AS user_email,
      edm_user.manager_name,
      edm_user.manager_id,
      
      IFNULL(edm_user.crm_user_geo, 'N/A')                AS user_geo,
      IFNULL(edm_user.crm_user_business_unit, 'N/A')      AS user_business_unit,
      IFNULL(edm_user.crm_user_region, 'N/A')             AS user_region,
      IFNULL(edm_user.crm_user_area, 'N/A')               AS user_area,

      CASE 
        WHEN LOWER(source_user.user_segment) = 'lrg' 
          THEN 'Large'
        WHEN LOWER(source_user.user_segment) = 'mm' 
          THEN 'Mid-Market' 
        WHEN LOWER(source_user.user_segment) = 'jihu' 
          THEN 'Jihu'         
        WHEN LOWER(source_user.user_segment) = 'all' 
          THEN 'All'        
        ELSE
          IFNULL(source_user.user_segment, 'N/A') 
      END                                                   AS final_user_segment,

      LOWER(source_user.user_segment)                       AS raw_user_segment,

        -- NF: in FY24 we adjusted segments to represent the new GTM structure
        -- for FY25 we are not doing that anymore. 
      IFNULL(final_user_segment, 'N/A') AS adjusted_user_segment,

    
      IFNULL(edm_user.user_role_name, 'Other')         AS role_name,
      IFNULL(edm_user.crm_user_role_type, 'Other')     AS role_type,
      edm_user.start_date,
      edm_user.is_active,
      edm_user.employee_number,

      source_user.is_hybrid_flag

    FROM {{ref('dim_crm_user')}} edm_user
    LEFT JOIN source_user
        ON edm_user.dim_crm_user_id = source_user.user_id

), consolidation AS (
    SELECT
      base.user_id,
      base.name,
      base.department,
      base.title,
      base.team,
      base.user_email,
      base.manager_name,
      base.manager_id,
      base.user_geo,
      base.user_region,
      -- NF: adjusted to account for the updates the data team ran on source
      -- Needed to adjust ALL to Large
      base.final_user_segment AS user_segment,
      base.raw_user_segment,
      base.adjusted_user_segment,
      base.user_area,
      base.role_name,
      base.role_type,
      base.start_date,
      base.is_active,
      base.is_hybrid_flag,
      base.employee_number,

     
      CASE
        WHEN LOWER(title) LIKE '%strategic account%'
           OR LOWER(title) LIKE '%account executive%'
           OR LOWER(title) LIKE '%country manager%'
           OR LOWER(title) LIKE '%public sector channel manager%'
           OR LOWER(role_name) LIKE '%ae_%'
        THEN 1
        ELSE 0
      END                                                                                          AS is_rep_flag

    FROM base

), user_based_reporting_keys AS (
    SELECT
      consolidation.*,


      -- NF: These fields need to be remvoved           
      user_geo AS business_unit,
      -- Sub-Business Unit (X-Ray 2nd hierarchy)
      /*
      NF: 2024-01-30 In FY24 we created new fields to accomodate a complex logic that was used to simplify reporting.
      In FY25 that logic is pretty much already integrated in the geo, bu, region, area fields

      I am maintaining this fields for now, but changing the order, this fields are logical fields, not necessarily correlate with 
      with the SFDC field. For example, Business Unit here is equivalent to GEO. This fields could be better called Levels. 
      */
      user_business_unit                AS sub_business_unit,
      -- Division (X-Ray 3rd hierarchy)
      user_region                       AS division,
      -- ASM (X-Ray 4th hierarchy): definition pending
      user_area                         AS asm
    FROM consolidation

), final AS (

    SELECT *
    FROM user_based_reporting_keys

)

SELECT *,

    -- Fy24 GTM keys
    LOWER(business_unit)                                                              AS key_bu,
    LOWER(business_unit || '_' || sub_business_unit)                                  AS key_bu_subbu,
    LOWER(business_unit || '_' || sub_business_unit || '_' || division)               AS key_bu_subbu_division,
    LOWER(business_unit || '_' || sub_business_unit || '_' || division || '_' || asm) AS key_bu_subbu_division_asm,
    --FY24 LOWER(key_bu_subbu_division_asm || '_' || role_type || '_' || TO_VARCHAR(employee_number))  AS key_sal_heatmap

    -- FY25 GTM keys
    LOWER(user_geo)                                                                         AS key_geo,
    LOWER(user_geo || '_' || user_business_unit)                                            AS key_geo_bu,
    LOWER(user_geo || '_' || user_business_unit || '_' || user_region)                      AS key_geo_bu_region,
    LOWER(user_geo || '_' || user_business_unit || '_' || user_region || '_' || user_area)  AS key_geo_bu_region_area,
    LOWER(key_geo_bu_region_area || '_' || role_type || '_' || TO_VARCHAR(employee_number)) AS key_sal_heatmap

FROM final