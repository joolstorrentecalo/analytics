{{ config({
    "tags": ["mnpi_exception"],
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_user_hierarchy_sk') }}"
}) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_crm_user_daily_snapshot', 'prep_crm_user_daily_snapshot'),
    ('prep_crm_account_daily_snapshot', 'prep_crm_account_daily_snapshot'),
    ('prep_crm_account', 'prep_crm_account'),
    ('prep_crm_opportunity', 'prep_crm_opportunity'),
    ('prep_sales_funnel_target', 'prep_sales_funnel_target'),
    ('prep_sales_funnel_partner_alliance_target', 'prep_sales_funnel_partner_alliance_target'),
    ('prep_crm_person', 'prep_crm_person')
]) }}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date

), current_fiscal_year AS (

    SELECT fiscal_year
    FROM dim_date
    WHERE date_actual = CURRENT_DATE - 1

), sheetload_sales_funnel_targets_matrix_source AS (

    SELECT 
      sheetload_sales_funnel_targets_matrix_source.*
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source') }}

), sheetload_sales_funnel_partner_alliance_targets_matrix_source AS (

    SELECT 
      sheetload_sales_funnel_partner_alliance_targets_matrix_source.*
    FROM {{ ref('sheetload_sales_funnel_partner_alliance_targets_matrix_source') }}
  
), account_demographics_hierarchy AS (

    SELECT DISTINCT
      created_date_fiscal_year                  AS fiscal_year,
      UPPER(account_demographics_sales_segment) AS account_demographics_sales_segment,
      UPPER(account_demographics_geo)           AS account_demographics_geo,
      UPPER(account_demographics_region)        AS account_demographics_region,
      UPPER(account_demographics_area)          AS account_demographics_area,
      NULL                                      AS user_business_unit,
      dim_account_demographics_hierarchy_sk
    FROM prep_crm_person 

), user_hierarchy_source AS (

    SELECT 
      DISTINCT 
      dim_date.fiscal_year,
      prep_crm_user_daily_snapshot.crm_user_sales_segment              AS user_segment,
      prep_crm_user_daily_snapshot.crm_user_geo                        AS user_geo,
      prep_crm_user_daily_snapshot.crm_user_region                     AS user_region,
      prep_crm_user_daily_snapshot.crm_user_area                       AS user_area,
      prep_crm_user_daily_snapshot.crm_user_business_unit              AS user_business_unit,
      prep_crm_user_daily_snapshot.dim_crm_user_hierarchy_sk
    FROM prep_crm_user_daily_snapshot
    INNER JOIN dim_date 
      ON prep_crm_user_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE prep_crm_user_daily_snapshot.crm_user_sales_segment IS NOT NULL
      AND prep_crm_user_daily_snapshot.crm_user_geo IS NOT NULL
      AND prep_crm_user_daily_snapshot.crm_user_region IS NOT NULL
      AND prep_crm_user_daily_snapshot.crm_user_area IS NOT NULL
      AND IFF(dim_date.fiscal_year > 2023, prep_crm_user_daily_snapshot.crm_user_business_unit IS NOT NULL, 1=1) -- with the change in structure, business unit must be present after FY23
      AND IFF(dim_date.fiscal_year < dim_date.current_fiscal_year,dim_date.date_actual = dim_date.last_day_of_fiscal_year, dim_date.date_actual = dim_date.current_date_actual) -- take only the last valid hierarchy of the fiscal year for previous fiscal years
      AND prep_crm_user_daily_snapshot.is_active = TRUE 

), account_hierarchy_snapshot_source AS (

    SELECT 
      DISTINCT 
      dim_date.fiscal_year,
      prep_crm_account_daily_snapshot.parent_crm_account_sales_segment,
      prep_crm_account_daily_snapshot.parent_crm_account_geo,
      prep_crm_account_daily_snapshot.parent_crm_account_region,
      prep_crm_account_daily_snapshot.parent_crm_account_area,
      prep_crm_account_daily_snapshot.parent_crm_account_business_unit,
      prep_crm_account_daily_snapshot.dim_crm_parent_account_hierarchy_sk
    FROM prep_crm_account_daily_snapshot
    INNER JOIN dim_date 
      ON prep_crm_account_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE prep_crm_account_daily_snapshot.parent_crm_account_sales_segment IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_geo IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_region IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_area IS NOT NULL
      AND IFF(dim_date.fiscal_year > 2023, prep_crm_account_daily_snapshot.parent_crm_account_business_unit IS NOT NULL, TRUE) -- with the change in structure, business unit must be present after FY23
      AND IFF(dim_date.fiscal_year < dim_date.current_fiscal_year, dim_date.date_actual = dim_date.last_day_of_fiscal_year, dim_date.date_actual = dim_date.current_date_actual) -- take only the last valid hierarchy of the fiscal year for previous fiscal years

), account_hierarchy_source AS (

    SELECT 
      DISTINCT 
      current_fiscal_year.fiscal_year,
      prep_crm_account.parent_crm_account_sales_segment,
      prep_crm_account.parent_crm_account_geo,
      prep_crm_account.parent_crm_account_region,
      prep_crm_account.parent_crm_account_area,
      prep_crm_account.parent_crm_account_business_unit,
      prep_crm_account.dim_crm_parent_account_hierarchy_sk
    FROM prep_crm_account
    LEFT JOIN current_fiscal_year
    WHERE prep_crm_account.parent_crm_account_sales_segment IS NOT NULL
      AND prep_crm_account.parent_crm_account_geo IS NOT NULL
      AND prep_crm_account.parent_crm_account_region IS NOT NULL
      AND prep_crm_account.parent_crm_account_area IS NOT NULL

), user_hierarchy_sheetload AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the target setting model, we will union in the distinct hierarchy values from the file.
*/

    SELECT DISTINCT 
      prep_sales_funnel_target.fiscal_year,
      prep_sales_funnel_target.user_segment,
      prep_sales_funnel_target.user_geo,
      prep_sales_funnel_target.user_region,
      prep_sales_funnel_target.user_area,
      prep_sales_funnel_target.user_business_unit,
      prep_sales_funnel_target.dim_crm_user_hierarchy_sk
    FROM prep_sales_funnel_target
    WHERE prep_sales_funnel_target.user_area != 'N/A'
      AND prep_sales_funnel_target.user_segment IS NOT NULL
      AND prep_sales_funnel_target.user_geo IS NOT NULL
      AND prep_sales_funnel_target.user_region IS NOT NULL
      AND prep_sales_funnel_target.user_area IS NOT NULL

), user_hierarchy_sheetload_partner_alliance AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the target setting model, we will union in the distinct hierarchy values from the partner and alliance file.
*/

    SELECT DISTINCT 
      prep_sales_funnel_partner_alliance_target.fiscal_year,
      prep_sales_funnel_partner_alliance_target.user_segment,
      prep_sales_funnel_partner_alliance_target.user_geo,
      prep_sales_funnel_partner_alliance_target.user_region,
      prep_sales_funnel_partner_alliance_target.user_area,
      prep_sales_funnel_partner_alliance_target.user_business_unit,
      prep_sales_funnel_partner_alliance_target.dim_crm_user_hierarchy_sk
    FROM prep_sales_funnel_partner_alliance_target
    WHERE prep_sales_funnel_partner_alliance_target.user_area != 'N/A'
      AND prep_sales_funnel_partner_alliance_target.area IS NOT NULL

), user_hierarchy_stamped_opportunity AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
*/

    SELECT DISTINCT
      prep_crm_opportunity.close_fiscal_year                         AS fiscal_year,
      prep_crm_opportunity.user_segment_stamped                      AS user_segment,
      prep_crm_opportunity.user_geo_stamped                          AS user_geo,
      prep_crm_opportunity.user_region_stamped                       AS user_region,
      prep_crm_opportunity.user_area_stamped                         AS user_area,
      prep_crm_opportunity.user_business_unit_stamped                AS user_business_unit,
      prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk    AS dim_crm_user_hierarchy_sk
    FROM prep_crm_opportunity
    WHERE is_live = 1
  
), unioned AS (
/*
  Union all four hierarchy sources to combine all possible hierarchies generated used in the past, as well as those not currently used in the system, but used in target setting.
*/

    SELECT *
    FROM user_hierarchy_source
  
    UNION
 
    SELECT *
    FROM user_hierarchy_sheetload

    UNION

    SELECT *
    FROM  user_hierarchy_sheetload_partner_alliance

    UNION

    SELECT *
    FROM user_hierarchy_stamped_opportunity

    UNION

    SELECT *
    FROM account_hierarchy_snapshot_source

    UNION

    SELECT *
    FROM account_hierarchy_source

    UNION

    SELECT *
    FROM account_demographics_hierarchy

), pre_fy24_hierarchy AS (

/*
  Before FY24, the hierarchy only uncluded segment, geo, region, and area.
*/

    SELECT DISTINCT
      fiscal_year,
      UPPER(user_segment) AS user_segment,
      UPPER(user_geo)     AS user_geo,
      UPPER(user_region)  AS user_region,
      UPPER(user_area)    AS user_area,
      NULL                AS user_business_unit,
      dim_crm_user_hierarchy_sk
    FROM unioned 
    WHERE fiscal_year < 2024

), fy24_and_beyond_hierarchy AS (

/*
  After FY24, business unit was added to the hierarchy.
*/


    SELECT DISTINCT
      fiscal_year,
      UPPER(user_segment)       AS user_segment,
      UPPER(user_geo)           AS user_geo,
      UPPER(user_region)        AS user_region,
      UPPER(user_area)          AS user_area,
      UPPER(user_business_unit) AS user_business_unit,
      dim_crm_user_hierarchy_sk
    FROM unioned 
    WHERE fiscal_year >= 2024

), final_unioned AS (


    SELECT *
    FROM pre_fy24_hierarchy

    UNION ALL 

    SELECT *
    FROM fy24_and_beyond_hierarchy

), final AS (

    SELECT DISTINCT 
      {{ dbt_utils.generate_surrogate_key(['final_unioned.dim_crm_user_hierarchy_sk']) }}                                                AS dim_crm_user_hierarchy_id,
      final_unioned.dim_crm_user_hierarchy_sk,
      final_unioned.fiscal_year,
      final_unioned.user_business_unit                                                                                          AS crm_user_business_unit,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_business_unit']) }}                                                       AS dim_crm_user_business_unit_id,
      final_unioned.user_segment                                                                                                AS crm_user_sales_segment,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_segment']) }}                                                             AS dim_crm_user_sales_segment_id,
      final_unioned.user_geo                                                                                                    AS crm_user_geo,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_geo']) }}                                                                 AS dim_crm_user_geo_id,
      final_unioned.user_region                                                                                                 AS crm_user_region,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_region']) }}                                                              AS dim_crm_user_region_id,
      final_unioned.user_area                                                                                                   AS crm_user_area,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_area']) }}                                                                AS dim_crm_user_area_id,
      CASE
          WHEN final_unioned.user_segment IN ('Large', 'PubSec') THEN 'Large'
          ELSE final_unioned.user_segment
      END                                                                                                                       AS crm_user_sales_segment_grouped,
      {{ sales_segment_region_grouped('final_unioned.user_segment', 'final_unioned.user_geo', 'final_unioned.user_region') }}   AS crm_user_sales_segment_region_grouped,
      IFF(final_unioned.fiscal_year = current_fiscal_year.fiscal_year, 1, 0)                                                    AS is_current_crm_user_hierarchy
    FROM final_unioned
    LEFT JOIN current_fiscal_year
      ON final_unioned.fiscal_year = current_fiscal_year.fiscal_year
    WHERE dim_crm_user_hierarchy_sk IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2021-01-05",
    updated_date="2023-10-20"
) }}
