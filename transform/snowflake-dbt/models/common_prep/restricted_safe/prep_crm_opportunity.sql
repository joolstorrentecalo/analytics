{{ config(
    tags=["six_hourly"],
    materialized="incremental",
    unique_key="primary_key",
    on_schema_change="sync_all_columns"
) }}

{{ simple_cte([
    ('net_iacv_to_net_arr_ratio', 'net_iacv_to_net_arr_ratio'),
    ('dim_date', 'dim_date'),
    ('sfdc_opportunity_stage_source', 'sfdc_opportunity_stage_source'),
    ('sfdc_opportunity_source', 'sfdc_opportunity_source'),
    ('sfdc_opportunity_snapshots_source','sfdc_opportunity_snapshots_source'),
    ('sfdc_opportunity_stage', 'sfdc_opportunity_stage_source'),
    ('sfdc_record_type_source', 'sfdc_record_type_source'),
    ('sfdc_account_snapshots_source','sfdc_account_snapshots_source')
]) }}

, first_contact  AS (

    SELECT
      opportunity_id,                                                             -- opportunity_id
      contact_id                                                                  AS sfdc_contact_id,
      {{ dbt_utils.generate_surrogate_key(['contact_id']) }}                      AS dim_crm_person_id,
      ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC)   AS row_num
    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), account_history_final AS (
 
  SELECT
    account_id_18 AS dim_crm_account_id,
    owner_id AS dim_crm_user_id,
    ultimate_parent_id AS dim_crm_parent_account_id,
    abm_tier_1_date,
    abm_tier_2_date,
    abm_tier,
    MIN(dbt_valid_from)::DATE AS valid_from,
    MAX(dbt_valid_to)::DATE AS valid_to
  FROM sfdc_account_snapshots_source
  WHERE abm_tier_1_date >= '2022-02-01'
    OR abm_tier_2_date >= '2022-02-01'
  {{dbt_utils.group_by(n=6)}}

), attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), linear_attribution_base AS ( --the number of attribution touches a given opp has in total
    --linear attribution IACV of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)
    SELECT
     opportunity_id                                         AS dim_crm_opportunity_id,
     COUNT(DISTINCT attribution_touchpoints.touchpoint_id)  AS count_crm_attribution_touchpoints
    FROM  attribution_touchpoints
    GROUP BY 1

), campaigns_per_opp as (

    SELECT
      opportunity_id                                        AS dim_crm_opportunity_id,
      COUNT(DISTINCT attribution_touchpoints.campaign_id)   AS count_campaigns
    FROM attribution_touchpoints
    GROUP BY 1

), snapshot_dates AS (

    SELECT *
    FROM dim_date
    WHERE date_actual::DATE >= '2020-02-01' -- Restricting snapshot model to only have data from this date forward. More information https://gitlab.com/gitlab-data/analytics/-/issues/14418#note_1134521216
      AND date_actual < CURRENT_DATE

    {% if is_incremental() %}

      AND date_actual > (SELECT MAX(snapshot_date) FROM {{ this }} WHERE is_live = 0)

    {% endif %}

), live_date AS (

    SELECT *
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

), sfdc_account_snapshot AS (

    SELECT *
    FROM {{ ref('prep_crm_account_daily_snapshot') }}

), sfdc_user_snapshot AS (

    SELECT *
    FROM {{ ref('prep_crm_user_daily_snapshot') }}

), sfdc_account AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

), sfdc_user AS (

    SELECT *
    FROM {{ ref('prep_crm_user') }}

), sfdc_opportunity_snapshot AS (

    SELECT
      sfdc_opportunity_snapshots_source.account_id                                                                  AS dim_crm_account_id,
      sfdc_opportunity_snapshots_source.opportunity_id                                                              AS dim_crm_opportunity_id,
      sfdc_opportunity_snapshots_source.owner_id                                                                    AS dim_crm_user_id,
      sfdc_account_snapshot.dim_crm_user_id                                                                         AS dim_crm_account_user_id,
      sfdc_opportunity_snapshots_source.parent_opportunity_id                                                       AS dim_parent_crm_opportunity_id,
      sfdc_opportunity_snapshots_source.order_type_stamped                                                          AS order_type,
      sfdc_opportunity_snapshots_source.opportunity_term                                                            AS opportunity_term_base,
      {{ sales_qualified_source_cleaning('sfdc_opportunity_snapshots_source.sales_qualified_source') }}             AS sales_qualified_source,
      sfdc_opportunity_snapshots_source.user_segment_stamped                                                        AS crm_opp_owner_sales_segment_stamped,
      sfdc_opportunity_snapshots_source.user_geo_stamped                                                            AS crm_opp_owner_geo_stamped,
      sfdc_opportunity_snapshots_source.user_region_stamped                                                         AS crm_opp_owner_region_stamped,
      sfdc_opportunity_snapshots_source.user_area_stamped                                                           AS crm_opp_owner_area_stamped,
      sfdc_opportunity_snapshots_source.user_segment_geo_region_area_stamped                                        AS crm_opp_owner_sales_segment_geo_region_area_stamped,
      sfdc_opportunity_snapshots_source.user_business_unit_stamped                                                  AS crm_opp_owner_business_unit_stamped,
      sfdc_opportunity_snapshots_source.created_date::DATE                                                          AS created_date,
      sfdc_opportunity_snapshots_source.sales_accepted_date::DATE                                                   AS sales_accepted_date,
      sfdc_opportunity_snapshots_source.close_date::DATE                                                            AS close_date,
      sfdc_opportunity_snapshots_source.net_arr                                                                     AS raw_net_arr,
      {{ dbt_utils.generate_surrogate_key(['sfdc_opportunity_snapshots_source.opportunity_id','snapshot_dates.date_id'])}}   AS crm_opportunity_snapshot_id,
      snapshot_dates.date_id                                                                                        AS snapshot_id,
      snapshot_dates.date_actual                                                                                    AS snapshot_date,
      snapshot_dates.first_day_of_month                                                                             AS snapshot_month,
      snapshot_dates.fiscal_year                                                                                    AS snapshot_fiscal_year,
      snapshot_dates.fiscal_quarter_name_fy                                                                         AS snapshot_fiscal_quarter_name,
      snapshot_dates.first_day_of_fiscal_quarter                                                                    AS snapshot_fiscal_quarter_date,
      snapshot_dates.day_of_fiscal_quarter_normalised                                                               AS snapshot_day_of_fiscal_quarter_normalised,
      snapshot_dates.day_of_fiscal_year_normalised                                                                  AS snapshot_day_of_fiscal_year_normalised,
      snapshot_dates.last_day_of_fiscal_quarter                                                                     AS snapshot_last_day_of_fiscal_quarter,
      sfdc_account_snapshot.parent_crm_account_geo,
      sfdc_account_snapshot.crm_account_owner_sales_segment,
      sfdc_account_snapshot.crm_account_owner_geo,
      sfdc_account_snapshot.crm_account_owner_region,
      sfdc_account_snapshot.crm_account_owner_area,
      sfdc_account_snapshot.crm_account_owner_sales_segment_geo_region_area,
      account_owner.dim_crm_user_hierarchy_sk                                                                       AS dim_crm_user_hierarchy_account_user_sk,
      account_owner.user_role_name                                                                                  AS crm_account_owner_role,
      account_owner.user_role_level_1                                                                               AS crm_account_owner_role_level_1,
      account_owner.user_role_level_2                                                                               AS crm_account_owner_role_level_2,
      account_owner.user_role_level_3                                                                               AS crm_account_owner_role_level_3,
      account_owner.user_role_level_4                                                                               AS crm_account_owner_role_level_4,
      account_owner.user_role_level_5                                                                               AS crm_account_owner_role_level_5,
      account_owner.title                                                                                           AS crm_account_owner_title,
      fulfillment_partner.crm_account_name AS fulfillment_partner_account_name,
      fulfillment_partner.partner_track AS fulfillment_partner_partner_track,
      partner_account.crm_account_name AS partner_account_account_name,
      partner_account.partner_track AS partner_account_partner_track,
      sfdc_account_snapshot.is_jihu_account,
      sfdc_account_snapshot.dim_parent_crm_account_id,
      CASE
        WHEN sfdc_opportunity_snapshots_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 
                                                              'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                                                                         AS is_open,
      CASE
        WHEN sfdc_opportunity_snapshots_source.user_segment_stamped IS NULL
          OR is_open = 1
          THEN sfdc_account_snapshot.crm_account_owner_sales_segment
        ELSE sfdc_opportunity_snapshots_source.user_segment_stamped
      END                                                                                                         AS opportunity_owner_user_segment,
      sfdc_user_snapshot.user_role_name                                                                           AS opportunity_owner_role,
      sfdc_user_snapshot.user_role_level_1                                                                        AS crm_opp_owner_role_level_1,
      sfdc_user_snapshot.user_role_level_2                                                                        AS crm_opp_owner_role_level_2,
      sfdc_user_snapshot.user_role_level_3                                                                        AS crm_opp_owner_role_level_3,
      sfdc_user_snapshot.user_role_level_4                                                                        AS crm_opp_owner_role_level_4,
      sfdc_user_snapshot.user_role_level_5                                                                        AS crm_opp_owner_role_level_5,
      sfdc_user_snapshot.title                                                                                    AS opportunity_owner_title,
      sfdc_account_snapshot.crm_account_owner_role                                                                AS opportunity_account_owner_role,
      {{ dbt_utils.star(from=ref('sfdc_opportunity_snapshots_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "PARENT_OPPORTUNITY_ID", "ORDER_TYPE_STAMPED",
                                                                               "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM", "SALES_QUALIFIED_SOURCE", 
                                                                               "DBT_UPDATED_AT", "CREATED_DATE", "SALES_ACCEPTED_DATE", "CLOSE_DATE", 
                                                                               "NET_ARR", "DEAL_SIZE"],relation_alias="sfdc_opportunity_snapshots_source")}},
      0 AS is_live
    FROM sfdc_opportunity_snapshots_source
    INNER JOIN snapshot_dates
      ON sfdc_opportunity_snapshots_source.dbt_valid_from::DATE <= snapshot_dates.date_actual
        AND (sfdc_opportunity_snapshots_source.dbt_valid_to::DATE > snapshot_dates.date_actual OR sfdc_opportunity_snapshots_source.dbt_valid_to IS NULL)
    LEFT JOIN sfdc_account_snapshot AS fulfillment_partner
      ON sfdc_opportunity_snapshots_source.fulfillment_partner = fulfillment_partner.dim_crm_account_id
        AND snapshot_dates.date_id = fulfillment_partner.snapshot_id
    LEFT JOIN sfdc_account_snapshot AS partner_account
      ON sfdc_opportunity_snapshots_source.partner_account = partner_account.dim_crm_account_id
        AND snapshot_dates.date_id = partner_account.snapshot_id
    LEFT JOIN sfdc_account_snapshot
      ON sfdc_opportunity_snapshots_source.account_id = sfdc_account_snapshot.dim_crm_account_id
        AND snapshot_dates.date_id = sfdc_account_snapshot.snapshot_id
    LEFT JOIN sfdc_user_snapshot
      ON sfdc_opportunity_snapshots_source.owner_id = sfdc_user_snapshot.dim_crm_user_id
        AND snapshot_dates.date_id = sfdc_user_snapshot.snapshot_id
    LEFT JOIN sfdc_user_snapshot AS account_owner
      ON sfdc_account_snapshot.dim_crm_user_id = account_owner.dim_crm_user_id
        AND snapshot_dates.date_id = account_owner.snapshot_id
    WHERE sfdc_opportunity_snapshots_source.account_id IS NOT NULL
      AND sfdc_opportunity_snapshots_source.is_deleted = FALSE

), sfdc_opportunity_live AS (

    SELECT
      sfdc_opportunity_source.account_id                                                                    AS dim_crm_account_id,
      sfdc_opportunity_source.opportunity_id                                                                AS dim_crm_opportunity_id,
      sfdc_opportunity_source.owner_id                                                                      AS dim_crm_user_id,
      sfdc_account.dim_crm_user_id                                                                          AS dim_crm_account_user_id,
      sfdc_opportunity_source.parent_opportunity_id                                                         AS dim_parent_crm_opportunity_id,
      sfdc_opportunity_source.order_type_stamped                                                            AS order_type,
      sfdc_opportunity_source.opportunity_term                                                              AS opportunity_term_base,
      {{ sales_qualified_source_cleaning('sfdc_opportunity_source.sales_qualified_source') }}               AS sales_qualified_source,
      sfdc_opportunity_source.user_segment_stamped                                                          AS crm_opp_owner_sales_segment_stamped,
      sfdc_opportunity_source.user_geo_stamped                                                              AS crm_opp_owner_geo_stamped,
      sfdc_opportunity_source.user_region_stamped                                                           AS crm_opp_owner_region_stamped,
      sfdc_opportunity_source.user_area_stamped                                                             AS crm_opp_owner_area_stamped,
      sfdc_opportunity_source.user_segment_geo_region_area_stamped                                          AS crm_opp_owner_sales_segment_geo_region_area_stamped,
      sfdc_opportunity_source.user_business_unit_stamped                                                    AS crm_opp_owner_business_unit_stamped,
      sfdc_opportunity_source.created_date::DATE                                                            AS created_date,
      sfdc_opportunity_source.sales_accepted_date::DATE                                                     AS sales_accepted_date,
      sfdc_opportunity_source.close_date::DATE                                                              AS close_date,
      sfdc_opportunity_source.net_arr                                                                       AS raw_net_arr,
      {{ dbt_utils.generate_surrogate_key(['sfdc_opportunity_source.opportunity_id',"'99991231'"])}}        AS crm_opportunity_snapshot_id,
      '99991231'                                                                                            AS snapshot_id,
      live_date.date_actual                                                                                 AS snapshot_date,
      live_date.first_day_of_month                                                                          AS snapshot_month,
      live_date.fiscal_year                                                                                 AS snapshot_fiscal_year,
      live_date.fiscal_quarter_name_fy                                                                      AS snapshot_fiscal_quarter_name,
      live_date.first_day_of_fiscal_quarter                                                                 AS snapshot_fiscal_quarter_date,
      live_date.day_of_fiscal_quarter_normalised                                                            AS snapshot_day_of_fiscal_quarter_normalised,
      live_date.day_of_fiscal_year_normalised                                                               AS snapshot_day_of_fiscal_year_normalised,
      live_date.last_day_of_fiscal_quarter                                                                  AS snapshot_last_day_of_fiscal_quarter,
      sfdc_account.parent_crm_account_geo                                                                   AS parent_crm_account_geo,
      account_owner.dim_crm_user_hierarchy_sk                                                               AS dim_crm_user_hierarchy_account_user_sk,
      account_owner.crm_user_sales_segment                                                                  AS crm_account_owner_sales_segment,
      account_owner.crm_user_geo                                                                            AS crm_account_owner_geo,
      account_owner.crm_user_region                                                                         AS crm_account_owner_region,
      account_owner.crm_user_area                                                                           AS crm_account_owner_area,
      account_owner.crm_user_sales_segment_geo_region_area                                                  AS crm_account_owner_sales_segment_geo_region_area,
      account_owner.user_role_name                                                                          AS crm_account_owner_role,
      account_owner.user_role_level_1                                                                       AS crm_account_owner_role_level_1,
      account_owner.user_role_level_2                                                                       AS crm_account_owner_role_level_2,
      account_owner.user_role_level_3                                                                       AS crm_account_owner_role_level_3,
      account_owner.user_role_level_4                                                                       AS crm_account_owner_role_level_4,
      account_owner.user_role_level_5                                                                       AS crm_account_owner_role_level_5,
      account_owner.title                                                                                   AS crm_account_owner_title,
      fulfillment_partner.crm_account_name                                                                  AS fulfillment_partner_account_name,
      fulfillment_partner.partner_track                                                                     AS fulfillment_partner_partner_track,
      partner_account.crm_account_name                                                                      AS partner_account_account_name,
      partner_account.partner_track                                                                         AS partner_account_partner_track,
      sfdc_account.is_jihu_account,
      sfdc_account.dim_parent_crm_account_id                                                                AS dim_parent_crm_account_id,
      CASE
        WHEN sfdc_opportunity_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 
                                                    'Closed Won', '10-Duplicate')
            THEN 0
        ELSE 1
      END                                                                                                   AS is_open,
      CASE
        WHEN sfdc_opportunity_source.user_segment_stamped IS NULL
          OR is_open = 1
          THEN account_owner.crm_user_sales_segment
        ELSE sfdc_opportunity_source.user_segment_stamped
      END                                                                                                   AS opportunity_owner_user_segment,
      opportunity_owner.user_role_name                                                                      AS opportunity_owner_role,
      opportunity_owner.user_role_level_1                                                                   AS crm_opp_owner_role_level_1,
      opportunity_owner.user_role_level_2                                                                   AS crm_opp_owner_role_level_2,
      opportunity_owner.user_role_level_3                                                                   AS crm_opp_owner_role_level_3,
      opportunity_owner.user_role_level_4                                                                   AS crm_opp_owner_role_level_4,
      opportunity_owner.user_role_level_5                                                                   AS crm_opp_owner_role_level_5,
      opportunity_owner.title                                                                               AS opportunity_owner_title,
      sfdc_account.crm_account_owner_role                                                                   AS opportunity_account_owner_role,
      {{ dbt_utils.star(from=ref('sfdc_opportunity_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "PARENT_OPPORTUNITY_ID", "ORDER_TYPE_STAMPED", "IS_WON", 
                                                                     "ORDER_TYPE", "OPPORTUNITY_TERM","SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT", 
                                                                     "CREATED_DATE", "SALES_ACCEPTED_DATE", "CLOSE_DATE", "NET_ARR", "DEAL_SIZE"],relation_alias="sfdc_opportunity_source")}},
      NULL                                                                                                  AS dbt_scd_id,
      CURRENT_DATE()                                                                                        AS dbt_valid_from,
      CURRENT_DATE()                                                                                        AS dbt_valid_to,
      1                                                                                                     AS is_live
    FROM sfdc_opportunity_source
    LEFT JOIN live_date
      ON CURRENT_DATE() = live_date.date_actual
    LEFT JOIN sfdc_account AS fulfillment_partner
      ON sfdc_opportunity_source.fulfillment_partner = fulfillment_partner.dim_crm_account_id
    LEFT JOIN sfdc_account AS partner_account
      ON sfdc_opportunity_source.partner_account = partner_account.dim_crm_account_id
    LEFT JOIN sfdc_account
      ON sfdc_opportunity_source.account_id= sfdc_account.dim_crm_account_id
    LEFT JOIN sfdc_user AS account_owner
      ON sfdc_account.dim_crm_user_id = account_owner.dim_crm_user_id
    LEFT JOIN sfdc_user AS opportunity_owner
      ON sfdc_opportunity_source.owner_id = opportunity_owner.dim_crm_user_id
    WHERE sfdc_opportunity_source.account_id IS NOT NULL
      AND sfdc_opportunity_source.is_deleted = FALSE

), sfdc_opportunity AS (

    SELECT *
    FROM sfdc_opportunity_snapshot

    UNION ALL 

    SELECT * 
    FROM sfdc_opportunity_live

), sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = FALSE

), quote AS (

    SELECT DISTINCT
      sfdc_zqu_quote_source.zqu__opportunity                AS dim_crm_opportunity_id,
      sfdc_zqu_quote_source.zqu_quote_id                    AS dim_quote_id,
      sfdc_zqu_quote_source.zqu__start_date::DATE           AS quote_start_date,
      (ROW_NUMBER() OVER (PARTITION BY sfdc_zqu_quote_source.zqu__opportunity ORDER BY sfdc_zqu_quote_source.created_date DESC))
                                                            AS record_number
    FROM sfdc_zqu_quote_source
    INNER JOIN sfdc_opportunity
      ON sfdc_zqu_quote_source.zqu__opportunity = sfdc_opportunity.dim_crm_opportunity_id
    WHERE stage_name IN ('Closed Won', '8-Closed Lost')
      AND zqu__primary = TRUE
    QUALIFY record_number = 1

), sao_base AS (
  
  SELECT
   --IDs
    sfdc_opportunity.dim_crm_opportunity_id,
  
  --Opp Data  

    sfdc_opportunity.sales_accepted_date,
    CASE 
      WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name != '10-Duplicate'
          AND sales_accepted_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_sao  
  FROM sfdc_opportunity
  LEFT JOIN account_history_final
    ON sfdc_opportunity.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND sales_accepted_date IS NOT NULL
  AND sales_accepted_date >= '2022-02-01'
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  AND is_abm_tier_sao = TRUE

), cw_base AS (
  
  SELECT
   --IDs
    sfdc_opportunity.dim_crm_opportunity_id,
  
  --Opp Data  
    sfdc_opportunity.close_date,
    CASE 
      WHEN stage_name = 'Closed Won'
        AND close_date BETWEEN valid_from AND valid_to
        THEN TRUE
      ELSE FALSE
    END AS is_abm_tier_closed_won 
  FROM sfdc_opportunity
  LEFT JOIN account_history_final
    ON sfdc_opportunity.dim_crm_account_id=account_history_final.dim_crm_account_id
  WHERE abm_tier IS NOT NULL
  AND close_date IS NOT NULL
  AND close_date >= '2022-02-01'
  AND (abm_tier_1_date IS NOT NULL
    OR abm_tier_2_date IS NOT NULL)
  AND is_abm_tier_closed_won = TRUE
  
), abm_tier_id AS (

    SELECT
        dim_crm_opportunity_id
    FROM sao_base
    UNION
    SELECT
        dim_crm_opportunity_id
    FROM cw_base

), abm_tier_unioned AS (
  
SELECT
  abm_tier_id.dim_crm_opportunity_id,
  is_abm_tier_sao,
  is_abm_tier_closed_won
FROM abm_tier_id
LEFT JOIN sao_base
  ON abm_tier_id.dim_crm_opportunity_id=sao_base.dim_crm_opportunity_id  
LEFT JOIN cw_base
  ON abm_tier_id.dim_crm_opportunity_id=cw_base.dim_crm_opportunity_id    

), final AS (

    SELECT DISTINCT
      -- opportunity information
      sfdc_opportunity.*,
      sfdc_opportunity.crm_opportunity_snapshot_id||'-'||sfdc_opportunity.is_live                  AS primary_key,

      -- dates & date ids
      {{ get_date_id('sfdc_opportunity.created_date') }}                                          AS created_date_id,
      {{ get_date_id('sfdc_opportunity.sales_accepted_date') }}                                   AS sales_accepted_date_id,
      {{ get_date_id('sfdc_opportunity.close_date') }}                                            AS close_date_id,
      {{ get_date_id('sfdc_opportunity.stage_0_pending_acceptance_date') }}                       AS stage_0_pending_acceptance_date_id,
      {{ get_date_id('sfdc_opportunity.stage_1_discovery_date') }}                                AS stage_1_discovery_date_id,
      {{ get_date_id('sfdc_opportunity.stage_2_scoping_date') }}                                  AS stage_2_scoping_date_id,
      {{ get_date_id('sfdc_opportunity.stage_3_technical_evaluation_date') }}                     AS stage_3_technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.stage_4_proposal_date') }}                                 AS stage_4_proposal_date_id,
      {{ get_date_id('sfdc_opportunity.stage_5_negotiating_date') }}                              AS stage_5_negotiating_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_awaiting_signature_date') }}                       AS stage_6_awaiting_signature_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_won_date') }}                               AS stage_6_closed_won_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_lost_date') }}                              AS stage_6_closed_lost_date_id,
      {{ get_date_id('sfdc_opportunity.technical_evaluation_date') }}                             AS technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.last_activity_date') }}                                    AS last_activity_date_id,
      {{ get_date_id('sfdc_opportunity.sales_last_activity_date') }}                              AS sales_last_activity_date_id,
      {{ get_date_id('sfdc_opportunity.subscription_start_date') }}                               AS subscription_start_date_id,
      {{ get_date_id('sfdc_opportunity.subscription_end_date') }}                                 AS subscription_end_date_id,
      {{ get_date_id('sfdc_opportunity.sales_qualified_date') }}                                  AS sales_qualified_date_id,

      sfdc_opportunity_live.close_date                                                            AS close_date_live,
      close_date.first_day_of_fiscal_quarter                                                      AS close_fiscal_quarter_date,
      90 - DATEDIFF(DAY, sfdc_opportunity.snapshot_date, close_date.last_day_of_fiscal_quarter)   AS close_day_of_fiscal_quarter_normalised,
      -- The fiscal year has to be created from scratch instead of joining to the date model because of sales practices which put close dates out 100+ years in the future
      CASE 
        WHEN DATE_PART('month', sfdc_opportunity.close_date) < 2
          THEN DATE_PART('year', sfdc_opportunity.close_date)
        ELSE (DATE_PART('year', sfdc_opportunity.close_date)+1) 
      END                                                                                         AS close_fiscal_year,

      CASE 
        WHEN DATE_PART('month', sfdc_opportunity_live.close_date) < 2
          THEN DATE_PART('year', sfdc_opportunity_live.close_date)
        ELSE (DATE_PART('year', sfdc_opportunity_live.close_date)+1) 
      END                                                                                         AS close_fiscal_year_live,

      {{ get_date_id('sfdc_opportunity.iacv_created_date')}}                                      AS arr_created_date_id,
      sfdc_opportunity.iacv_created_date                                                          AS arr_created_date,
      arr_created_date.fiscal_quarter_name_fy                                                     AS arr_created_fiscal_quarter_name,
      arr_created_date.first_day_of_fiscal_quarter                                                AS arr_created_fiscal_quarter_date,
      created_date.fiscal_quarter_name_fy                                                         AS created_fiscal_quarter_name,
      created_date.first_day_of_fiscal_quarter                                                    AS created_fiscal_quarter_date,
      subscription_start_date.fiscal_quarter_name_fy                                              AS subscription_start_date_fiscal_quarter_name,
      subscription_start_date.first_day_of_fiscal_quarter                                         AS subscription_start_date_fiscal_quarter_date,

      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)                             AS segment_order_type_iacv_to_net_arr_ratio,


      -- live fields

      sfdc_opportunity_live.sales_qualified_source                                                AS sales_qualified_source_live,
      sfdc_opportunity_live.sales_qualified_source_grouped                                        AS sales_qualified_source_grouped_live,
      sfdc_opportunity_live.is_edu_oss                                                            AS is_edu_oss_live,
      sfdc_opportunity_live.opportunity_category                                                  AS opportunity_category_live,
      sfdc_opportunity_live.is_jihu_account                                                       AS is_jihu_account_live,
      sfdc_opportunity_live.deal_path                                                             AS deal_path_live,
      sfdc_opportunity_live.parent_crm_account_geo                                                AS parent_crm_account_geo_live,
      sfdc_opportunity_live.order_type_grouped                                                    AS order_type_grouped_live,
      sfdc_opportunity_live.order_type                                                            AS order_type_live,


      -- net arr
      CASE
        WHEN sfdc_opportunity_stage.is_won = 1 -- only consider won deals
          AND sfdc_opportunity_live.opportunity_category <> 'Contract Reset' -- contract resets have a special way of calculating net iacv
          AND COALESCE(sfdc_opportunity.raw_net_arr,0) <> 0
          AND COALESCE(sfdc_opportunity.net_incremental_acv,0) <> 0
            THEN COALESCE(sfdc_opportunity.raw_net_arr / sfdc_opportunity.net_incremental_acv,0)
        ELSE NULL
      END                                                                     AS opportunity_based_iacv_to_net_arr_ratio,
      -- If there is no opportunity, use a default table ratio
      -- I am faking that using the upper CTE, that should be replaced by the official table
      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- if there is an opportunity based ratio, use that, if not, use default from segment / order type
      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE
        WHEN sfdc_opportunity.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(sfdc_opportunity.net_incremental_acv,0) = 0
            THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(sfdc_opportunity.net_incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,
      -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
      -- Those were later fixed in the opportunity object but stayed in the snapshot table.
      -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
      CASE
        WHEN  sfdc_opportunity.snapshot_date::DATE < '2021-02-01' -- All deals before cutoff and that were not updated to Net ARR
          THEN calculated_from_ratio_net_arr
        WHEN  sfdc_opportunity.snapshot_date::DATE >= '2021-02-01'  -- After cutoff date, for all deals earlier than FY19 that are closed and have no net arr
              AND sfdc_opportunity.close_date::DATE < '2018-02-01'
              AND sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
              AND COALESCE(sfdc_opportunity.raw_net_arr,0) = 0
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(sfdc_opportunity.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,

      -- opportunity flags
      is_abm_tier_sao,
      is_abm_tier_closed_won,
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
          THEN TRUE
          ELSE FALSE
      END                                                                                         AS is_risky,
      CASE
        WHEN sfdc_opportunity.opportunity_term_base IS NULL THEN
          DATEDIFF('month', quote.quote_start_date, sfdc_opportunity.subscription_end_date)
        ELSE sfdc_opportunity.opportunity_term_base
      END                                                                                         AS opportunity_term,
      -- opportunity stage information
      sfdc_opportunity_stage.is_active                                                            AS is_active,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      IFF(sfdc_opportunity.stage_name IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing'), 1, 0) AS is_stage_1_plus,
      IFF(sfdc_opportunity.stage_name IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing'), 1, 0) AS is_stage_3_plus,
      IFF(sfdc_opportunity.stage_name IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing'), 1, 0) AS is_stage_4_plus,
      IFF(sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost'), 1, 0) AS is_lost,
      IFF(LOWER(sfdc_opportunity.sales_type) like '%renewal%', 1, 0) AS is_renewal,
      IFF(sfdc_opportunity_live.opportunity_category IN ('Decommission'), 1, 0) AS is_decommissed,

     -- flags
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity_live.is_edu_oss = 0
          AND sfdc_opportunity.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sao,
      CASE
        WHEN is_sao = TRUE
          AND sfdc_opportunity_live.sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sdr_sao,
      CASE
        WHEN (
               (sfdc_opportunity.sales_type = 'Renewal' AND sfdc_opportunity.stage_name = '8-Closed Lost')
                 OR sfdc_opportunity.stage_name = 'Closed Won'
              )
            AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
          THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_net_arr_closed_deal,
      CASE
        WHEN (sfdc_opportunity.new_logo_count = 1
          OR sfdc_opportunity.new_logo_count = -1
          )
          AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
          THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_new_logo_first_order,
      -- align is_booked_net_arr with fpa_master_bookings_flag definition from salesforce: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
      -- coalesce both flags so we don't have NULL values for records before the fpa_master_bookings_flag was created
      COALESCE(
        sfdc_opportunity.fpa_master_bookings_flag, 
        CASE
          WHEN (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL) 
            AND (sfdc_opportunity_stage.is_won = 1
                  OR (
                      is_renewal = 1
                      AND is_lost = 1)
                    )
              THEN 1
            ELSE 0
        END)                                            AS is_booked_net_arr, 
      /* 
        Stop coalescing is_pipeline_created_eligible and is_net_arr_pipeline_created 
      Definition changed for is_pipeline_created_eligible and if we coalesce both, the values will be inaccurate for 
      snapshots before the definition changed in SFDC: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/5331
      Use is_net_arr_pipeline_created as the SSOT 
      */
        CASE
          WHEN sfdc_opportunity_live.order_type IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
            AND sfdc_opportunity_live.is_edu_oss  = 0
            AND arr_created_date.first_day_of_fiscal_quarter IS NOT NULL
            AND sfdc_opportunity_live.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset','Contract Reset/Ramp Deal')
            AND sfdc_opportunity.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
            AND (net_arr > 0
              OR sfdc_opportunity_live.opportunity_category = 'Credit')
            AND sfdc_opportunity_live.sales_qualified_source != 'Web Direct Generated'
            AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
            AND sfdc_opportunity.stage_1_discovery_date IS NOT NULL
          THEN 1
          ELSE 0
        END                                                                                      AS is_net_arr_pipeline_created,
      CASE
        WHEN sfdc_opportunity.close_date <= CURRENT_DATE()
         AND sfdc_opportunity.is_closed = 'TRUE'
         AND sfdc_opportunity_live.is_edu_oss = 0
         AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
         AND (sfdc_opportunity.reason_for_loss IS NULL OR sfdc_opportunity.reason_for_loss != 'Merged into another opportunity')
         AND sfdc_opportunity_live.sales_qualified_source != 'Web Direct Generated'
         AND sfdc_opportunity_live.parent_crm_account_geo != 'JIHU'
         AND sfdc_opportunity.stage_name NOT IN ('10-Duplicate', '9-Unqualified')
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_win_rate_calc,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 'TRUE'
          AND sfdc_opportunity.is_closed = 'TRUE'
          AND sfdc_opportunity_live.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_closed_won,
      CASE
        WHEN LOWER(sfdc_opportunity_live.order_type_grouped) LIKE ANY ('%growth%', '%new%')
          AND sfdc_opportunity_live.is_edu_oss = 0
          AND is_stage_1_plus = 1
          AND sfdc_opportunity.forecast_category_name != 'Omitted'
          AND sfdc_opportunity.is_open = 1
          AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
         THEN 1
         ELSE 0
      END                                                                                         AS is_eligible_open_pipeline,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity_live.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
            THEN 1
        ELSE 0
      END                                                                                         AS is_eligible_sao,
      CASE
        WHEN sfdc_opportunity_live.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          -- For ASP we care mainly about add on, new business, excluding contraction / churn
          AND sfdc_opportunity_live.order_type IN ('1. New - First Order','2. New - Connected','3. Growth')
          -- Exclude Decomissioned as they are not aligned to the real owner
          -- Contract Reset, Decomission
          AND sfdc_opportunity_live.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          -- Exclude Deals with nARR < 0
          AND net_arr > 0
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_asp_analysis,
      CASE
        WHEN sfdc_opportunity.close_date <= CURRENT_DATE()
         AND is_booked_net_arr = TRUE
         AND sfdc_opportunity_live.is_edu_oss = 0
         AND (sfdc_opportunity_live.is_jihu_account != TRUE OR sfdc_opportunity_live.is_jihu_account IS NULL)
         AND (sfdc_opportunity.reason_for_loss IS NULL OR sfdc_opportunity.reason_for_loss != 'Merged into another opportunity')
         AND sfdc_opportunity_live.sales_qualified_source != 'Web Direct Generated'
         AND sfdc_opportunity_live.deal_path != 'Web Direct'
         AND sfdc_opportunity_live.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
         AND sfdc_opportunity_live.parent_crm_account_geo != 'JIHU'
         AND (sfdc_opportunity_live.opportunity_category IN ('Standard') OR (
            /* Include only first year ramp deals. The ssp_id should be either equal to the SFDC id (18)
            or equal to the SFDC id (15) for first year ramp deals */
            sfdc_opportunity_live.opportunity_category = 'Ramp Deal' AND 
            LEFT(sfdc_opportunity.dim_crm_opportunity_id, LENGTH(sfdc_opportunity.ssp_id)) = sfdc_opportunity.ssp_id))
            THEN 1
        ELSE 0
      END                                                                                         AS is_eligible_age_analysis,
      CASE
        WHEN sfdc_opportunity_live.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND (sfdc_opportunity_stage.is_won = 1
              OR (is_renewal = 1 AND is_lost = 1))
          AND sfdc_opportunity_live.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_net_arr,
      CASE
        WHEN sfdc_opportunity_live.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND sfdc_opportunity_live.order_type IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_churn_contraction,
      CASE
        WHEN sfdc_opportunity.stage_name IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                                                         AS is_duplicate,
      CASE
        WHEN sfdc_opportunity_live.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                                                         AS is_credit,
      CASE
        WHEN sfdc_opportunity_live.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                                                         AS is_contract_reset,

      -- alliance type fields

      {{ alliance_partner_current('sfdc_opportunity.fulfillment_partner_account_name', 'sfdc_opportunity.partner_account_account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path', 'sfdc_opportunity.is_focus_partner') }} AS alliance_type_current,
      {{ alliance_partner_short_current('sfdc_opportunity.fulfillment_partner_account_name', 'sfdc_opportunity.partner_account_account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path', 'sfdc_opportunity.is_focus_partner') }} AS alliance_type_short_current,

      {{ alliance_partner('sfdc_opportunity.fulfillment_partner_account_name', 'sfdc_opportunity.partner_account_account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path', 'sfdc_opportunity.is_focus_partner') }} AS alliance_type,
      {{ alliance_partner_short('sfdc_opportunity.fulfillment_partner_account_name', 'sfdc_opportunity.partner_account_account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path', 'sfdc_opportunity.is_focus_partner') }} AS alliance_type_short,

      sfdc_opportunity.fulfillment_partner_account_name AS resale_partner_name,

      --  quote information
      quote.dim_quote_id,
      quote.quote_start_date,

      -- contact information
      first_contact.dim_crm_person_id,
      first_contact.sfdc_contact_id,

      -- Record type information
      sfdc_record_type_source.record_type_name,

      -- attribution information
      linear_attribution_base.count_crm_attribution_touchpoints,
      campaigns_per_opp.count_campaigns,
      sfdc_opportunity.incremental_acv/linear_attribution_base.count_crm_attribution_touchpoints   AS weighted_linear_iacv,

     -- opportunity attributes
      CASE
        WHEN sfdc_opportunity.days_in_sao < 0                  THEN '1. Closed in < 0 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 0 AND 30     THEN '2. Closed in 0-30 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 31 AND 60    THEN '3. Closed in 31-60 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 61 AND 90    THEN '4. Closed in 61-90 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 91 AND 180   THEN '5. Closed in 91-180 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 181 AND 270  THEN '6. Closed in 181-270 days'
        WHEN sfdc_opportunity.days_in_sao > 270                THEN '7. Closed in > 270 days'
        ELSE NULL
      END                                                                                         AS closed_buckets,
      CASE
        WHEN net_arr > -5000
            AND is_eligible_churn_contraction = 1
          THEN '1. < 5k'
        WHEN net_arr > -20000
          AND net_arr <= -5000
          AND is_eligible_churn_contraction = 1
          THEN '2. 5k-20k'
        WHEN net_arr > -50000
          AND net_arr <= -20000
          AND is_eligible_churn_contraction = 1
          THEN '3. 20k-50k'
        WHEN net_arr > -100000
          AND net_arr <= -50000
          AND is_eligible_churn_contraction = 1
          THEN '4. 50k-100k'
        WHEN net_arr < -100000
          AND is_eligible_churn_contraction = 1
          THEN '5. 100k+'
      END                                                 AS churn_contraction_net_arr_bucket,
      CASE
        WHEN sfdc_opportunity.created_date < '2022-02-01'
          THEN 'Legacy'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL AND sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'SDR & BDR'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL
          THEN 'SDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'BDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NULL AND sfdc_opportunity.opportunity_sales_development_representative IS NULL
          THEN 'No XDR Assigned'
      END                                               AS sdr_or_bdr,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 1
          THEN '1.Won'
        WHEN is_lost = 1
          THEN '2.Lost'
        WHEN sfdc_opportunity.is_open = 1
          THEN '0. Open'
        ELSE 'N/A'
      END                                                                                         AS stage_category,
      CASE
       WHEN sfdc_opportunity_live.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity_live.order_type IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction')
         THEN '2. Growth'
       ELSE '3. Other'
     END                                                                   AS deal_group,
     CASE
       WHEN sfdc_opportunity_live.order_type = '1. New - First Order'
         THEN '1. New'
       WHEN sfdc_opportunity_live.order_type IN ('2. New - Connected', '3. Growth')
         THEN '2. Growth'
       WHEN sfdc_opportunity_live.order_type IN ('4. Contraction')
         THEN '3. Contraction'
       WHEN sfdc_opportunity_live.order_type IN ('5. Churn - Partial','6. Churn - Final')
         THEN '4. Churn'
       ELSE '5. Other'
      END                                                                                       AS deal_category,
      COALESCE(sfdc_opportunity.reason_for_loss, sfdc_opportunity.downgrade_reason)               AS reason_for_loss_staged,
      CASE
        WHEN reason_for_loss_staged IN ('Do Nothing','Other','Competitive Loss','Operational Silos')
          OR reason_for_loss_staged IS NULL
          THEN 'Unknown'
        WHEN reason_for_loss_staged IN ('Missing Feature','Product value/gaps','Product Value / Gaps',
                                          'Stayed with Community Edition','Budget/Value Unperceived')
          THEN 'Product Value / Gaps'
        WHEN reason_for_loss_staged IN ('Lack of Engagement / Sponsor','Went Silent','Evangelist Left')
          THEN 'Lack of Engagement / Sponsor'
        WHEN reason_for_loss_staged IN ('Loss of Budget','No budget')
          THEN 'Loss of Budget'
        WHEN reason_for_loss_staged = 'Merged into another opportunity'
          THEN 'Merged Opp'
        WHEN reason_for_loss_staged = 'Stale Opportunity'
          THEN 'No Progression - Auto-close'
        WHEN reason_for_loss_staged IN ('Product Quality / Availability','Product quality/availability')
          THEN 'Product Quality / Availability'
        ELSE reason_for_loss_staged
     END                                                                                        AS reason_for_loss_calc,
     CASE
       WHEN (
              (
                is_renewal = 1
                  AND is_lost = 1
               )
                OR sfdc_opportunity_stage.is_won = 1
              )
               AND sfdc_opportunity_live.order_type IN ('4. Contraction','5. Churn - Partial')
          THEN 'Contraction'
               WHEN (
              (
                is_renewal = 1
                  AND is_lost = 1
               )
                OR sfdc_opportunity_stage.is_won = 1
              )
               AND sfdc_opportunity_live.order_type = '6. Churn - Final'
          THEN 'Churn'
        ELSE NULL
     END                                                                                        AS churn_contraction_type,
     CASE
        WHEN is_renewal = 1
          AND subscription_start_date_fiscal_quarter_date >= close_fiscal_quarter_date
         THEN 'On-Time'
        WHEN is_renewal = 1
          AND subscription_start_date_fiscal_quarter_date < close_fiscal_quarter_date
            THEN 'Late'
      END                                                                                       AS renewal_timing_status,
      CASE
        WHEN net_arr > -5000
          THEN '1. < 5k'
        WHEN net_arr > -20000 AND net_arr <= -5000
          THEN '2. 5k-20k'
        WHEN net_arr > -50000 AND net_arr <= -20000
          THEN '3. 20k-50k'
        WHEN net_arr > -100000 AND net_arr <= -50000
          THEN '4. 50k-100k'
        WHEN net_arr < -100000
          THEN '5. 100k+'
      END                                                                                       AS churned_contraction_net_arr_bucket,
      CASE
        WHEN sfdc_opportunity_live.deal_path = 'Direct'
          THEN 'Direct'
        WHEN sfdc_opportunity_live.deal_path = 'Web Direct'
          THEN 'Web Direct'
        WHEN sfdc_opportunity_live.deal_path = 'Partner'
            AND sfdc_opportunity_live.sales_qualified_source = 'Partner Generated'
          THEN 'Partner Sourced'
        WHEN sfdc_opportunity_live.deal_path = 'Partner'
            AND sfdc_opportunity_live.sales_qualified_source != 'Partner Generated'
          THEN 'Partner Co-Sell'
      END                                                                                       AS deal_path_engagement,
      CASE
        WHEN net_arr > 0 AND net_arr < 5000
          THEN '1 - Small (<5k)'
        WHEN net_arr >=5000 AND net_arr < 25000
          THEN '2 - Medium (5k - 25k)'
        WHEN net_arr >=25000 AND net_arr < 100000
          THEN '3 - Big (25k - 100k)'
        WHEN net_arr >= 100000
          THEN '4 - Jumbo (>100k)'
        ELSE 'Other'
      END                                                          AS deal_size,
      CASE
        WHEN net_arr > 0 AND net_arr < 1000
          THEN '1. (0k -1k)'
        WHEN net_arr >=1000 AND net_arr < 10000
          THEN '2. (1k - 10k)'
        WHEN net_arr >=10000 AND net_arr < 50000
          THEN '3. (10k - 50k)'
        WHEN net_arr >=50000 AND net_arr < 100000
          THEN '4. (50k - 100k)'
        WHEN net_arr >= 100000 AND net_arr < 250000
          THEN '5. (100k - 250k)'
        WHEN net_arr >= 250000 AND net_arr < 500000
          THEN '6. (250k - 500k)'
        WHEN net_arr >= 500000 AND net_arr < 1000000
          THEN '7. (500k-1000k)'
        WHEN net_arr >= 1000000
          THEN '8. (>1000k)'
        ELSE 'Other'
      END                                                                                         AS calculated_deal_size,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '3-Technical Evaluation',
            '4-Proposal',
            '5-Negotiating',
            '6-Awaiting Signature',
            '7-Closing'
          )
          THEN '3+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_3plus,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping',
            '3-Technical Evaluation'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing'
          )
          THEN '4+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_4plus,

      -- counts and arr totals by pipeline stage
       CASE
        WHEN is_decommissed = 1
          THEN -1
        WHEN is_credit = 1
          THEN 0
        ELSE 1
      END                                               AS calculated_deal_count,
      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_1_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_1plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,
      CASE
        WHEN is_booked_net_arr = 1 
          THEN calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,
      CASE
        WHEN is_eligible_churn_contraction = 1
          THEN calculated_deal_count
        ELSE 0
      END                                               AS churned_contraction_deal_count,
      CASE
        WHEN (
              (
                is_renewal = 1
                  AND is_lost = 1
               )
                OR sfdc_opportunity_stage.is_won = 1
              )
              AND is_eligible_churn_contraction = 1
          THEN calculated_deal_count
        ELSE 0
      END                                                 AS booked_churned_contraction_deal_count,
      CASE
        WHEN
          (
            (
              is_renewal = 1
                AND is_lost = 1
              )
            OR sfdc_opportunity_stage.is_won = 1
            )
            AND is_eligible_churn_contraction = 1
          THEN net_arr
        ELSE 0
      END                                                 AS booked_churned_contraction_net_arr,

      CASE
        WHEN is_eligible_churn_contraction = 1
          THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,
      CASE
        WHEN is_eligible_open_pipeline = 1
          THEN net_arr
        ELSE 0
      END                                                AS open_1plus_net_arr,
      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,
      CASE
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,
      CASE
        WHEN is_booked_net_arr = 1 
          THEN net_arr
        ELSE 0
      END                                                 AS booked_net_arr,
      CASE
        WHEN sfdc_opportunity_live.deal_path = 'Partner'
          THEN REPLACE(COALESCE(sfdc_opportunity.partner_track, sfdc_opportunity.partner_account_partner_track, sfdc_opportunity.fulfillment_partner_partner_track,'Open'),'select','Select')
        ELSE 'Direct'
      END                                                                                           AS calculated_partner_track,
      CASE
        WHEN
        sfdc_opportunity.dim_parent_crm_account_id IN (
          '001610000111bA3',
          '0016100001F4xla',
          '0016100001CXGCs',
          '00161000015O9Yn',
          '0016100001b9Jsc'
        )
        AND sfdc_opportunity.close_date < '2020-08-01'
        THEN 1
      -- NF 2021 - Pubsec extreme deals
      WHEN
        sfdc_opportunity.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
        AND (sfdc_opportunity.snapshot_date < '2021-05-01' OR sfdc_opportunity.is_live = 1)
        THEN 1
      -- exclude vision opps from FY21-Q2
      WHEN arr_created_fiscal_quarter_name = 'FY21-Q2'
        AND sfdc_opportunity.snapshot_day_of_fiscal_quarter_normalised = 90
        AND sfdc_opportunity.stage_name IN (
          '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
        )
        THEN 1
      -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
      WHEN
        sfdc_opportunity.dim_crm_opportunity_id IN (
          '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
        )
        THEN 1
       -- remove test accounts
       WHEN
         sfdc_opportunity.dim_crm_account_id = '0014M00001kGcORQA0'
         THEN 1
       --remove test accounts
       WHEN (sfdc_opportunity.dim_parent_crm_account_id = ('0016100001YUkWVAA1')
            OR sfdc_opportunity.dim_crm_account_id IS NULL)
         THEN 1
       -- remove jihu accounts
       WHEN sfdc_opportunity_live.is_jihu_account = 1
         THEN 1
       -- remove deleted opps
        WHEN sfdc_opportunity.is_deleted = 1
          THEN 1
         ELSE 0
      END AS is_excluded_from_pipeline_created,
      CASE
        WHEN sfdc_opportunity.is_open = 1
          THEN DATEDIFF(days, sfdc_opportunity.created_date, sfdc_opportunity.snapshot_date)
        WHEN sfdc_opportunity.is_open = 0 AND sfdc_opportunity.snapshot_date < sfdc_opportunity.close_date
          THEN DATEDIFF(days, sfdc_opportunity.created_date, sfdc_opportunity.snapshot_date)
        ELSE DATEDIFF(days, sfdc_opportunity.created_date, sfdc_opportunity.close_date)
      END                                                       AS calculated_age_in_days,
      CASE
        WHEN arr_created_fiscal_quarter_date = close_fiscal_quarter_date
          AND is_net_arr_pipeline_created = 1
            THEN net_arr
        ELSE 0
      END                                                         AS created_and_won_same_quarter_net_arr,
      IFF(sfdc_opportunity.comp_new_logo_override = 'Yes', 1, 0) AS is_comp_new_logo_override,
      IFF(arr_created_date.fiscal_quarter_name_fy = sfdc_opportunity.snapshot_fiscal_quarter_name AND is_net_arr_pipeline_created = 1, net_arr, 0) AS created_in_snapshot_quarter_net_arr,
      IFF(arr_created_date.fiscal_quarter_name_fy = sfdc_opportunity.snapshot_fiscal_quarter_name AND is_net_arr_pipeline_created = 1, calculated_deal_count, 0) AS created_in_snapshot_quarter_deal_count,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Other'),1,0) AS competitors_other_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'GitLab Core'),1,0) AS competitors_gitlab_core_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'None'),1,0) AS competitors_none_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'GitHub Enterprise'),1,0) AS competitors_github_enterprise_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'BitBucket Server'),1,0) AS competitors_bitbucket_server_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Unknown'),1,0) AS competitors_unknown_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'GitHub.com'),1,0) AS competitors_github_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'GitLab.com'),1,0) AS competitors_gitlab_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Jenkins'),1,0) AS competitors_jenkins_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Azure DevOps'),1,0) AS competitors_azure_devops_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'SVN'),1,0) AS competitors_svn_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'BitBucket.Org'),1,0) AS competitors_bitbucket_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Atlassian'),1,0) AS competitors_atlassian_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Perforce'),1,0) AS competitors_perforce_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Visual Studio Team Services'),1,0) AS competitors_visual_studio_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Azure'),1,0) AS competitors_azure_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Amazon Code Commit'),1,0) AS competitors_amazon_code_commit_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'CircleCI'),1,0) AS competitors_circleci_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'Bamboo'),1,0) AS competitors_bamboo_flag,
      IFF(CONTAINS(sfdc_opportunity.competitors, 'AWS'),1,0) AS competitors_aws_flag,
    CASE
        WHEN close_fiscal_year_live < 2024
          THEN CONCAT(
                    UPPER(sfdc_opportunity_live.crm_opp_owner_sales_segment_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_geo_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_region_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_area_stamped),
                    '-',
                    close_fiscal_year
                    )
        WHEN close_fiscal_year_live = 2024 AND LOWER(sfdc_opportunity_live.crm_opp_owner_business_unit_stamped) = 'comm'
          THEN CONCAT(
                    UPPER(sfdc_opportunity.crm_opp_owner_business_unit_stamped),
                    '-',
                    UPPER(sfdc_opportunity.crm_opp_owner_geo_stamped),
                    '-',
                    UPPER(sfdc_opportunity.crm_opp_owner_sales_segment_stamped),
                    '-',
                    UPPER(sfdc_opportunity.crm_opp_owner_region_stamped),
                    '-',
                    UPPER(sfdc_opportunity.crm_opp_owner_area_stamped),
                    '-',
                    close_fiscal_year_live
                    )
        WHEN close_fiscal_year_live = 2024 AND LOWER(sfdc_opportunity_live.crm_opp_owner_business_unit_stamped) = 'entg'
          THEN CONCAT(
                    UPPER(sfdc_opportunity_live.crm_opp_owner_business_unit_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_geo_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_region_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_area_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_sales_segment_stamped),
                    '-',
                    close_fiscal_year_live
                    )
        WHEN close_fiscal_year_live = 2024
          AND (sfdc_opportunity_live.crm_opp_owner_business_unit_stamped IS NOT NULL AND LOWER(sfdc_opportunity_live.crm_opp_owner_business_unit_stamped) NOT IN ('comm', 'entg')) -- some opps are closed by non-sales reps, so fill in their values completely
          THEN CONCAT(
                    UPPER(sfdc_opportunity_live.crm_opp_owner_business_unit_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_sales_segment_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_geo_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_region_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_area_stamped),
                    '-',
                    close_fiscal_year_live
                    )
        WHEN close_fiscal_year_live = 2024 AND sfdc_opportunity_live.crm_opp_owner_business_unit_stamped IS NULL -- done for data quality issues
          THEN CONCAT(
                    UPPER(sfdc_opportunity_live.crm_opp_owner_sales_segment_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_geo_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_region_stamped),
                    '-',
                    UPPER(sfdc_opportunity_live.crm_opp_owner_area_stamped),
                    '-',
                    close_fiscal_year_live
                    )
        WHEN close_fiscal_year_live >= 2025
          THEN CONCAT(

                      UPPER(COALESCE(sfdc_opportunity_live.opportunity_owner_role, sfdc_opportunity_live.opportunity_account_owner_role)),
                      '-',
                      close_fiscal_year_live
                      ) 
      END AS dim_crm_opp_owner_stamped_hierarchy_sk, 

      UPPER(
        IFF(sfdc_opportunity_live.close_date < close_date_live.current_first_day_of_fiscal_year, sfdc_opportunity_live.dim_crm_user_hierarchy_account_user_sk, dim_crm_opp_owner_stamped_hierarchy_sk)
      ) AS dim_crm_current_account_set_hierarchy_sk,

      DATEDIFF(MONTH, arr_created_fiscal_quarter_date, close_fiscal_quarter_date) AS arr_created_to_close_diff,
      CASE        
        WHEN arr_created_to_close_diff BETWEEN 0 AND 2 THEN 'CQ'
        WHEN arr_created_to_close_diff BETWEEN 3 AND 5 THEN 'CQ+1'
        WHEN arr_created_to_close_diff BETWEEN 6 AND 8 THEN 'CQ+2'
        WHEN arr_created_to_close_diff BETWEEN 9 AND 11 THEN 'CQ+3'
        WHEN arr_created_to_close_diff >= 12 THEN 'CQ+4 >='
      END AS landing_quarter_relative_to_arr_created_date,  

      DATEDIFF(MONTH, sfdc_opportunity.snapshot_fiscal_quarter_date, close_fiscal_quarter_date) AS snapshot_to_close_diff,

      CASE
        WHEN snapshot_to_close_diff BETWEEN 0 AND 2 THEN 'CQ'
        WHEN snapshot_to_close_diff BETWEEN 3 AND 5 THEN 'CQ+1'
        WHEN snapshot_to_close_diff BETWEEN 6 AND 8 THEN 'CQ+2'
        WHEN snapshot_to_close_diff BETWEEN 9 AND 11 THEN 'CQ+3'
        WHEN snapshot_to_close_diff >= 12 THEN 'CQ+4 >='
      END AS landing_quarter_relative_to_snapshot_date,  

    CASE
      WHEN is_renewal = 1 
          AND is_eligible_age_analysis = 1
            THEN DATEDIFF(day, arr_created_date, close_date.date_actual)
      WHEN is_renewal = 0 
          AND is_eligible_age_analysis = 1
            THEN DATEDIFF(day, sfdc_opportunity.created_date, close_date.date_actual) 
    END                                                           AS cycle_time_in_days,
    -- Snapshot Quarter Metrics

    -- This code calculates sales metrics for each snapshot quarter
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = arr_created_fiscal_quarter_date
        AND is_net_arr_pipeline_created = 1 
          THEN net_arr
      ELSE NULL
    END                                                         AS created_arr_in_snapshot_quarter,

    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_closed_won = TRUE 
          AND is_win_rate_calc = TRUE
            THEN calculated_deal_count
      ELSE NULL
    END                                                         AS closed_won_opps_in_snapshot_quarter,

    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_win_rate_calc = TRUE
          THEN calculated_deal_count
      ELSE NULL
    END                                                         AS closed_opps_in_snapshot_quarter,

    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_booked_net_arr = TRUE
          THEN net_arr
      ELSE NULL
    END                                                         AS booked_net_arr_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = arr_created_fiscal_quarter_date
        AND is_net_arr_pipeline_created = 1 
          THEN calculated_deal_count 
      ELSE NULL
    END                                                         AS created_deals_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_renewal = 1 
          AND  is_eligible_age_analysis = 1
            THEN DATEDIFF(day, arr_created_date, close_date.date_actual)
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_renewal = 0 
          AND  is_eligible_age_analysis = 1
            THEN DATEDIFF(day, sfdc_opportunity.created_date, close_date.date_actual) 
      ELSE NULL
    END                                                         AS cycle_time_in_days_in_snapshot_quarter, 
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_booked_net_arr = TRUE 
        THEN calculated_deal_count
      ELSE NULL
    END                                                         AS booked_deal_count_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1
            THEN net_arr
      ELSE NULL
    END                                                          AS open_1plus_net_arr_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN net_arr
      ELSE NULL
    END                                                          AS open_3plus_net_arr_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN net_arr
      ELSE NULL
    END                                                          AS open_4plus_net_arr_in_snapshot_quarter,
    CASE 
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date  
        AND is_eligible_open_pipeline = 1
          THEN calculated_deal_count
      ELSE NULL
    END                                                         AS open_1plus_deal_count_in_snapshot_quarter,

    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date  
        AND is_eligible_open_pipeline = 1
        AND is_stage_3_plus = 1
          THEN calculated_deal_count
      ELSE NULL
    END                                                         AS open_3plus_deal_count_in_snapshot_quarter,

    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date  
        AND is_eligible_open_pipeline = 1
        AND is_stage_4_plus = 1
          THEN calculated_deal_count
      ELSE NULL
    END                                                         AS open_4plus_deal_count_in_snapshot_quarter,

    -- Fields to calculate average deal size. Net arr in the numerator / deal count in the denominator
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_booked_net_arr = TRUE 
          AND net_arr > 0
        THEN 1
      ELSE NULL
    END                                                         AS positive_booked_deal_count_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND is_booked_net_arr = TRUE 
          AND net_arr > 0
        THEN net_arr
      ELSE NULL
    END                                                         AS positive_booked_net_arr_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1
            AND net_arr > 0
        THEN 1
      ELSE NULL
    END                                                         AS positive_open_deal_count_in_snapshot_quarter,
    CASE
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date 
        AND is_eligible_open_pipeline = 1
            AND net_arr > 0
        THEN net_arr
      ELSE NULL
    END                                                         AS positive_open_net_arr_in_snapshot_quarter,
    CASE 
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND sfdc_opportunity.is_closed = 'TRUE' 
          THEN calculated_deal_count 
        ELSE NULL
    END                                                         AS closed_deals_in_snapshot_quarter,
    CASE 
      WHEN sfdc_opportunity.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
        AND sfdc_opportunity.is_closed = 'TRUE' 
          THEN net_arr 
      ELSE NULL
    END                                                         AS closed_net_arr_in_snapshot_quarter, 
    -- Overall Sales Metrics

    -- This code calculates sales metrics without specific quarter alignment
    CASE
      WHEN is_net_arr_pipeline_created = 1 
          THEN net_arr
      ELSE NULL
    END                                                         AS created_arr,

    CASE
      WHEN is_closed_won = TRUE 
          AND is_win_rate_calc = TRUE
            THEN calculated_deal_count
      ELSE NULL
    END                                                         AS closed_won_opps,

    CASE
      WHEN is_win_rate_calc = TRUE
          THEN calculated_deal_count
      ELSE NULL
    END                                                         AS closed_opps,
    CASE
      WHEN is_net_arr_pipeline_created = 1 
          THEN calculated_deal_count 
      ELSE NULL
    END                                                         AS created_deals, 

    -- Fields to calculate average deal size. Net arr in the numerator / deal count in the denominator
    CASE
      WHEN is_booked_net_arr = TRUE 
          AND net_arr > 0
        THEN 1
      ELSE NULL
    END                                                         AS positive_booked_deal_count,
    CASE
      WHEN is_booked_net_arr = TRUE 
          AND net_arr > 0
        THEN net_arr
      ELSE NULL
    END                                                         AS positive_booked_net_arr,
    CASE
      WHEN is_eligible_open_pipeline = 1
            AND net_arr > 0
        THEN 1
      ELSE NULL
    END                                                         AS positive_open_deal_count,
    CASE
      WHEN is_eligible_open_pipeline = 1
            AND net_arr > 0
        THEN net_arr
      ELSE NULL
    END                                                         AS positive_open_net_arr,
    CASE 
      WHEN sfdc_opportunity.is_closed = 'TRUE' 
          THEN calculated_deal_count 
        ELSE NULL
    END                                                         AS closed_deals,
    CASE 
      WHEN sfdc_opportunity.is_closed = 'TRUE' 
          THEN net_arr 
      ELSE NULL
    END                                                         AS closed_net_arr
  FROM sfdc_opportunity
  INNER JOIN sfdc_opportunity_stage
    ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
  LEFT JOIN quote
    ON sfdc_opportunity.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
  LEFT JOIN linear_attribution_base
    ON sfdc_opportunity.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
  LEFT JOIN campaigns_per_opp
    ON sfdc_opportunity.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id
  LEFT JOIN first_contact
    ON sfdc_opportunity.dim_crm_opportunity_id = first_contact.opportunity_id AND first_contact.row_num = 1
  LEFT JOIN sfdc_opportunity_live
    ON sfdc_opportunity_live.dim_crm_opportunity_id = sfdc_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_date AS close_date
    ON sfdc_opportunity.close_date = close_date.date_actual
  LEFT JOIN dim_date AS close_date_live
    ON sfdc_opportunity_live.close_date = close_date_live.date_actual
  LEFT JOIN dim_date AS created_date
    ON sfdc_opportunity.created_date = created_date.date_actual
  LEFT JOIN dim_date AS arr_created_date
    ON sfdc_opportunity.iacv_created_date::DATE = arr_created_date.date_actual
  LEFT JOIN dim_date AS subscription_start_date
    ON sfdc_opportunity.subscription_start_date::DATE = subscription_start_date.date_actual
  LEFT JOIN net_iacv_to_net_arr_ratio
    ON sfdc_opportunity.opportunity_owner_user_segment = net_iacv_to_net_arr_ratio.user_segment_stamped
      AND sfdc_opportunity.order_type = net_iacv_to_net_arr_ratio.order_type
  LEFT JOIN sfdc_record_type_source 
    ON sfdc_opportunity.record_type_id = sfdc_record_type_source.record_type_id
  LEFT JOIN abm_tier_unioned
    ON sfdc_opportunity.dim_crm_opportunity_id=abm_tier_unioned.dim_crm_opportunity_id
      AND sfdc_opportunity.is_live = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2022-02-23",
    updated_date="2024-05-28"
) }}
