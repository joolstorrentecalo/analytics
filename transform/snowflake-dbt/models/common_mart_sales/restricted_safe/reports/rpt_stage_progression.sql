with base as (
    select
    --create date and close date
    distinct dim_crm_opportunity_id, 
    case when stage_name='7 - Closing' then '7-Closing'
        when stage_name='Closed Lost' then '8-Closed Lost'
        else stage_name end as stage_name,
    arr_created_date as created_date, 
    min(snapshot_date) stage_date
    -- from prod.restricted_safe_common_mart_sales.mart_crm_opportunity_daily_snapshot
    from {{ ref('mart_crm_opportunity_daily_snapshot') }}
    where sales_qualified_source_name<>'Web Direct Generated'
    and arr_created_date>='2020-02-01'
    and sales_type<>'Renewal'
    and is_web_portal_purchase=false
    and opportunity_category not in ('Decommission','Internal Correction')
    and lower(opportunity_name) not like '%rebook%'
    and net_arr>0
    --exclude renewal sales_type=renewal
    --web portal purchase
    --opp category: exclude decommission and internal correction
    --opp name does not contain rebook
    --net arr > 0 ?
    group by 1,2,3
),

stage_base as(
    select
    dim_crm_opportunity_id,
    case when stage_name='0-Pending Acceptance' then 'stage0'
        when stage_name='1-Discovery' then 'stage1'
        when stage_name='2-Scoping' then 'stage2'
        when stage_name='3-Technical Evaluation' then 'stage3'
        when stage_name='4-Proposal' then 'stage4'
        when stage_name='5-Negotiating' then 'stage5'
        when stage_name='6-Awaiting Signature' then 'stage6'
        when stage_name='7-Closing' then 'stage7'
        when stage_name='8-Closed Lost' then 'closed_lost'
        when stage_name='Closed Won' then 'closed_won'
        end as stage_name, 
    created_date, stage_date,
    lag(stage_date,1) over(partition by dim_crm_opportunity_id order by stage_date) as prev_stage_date,
    lag(stage_name,1) over(partition by dim_crm_opportunity_id order by stage_date) as prev_stage_name,
    datediff(day,lag(stage_date,1) over(partition by dim_crm_opportunity_id order by stage_date),stage_date) as num_days_in_stage,
    count(*) over(partition by dim_crm_opportunity_id order by stage_date) as stage_rank,
    from base
    -- join to live table for role levels crm_opp_owner_role_level_#
    where stage_name in('0-Pending Acceptance','1-Discovery','2-Scoping','3-Technical Evaluation','4-Proposal','5-Negotiating','6-Awaiting Signature','7-Closing','8-Closed Lost','Closed Won')
),

stage_pivot as(
    select *
    from (select dim_crm_opportunity_id,created_date,stage_name,stage_date from stage_base)
    pivot(max(stage_date) for stage_name in('stage0','stage1','stage2','stage3','stage4','stage5','stage6','stage7','closed_lost','closed_won'))
),

stage_dates as (
    select 
    dim_crm_opportunity_id,
    case when "'closed_lost'" is not null then 'Lost'
        when "'closed_won'" is not null then 'Won'
        else 'Open' end as stage_category,
    created_date,
    "'stage0'" as stage0_date,
    "'stage1'" as stage1_date,
    "'stage2'" as stage2_date,
    "'stage3'" as stage3_date,
    "'stage4'" as stage4_date,
    "'stage5'" as stage5_date,
    "'stage6'" as stage6_date,
    "'stage7'" as stage7_date,
    coalesce("'closed_lost'","'closed_won'") as close_date
    from stage_pivot
),

dates_adj as(
    SELECT 
    dim_crm_opportunity_id,stage_category,created_date,
    --STAGE0
    IFF(STAGE0_DATE IS NULL, 
        IFF(STAGE1_DATE IS NULL, 
            IFF(STAGE2_DATE IS NULL, 
                IFF(STAGE3_DATE IS NULL, 
                    IFF(STAGE4_DATE IS NULL, 
                        IFF(STAGE5_DATE IS NULL, 
                            IFF(STAGE6_DATE IS NULL, 
                                IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
                            STAGE6_DATE), 
                        STAGE5_DATE), 
                    STAGE4_DATE), 
                STAGE3_DATE), 
            STAGE2_DATE), 
        STAGE1_DATE), 
    STAGE0_DATE) AS STAGE0_DATE,
    --STAGE1
    IFF(STAGE1_DATE IS NULL, 
        IFF(STAGE2_DATE IS NULL, 
            IFF(STAGE3_DATE IS NULL, 
                IFF(STAGE4_DATE IS NULL, 
                    IFF(STAGE5_DATE IS NULL, 
                        IFF(STAGE6_DATE IS NULL, 
                            IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
                        STAGE6_DATE), 
                    STAGE5_DATE), 
                STAGE4_DATE), 
            STAGE3_DATE), 
        STAGE2_DATE), 
    STAGE1_DATE) AS STAGE1_DATE,
    --STAGE2
    IFF(STAGE2_DATE IS NULL, 
        IFF(STAGE3_DATE IS NULL, 
            IFF(STAGE4_DATE IS NULL, 
                IFF(STAGE5_DATE IS NULL, 
                    IFF(STAGE6_DATE IS NULL, 
                        IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
                    STAGE6_DATE), 
                STAGE5_DATE), 
            STAGE4_DATE), 
        STAGE3_DATE), 
    STAGE2_DATE) AS STAGE2_DATE,
    --STAGE3
    IFF(STAGE3_DATE IS NULL, 
        IFF(STAGE4_DATE IS NULL, 
            IFF(STAGE5_DATE IS NULL, 
                IFF(STAGE6_DATE IS NULL, 
                    IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
                STAGE6_DATE), 
            STAGE5_DATE), 
        STAGE4_DATE), 
    STAGE3_DATE) AS STAGE3_DATE,
    --STAGE4
    IFF(STAGE4_DATE IS NULL, 
        IFF(STAGE5_DATE IS NULL, 
            IFF(STAGE6_DATE IS NULL, 
                IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
            STAGE6_DATE), 
        STAGE5_DATE), 
    STAGE4_DATE)  AS STAGE4_DATE,
    --STAGE5
    IFF(STAGE5_DATE IS NULL, 
        IFF(STAGE6_DATE IS NULL, 
            IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
        STAGE6_DATE), 
    STAGE5_DATE) AS STAGE5_DATE,
    --STAGE6
    IFF(STAGE6_DATE IS NULL, 
        IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE), 
    STAGE6_DATE) AS STAGE6_DATE,
    --STAGE7
    IFF(STAGE7_DATE IS NULL, CLOSE_DATE, STAGE7_DATE) AS STAGE7_DATE,
    --CLOSE_DATE
    CLOSE_DATE                    
    FROM stage_dates
),

opp_snap as(
    select 
    dim_crm_opportunity_id,stage_category,created_date,
    stage0_date-created_date as create_days,
    stage0_date,
    stage1_date-stage0_date as stage0_days,
    stage1_date,
    stage2_date-stage1_date as stage1_days,
    stage2_date,
    stage3_date-stage2_date as stage2_days,
    stage3_date,
    stage4_date-stage3_date as stage3_days,
    stage4_date,
    stage5_date-stage4_date as stage4_days,
    stage5_date,
    stage6_date-stage5_date as stage5_days,
    stage6_date,
    stage7_date-stage6_date as stage6_days,
    stage7_date,
    close_date-stage7_date as stage7_days,
    close_date,
    case when stage_category='Open' then current_date()-coalesce(stage7_date,stage6_date,stage5_date,stage4_date,stage3_date,stage2_date,stage1_date,stage0_date,created_date) end as current_days
    from dates_adj
    --remove negatives
    where (stage0_date-created_date>=0 or stage0_date-created_date is null)
    and (stage1_date-stage0_date>=0 or stage1_date-stage0_date is null)
    and (stage2_date-stage1_date>=0 or stage2_date-stage1_date is null)
    and (stage3_date-stage2_date>=0 or stage3_date-stage2_date is null)
    and (stage4_date-stage3_date>=0 or stage4_date-stage3_date is null)
    and (stage5_date-stage4_date>=0 or stage5_date-stage4_date is null)
    and (stage6_date-stage5_date>=0 or stage6_date-stage5_date is null)
    and (stage7_date-stage6_date>=0 or stage7_date-stage6_date is null)
    and (close_date-stage7_date>=0 or close_date-stage7_date is null)
    --remove duplicates
    and dim_crm_opportunity_id not in (select dim_crm_opportunity_id from dates_adj group by 1 having count(dim_crm_opportunity_id) > 1)
)

SELECT * FROM opp_snap
