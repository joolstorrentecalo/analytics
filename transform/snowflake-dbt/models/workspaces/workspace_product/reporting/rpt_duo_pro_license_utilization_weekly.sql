{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_arr_all_weekly','mart_arr_weekly_with_zero_dollar_charges'),
    ('mart_ping_instance', 'mart_ping_instance'),
    ('dim_subscription', 'dim_subscription'),
    ('gitlab_dotcom_subscription_user_add_on_assignments', 'gitlab_dotcom_subscription_user_add_on_assignments'),
    ('gitlab_dotcom_subscription_add_on_purchases', 'gitlab_dotcom_subscription_add_on_purchases'),
    ('gitlab_dotcom_memberships', 'gitlab_dotcom_memberships'),
    ('mart_behavior_structured_event', 'mart_behavior_structured_event'),
    ('mart_ping_instance_metric_weekly', 'mart_ping_instance_metric_weekly'),
    ('rpt_behavior_code_suggestion_gateway_request', 'rpt_behavior_code_suggestion_gateway_request'),
    ('dim_product_detail', 'dim_product_detail')
	
    ])
}},

all_duo_pro_weekly_seats AS (

SELECT 
  arr_week
    AS reporting_week, --WEEK STARTING ON SUNDAY
  subscription_name,
  dim_subscription_id,
  crm_account_name,
  dim_crm_account_id,
  dim_parent_crm_account_id,
  product_deployment_type
    AS product_deployment,
  SUM(quantity) 
    AS dp_seats,
  SUM(arr)
    AS dp_arr,
  IFF(dp_arr > 0, TRUE, FALSE)
    AS is_dp_subscription_paid
FROM mart_arr_all_weekly
WHERE arr_week BETWEEN '2024-02-18' AND CURRENT_DATE -- first duo pro arr
  AND LOWER(product_rate_plan_name) LIKE '%duo pro%'
GROUP BY ALL

), 

duo_pro_and_paired_tier AS ( --tier occurring concurrently with paid duo pro subscription

SELECT 
  duo_pro.*,
  detail.is_oss_or_edu_rate_plan,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT SPLIT_PART(tier.product_tier_name, ' - ', 2)), ', ') -- multiple product tiers can show up within the same ARR reporting week
    AS paired_tier,
  IFF(paired_tier IN ('Premium, Ultimate', 'Ultimate, Premium'), 'Premium & Ultimate', paired_tier)
    AS clean_paired_tier, -- not able to sort within group while using SPLIT_PART function - using this method for standard results
FROM all_duo_pro_weekly_seats AS duo_pro
LEFT JOIN mart_arr_all_weekly AS tier -- joining to get tier occuring within same week as add on
  ON duo_pro.reporting_week = tier.arr_week
  AND duo_pro.dim_crm_account_id = tier.dim_crm_account_id
  AND duo_pro.dim_subscription_id = tier.dim_subscription_id -- add on will be on the same subscription as the tier
  AND LOWER(tier.product_rate_plan_name) NOT LIKE '%duo pro%'
  AND LOWER(tier.product_rate_plan_name) NOT LIKE '%storage%'
  AND LOWER(tier.product_rate_plan_name) NOT LIKE '%success plan%' --new non-tier plan
LEFT JOIN dim_product_detail AS detail
  ON detail.dim_product_detail_id = tier.dim_product_detail_id
GROUP BY ALL

), 

sm_dedicated_duo_pro_weekly_seats AS ( -- duo pro weekly seats associated entities -- dedicated and SM in one CTE due to the same type of product entity identifier used - dim_installation_id
--WEEK STARTING ON SUNDAY

  SELECT DISTINCT
    duo_pro.*,
    'Duo Pro'                                                                                        
      AS add_on_name, -- it've very possible that we will need to add additional add on names to this model in the future
    m.dim_installation_id
      AS product_entity_id,
    'dim_installation_id'
      AS product_entity_type,
    IFF(m.dim_installation_id IS NOT NULL, TRUE, FALSE)
      AS is_product_entity_associated_w_subscription,
    MAX(m.major_minor_version_id)
      AS major_minor_version_id, --max major minor version within week
    MAX(duo_pro.dp_seats)
      AS duo_pro_seats -- max because left join can result in duplicate records
  FROM duo_pro_and_paired_tier AS duo_pro
  LEFT JOIN mart_ping_instance_metric_weekly AS m -- joining to get installation id because that identifier is not in mart_arr
    ON duo_pro.dim_subscription_id = m.latest_subscription_id
    AND m.ping_created_date_week = reporting_week
  WHERE duo_pro.product_deployment IN ('Self-Managed', 'Dedicated')
    AND reporting_week BETWEEN '2024-02-18' AND DATEADD(day, -8, DATE_TRUNC(WEEK, current_date)) -- last week sunday (not including current week)
  GROUP BY ALL

), 

dotcom_duo_pro_weekly_seats AS ( -- duo pro weekly seats and associated entities 

  SELECT DISTINCT
    duo_pro.*,
    'Duo Pro'                                                                                        
      AS add_on_name,
    s.namespace_id
      AS product_entity_id,
    'ultimate_parent_namespace_id'
      AS product_entity_type,
    IFF(s.namespace_id IS NOT NULL, TRUE, FALSE)
      AS is_product_entity_associated_w_subscription,
    MAX(m.major_minor_version_id)
      AS major_minor_version_id, --max major minor version within week
    MAX(duo_pro.dp_seats)
      AS duo_pro_seats -- max because left join can result in duplicate records
  FROM duo_pro_and_paired_tier AS duo_pro
  INNER JOIN dim_subscription AS s -- joining to get namespace id because that identifier is not in mart_arr
    ON duo_pro.dim_subscription_id = s.dim_subscription_id
  LEFT JOIN mart_ping_instance_metric_weekly AS m-- for latest version
    ON m.ping_created_date_week = reporting_week
    AND m.dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7' -- installation id for Gitlab.com
  WHERE product_deployment = 'GitLab.com'
    AND reporting_week BETWEEN '2024-02-18' AND DATEADD(day, -8, DATE_TRUNC(WEEK, current_date)) -- last week sunday (not including current week)
  GROUP BY ALL

),

duo_pro_seat_assignments AS ( -- CTE to get number of seats assigned per namepsace ID
-- methodology used in:
-- https://gitlab.com/gitlab-data/product-analytics/-/issues/1677
-- AND https://10az.online.tableau.com/#/site/gitlab/workbooks/2252465/views
-- Given that admins can re-assign purchased Duo Pro seats to different users during the course of time, gitlab_dotcom_subscription_user_add_on_assignments can have more user assignments than its purchase quantity (per purchase). i.e. The data source will have records for both old users & new/replaced users. 
-- We can only identify valid users once per month using PGP_IS_DELETED. Until this process runs, the assignment rate for some subscriptions can be greater than 100%. 

  SELECT
    m.namespace_id,
    COUNT(DISTINCT a.user_id) 
      AS number_of_seats_assigned,
    p.purchase_xid -- subscription name
  FROM gitlab_dotcom_subscription_user_add_on_assignments AS a
  INNER JOIN gitlab_dotcom_subscription_add_on_purchases AS p
    ON a.add_on_purchase_id = p.id
  INNER JOIN gitlab_dotcom_memberships AS m
    ON a.user_id = m.user_id
     AND p.namespace_id = p.namespace_id
  WHERE (a.pgp_is_deleted = FALSE OR a.pgp_is_deleted IS NULL) -- Exclude deleted rows -- https://gitlab.com/gitlab-data/analytics/-/issues/17911#note_1855404601
  GROUP BY ALL

),

dotcom_chat_users AS ( -- gitlab.com chat weekly users with subacriptions

  SELECT
    DATEADD(day, -1, DATE_TRUNC(week, behavior_date))  --WEEK STARTING ON SUNDAY                 
      AS reporting_week,
    duo_pro.product_entity_id,
    duo_pro.product_entity_type,
    'duo pro'                                               
      AS unit_primitive_group,
    'chat'                                                  
      AS primitive,
    ZEROIFNULL(COUNT(DISTINCT gsc_pseudonymized_user_id))
      AS count_active_users
  FROM mart_behavior_structured_event, lateral flatten(input => contexts:data[0]:data:feature_enabled_by_namespace_ids) AS f  --GSC_FEATURE_ENABLED_BY_NAMESPACE_IDS is a VARCHAR datatype, so I need to use the value from contexts here
  INNER JOIN dotcom_duo_pro_weekly_seats AS duo_pro
    ON duo_pro.product_entity_id = f.value
  WHERE event_action = 'request_duo_chat_response'
    AND behavior_at >= '2024-04-11' -- event was implemented in production
    AND f.value IS NOT NULL
  GROUP BY ALL


),

sm_dedicated_chat_users AS ( -- sm & dedicated chat 28d count unique users with subscriptions

  SELECT DISTINCT
    p.ping_created_date_week  
      AS reporting_week,
    duo_pro.product_entity_id,
    duo_pro.product_entity_type,
    'duo pro'                  
      AS unit_primitive_group,
    'chat'                     
      AS primitive,
    ZEROIFNULL(p.metric_value) 
      AS count_active_users
  FROM sm_dedicated_duo_pro_weekly_seats AS duo_pro
  INNER JOIN mart_ping_instance_metric_weekly AS p
    ON duo_pro.product_entity_id = p.dim_installation_id
  WHERE p.metrics_path = 'redis_hll_counters.count_distinct_user_id_from_request_duo_chat_response_weekly'
    AND p.major_minor_version_id >= 1611 --metric instrumented for 1611
    AND p.metric_value > 0

),


dotcom_cs_users AS ( -- gitlab.com code_suggestions weekly users with subscriptions - first step is to flatten on the entity id

  SELECT
    DATEADD(day, -1, DATE_TRUNC(week, behavior_date))  --WEEK STARTING ON SUNDAY                 
      AS reporting_week,    
    duo_pro.product_entity_id,
    duo_pro.product_entity_type,
    'duo pro'                             
      AS unit_primitive_group,
    'code suggestions'                    
      AS primitive,
    COUNT(DISTINCT gitlab_global_user_id) 
      AS count_active_users
  FROM rpt_behavior_code_suggestion_gateway_request, LATERAL FLATTEN(input => ultimate_parent_namespace_ids) AS f
  INNER JOIN dotcom_duo_pro_weekly_seats AS duo_pro
    ON duo_pro.product_entity_id = f.value::VARCHAR
  WHERE event_action = 'suggestion_requested'
    AND app_id = 'gitlab_ai_gateway'
    AND behavior_at >= '2024-01-01' --first charge month
    AND f.value IS NOT NULL
  GROUP BY ALL

),

sm_dedicated_cs_users AS ( -- sm & dedicated chat code suggestions users with subscriptions - first step is to flatten on the entity id

  SELECT
    DATEADD(day, -1, DATE_TRUNC(week, behavior_date))  --WEEK STARTING ON SUNDAY                 
      AS reporting_week, 
    duo_pro.product_entity_id,
    duo_pro.product_entity_type,
    'duo pro'                             
      AS unit_primitive_group,
    'code suggestions'                    
      AS primitive,
    COUNT(DISTINCT gitlab_global_user_id)  
      AS count_active_users
  FROM rpt_behavior_code_suggestion_gateway_request, LATERAL FLATTEN(input => dim_installation_ids) AS f
  INNER JOIN sm_dedicated_duo_pro_weekly_seats AS duo_pro
    ON duo_pro.product_entity_id = f.value::VARCHAR
  WHERE event_action = 'suggestion_requested'
    AND app_id = 'gitlab_ai_gateway'
    AND behavior_at >= '2024-01-01' -- first charge month
    AND f.value IS NOT NULL
  GROUP BY ALL

),

unit_primitive_group_product_usage AS ( --long format to accomodate more unit primitive in the future cleanly

--Grain: entitiy id, reporting week, unit_primitive_group (theoretically there will be multiple in the future), primitive

  SELECT * FROM dotcom_chat_users

  UNION ALL

  SELECT * FROM sm_dedicated_chat_users

  UNION ALL

  SELECT * FROM dotcom_cs_users

  UNION ALL

  SELECT * FROM sm_dedicated_cs_users

),

all_weekly_duo_pro_seats AS (

  SELECT * FROM sm_dedicated_duo_pro_weekly_seats

  UNION ALL

  SELECT * FROM dotcom_duo_pro_weekly_seats

),


final AS (

--Grain: dim_crm_account_id, dim_subscription_id, reporting_week
--Because some SM and Dedicated installations can have multiple dim_crm_account_id values in mart_arr, including product_entity_id in the final model could lead to over counting seats purchased in a few cases

  SELECT
    a.reporting_week,
    a.subscription_name,
    a.dim_subscription_id,
    a.crm_account_name,
    a.dim_crm_account_id,
    a.dim_parent_crm_account_id,
    a.product_deployment, 
    a.add_on_name,
    a.clean_paired_tier  
      AS paired_tier,                                                                          
    a.is_product_entity_associated_w_subscription,
    a.is_dp_subscription_paid,
    MAX(a.major_minor_version_id)                                                                  
      AS major_minor_version_id,
    ZEROIFNULL(MAX(a.duo_pro_seats))                                                               
      AS paid_duo_pro_seats,
    MAX(CASE WHEN a.product_deployment = 'GitLab.com' THEN ZEROIFNULL(s.number_of_seats_assigned)
           ELSE null END)            
      AS count_seats_assigned,  -- only available for dotcom data - all SM/Dedicated deployments will show null  
    ZEROIFNULL(MAX(IFF(u.primitive = 'chat', ZEROIFNULL(u.count_active_users), NULL)))
      AS chat_active_users,
    ZEROIFNULL(MAX(IFF(u.primitive = 'code suggestions', ZEROIFNULL(u.count_active_users), NULL)))
      AS code_suggestions_active_users,
    ZEROIFNULL(MAX(count_active_users))
      AS max_duo_pro_active_users,
    ZEROIFNULL(max_duo_pro_active_users / paid_duo_pro_seats)
      AS pct_usage_seat_utilization,
    IFF(pct_usage_seat_utilization > 1, 1, pct_usage_seat_utilization)
      AS standard_pct_usage_seat_utilization,
    count_seats_assigned / paid_duo_pro_seats
      AS pct_assignment_seat_utilization,  -- only available for dotcom data - all SM/Dedicated deployments will show null     
    IFF(pct_assignment_seat_utilization > 1, 1,  pct_assignment_seat_utilization)
      AS standard_pct_assignment_seat_utilization,
    COALESCE(a.is_oss_or_edu_rate_plan, FALSE) 
      AS is_oss_or_edu_rate_plan
  FROM all_weekly_duo_pro_seats AS a
  LEFT JOIN unit_primitive_group_product_usage AS u
    ON a.reporting_week = u.reporting_week
      AND TO_CHAR(u.product_entity_id) = TO_CHAR(a.product_entity_id) --installation id contains letters, namespace id only numbers - TO_CHAR prevents datatype errors
      AND a.product_entity_type = u.product_entity_type
  LEFT JOIN duo_pro_seat_assignments AS s
    ON TO_CHAR(s.namespace_id) = TO_CHAR(a.product_entity_id)
      AND a.product_entity_type = 'ultimate_parent_namespace_id'
      AND a.subscription_name = s.purchase_xid -- # .com seats assigned associated with a subscription
  GROUP BY ALL

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2024-07-22",
    updated_date="2024-08-26"
) }}