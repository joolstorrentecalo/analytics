WITH marketo_person AS (
  
  SELECT DISTINCT 
    dim_marketo_person_id,
    dim_crm_person_id
  FROM {{ ref('prep_marketo_person') }}
   
), mart_crm_person AS (

    SELECT
        marketo_lead_id,
        sfdc_record_id,
        email_domain_type
    FROM {{ ref('mart_crm_person') }}

), mart_marketing_contact_no_pii AS (

    SELECT
        sfdc_record_id,
        has_namespace_setup_for_company_use
    FROM {{ ref('mart_marketing_contact_no_pii') }}

), dim_campaign AS (
  
  SELECT DISTINCT
    dim_campaign_id,
    campaign_name
  FROM {{ ref('dim_campaign') }}

), add_to_campaign AS (
  
  SELECT
    marketo_activity_add_to_sfdc_campaign_source.lead_id AS dim_marketo_person_id,
    marketo_activity_add_to_sfdc_campaign_source.activity_date::TIMESTAMP AS activity_datetime,
    marketo_activity_add_to_sfdc_campaign_source.activity_date::DATE AS activity_date,
    marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value AS campaign_name,
    marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value_id AS campaign_id,
    marketo_activity_add_to_sfdc_campaign_source.campaign_id AS marketo_campaign_id,
    dim_campaign.type AS campaign_type,
    marketo_activity_add_to_sfdc_campaign_source.status AS campaign_member_status
  FROM {{ ref('marketo_activity_add_to_sfdc_campaign_source') }}
  LEFT JOIN dim_campaign
    ON marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value=dim_campaign.campaign_name
  
), change_campaign_status AS (
  
  SELECT
    marketo_activity_change_status_in_sfdc_campaign_source.lead_id AS dim_marketo_person_id,
    marketo_activity_change_status_in_sfdc_campaign_source.activity_date::TIMESTAMP AS activity_datetime,
    marketo_activity_change_status_in_sfdc_campaign_source.activity_date::DATE AS activity_date,
    marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value AS campaign_name,
    marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value_id AS campaign_id,
    marketo_activity_change_status_in_sfdc_campaign_source.campaign_id AS marketo_campaign_id,
    dim_campaign.type AS campaign_type,
    marketo_activity_change_status_in_sfdc_campaign_source.new_status AS campaign_member_status
  FROM {{ ref('marketo_activity_change_status_in_sfdc_campaign_source') }}
  LEFT JOIN dim_campaign
    ON marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value=dim_campaign.campaign_name

), combined_campaign AS (

    SELECT *
    FROM add_to_campaign
    UNION ALL
    SELECT *
    FROM change_campaign_status
    
), combined_campaigns_with_activity_type AS (

    SELECT 
        combined_campaign.*,
        CASE 
            WHEN campaign_type = 'Gated Content'
                AND LOWER(campaign_member_status) IN ('downloaded')
                AND (campaign_name LIKE '%Gartner%'
                    OR campaign_name LIKE '%Forrester%')
            THEN 'Gated Content: Analyst Report'
            WHEN campaign_type = 'Conference'
                AND LOWER(campaign_member_status) IN ('attended','attended on-demand','meeting attended')
            THEN 'Conference'
            WHEN LOWER(campaign_member_status) = 'visited booth'
            THEN 'Conference: Visited Booth'
            WHEN campaign_type = 'Content Syndication'
                 AND LOWER(campaign_member_status) = 'downloaded' 
            THEN 'Content Syndication'
            WHEN campaign_type = 'Executive Roundtables'
                 AND LOWER(campaign_member_status) IN ('attended')
            THEN 'Exec Roundtable' 
            WHEN LOWER(campaign_member_status) IN ('follow up requested')
                AND NOT (campaign_type IN ('Direct Mail') 
                    OR campaign_type IS NULL)
            THEN 'Event Follow Up Requested'
            WHEN (campaign_type = 'Gated Content' OR campaign_type IS NULL)
                AND LOWER(campaign_member_status) IN ('downloaded')
                AND NOT (campaign_name LIKE '%Gartner%'
                 OR campaign_name LIKE '%Forrester%')
            THEN 'Gated Content: Other'
            WHEN (campaign_type = 'Inbound Request'
                AND LOWER(campaign_member_status) IN ('requested contact'))
                AND campaign_name IN ('FY20_Contact_Request', 'Request_Matterhorn Contact Sales',
                    'Request - GitLab Dedicated', 'Request_Usage_Limits', 'Request - Upgrade to Ultimate',
                    'FY20 - Renewals', 'Request - VSD', 'Request - GitLab Duo Pro', 'FY20_Professional Services',
                    '20230115_FY23Q4_EmailPromo - Landing Page', 'FY22_Integrated_PubSec_DI2E Campaign')
            THEN 'Inbound Request: High'
            WHEN campaign_type = 'Inbound - offline'
                AND LOWER(campaign_member_status) IN ('requested contact') 
            THEN 'Inbound Request: Offline - PQL'
            WHEN  (campaign_type = 'Inbound Request'
                AND LOWER(campaign_member_status) IN ('requested contact'))
                AND campaign_name IN ('FY22_Campaign_PartnerMarketplaceOfferings', '20231205_FY24Q4_EmailPromo - LP',
                    'Web form - ROI calculator', 'Request - Google S3C and GitLab Security Solution', 'GitLab Subscription Portal',
                    'FY22_Campaign_PartnerCloudCreditsPromo')
                OR (campaign_type = 'Brand'
                    AND LOWER(campaign_member_status) IN ('engaged')) 
            THEN 'Inbound Request: Medium'
            WHEN  (campaign_type = 'Inbound Request'
                AND LOWER(campaign_member_status) IN ('requested contact'))
                AND campaign_name IN ('Request - JiHu', 'Request-VSA-WorldTour',
                    'Application - Non-profit program', 'WIP_WF_LinkedIn Lead Ads_Test', 'FY20_Startup Application',
                    'Request - Reference Program - Customer Reference Leads', 'FY20 - Heros Application', 
                    'FY20_CommitUserConf_PreLaunch', 'Impartner - Request - Contact', 'Request - Leading Orgs')
            THEN 'Inbound Request: DNI'
            WHEN campaign_type = 'Owned Event'
                AND LOWER(campaign_member_status) IN ('attended','attended on-demand','responded','meeting attended')
            THEN 'Owned Event'
            WHEN campaign_type = 'Paid Social'
                AND LOWER(campaign_member_status) IN ('responded','downloaded')
            THEN 'Paid Social'
            WHEN  LOWER(campaign_member_status) IN ('registered')
                AND NOT (campaign_type IN ('Direct Mail', 'Partner - MDF', 'Partners', 'Brand') 
                    OR campaign_type IS NULL)
                OR (campaign_type = 'Conference' 
                    AND LOWER(campaign_member_status) IN ('meeting requested'))
            THEN 'Event: Registered' 
            WHEN  campaign_type = 'Speaking Session'
                AND LOWER(campaign_member_status) IN ('attended')
            THEN 'Speaking Session'
            WHEN campaign_type = 'Sponsored Webcast'
                AND LOWER(campaign_member_status) IN ('attended','attended on-demand')
            THEN 'Sponsored Webcast'
            WHEN campaign_type = 'Survey'
                AND LOWER(campaign_member_status) IN ('filled-out survey')
                AND campaign_name ILIKE ANY ('DONOTSCORE', '%google%', '%default%')
                AND NOT campaign_name LIKE '%googlecloud%'
            THEN 'Survey: Low'
            WHEN campaign_type = 'Survey'
                AND LOWER(campaign_member_status) IN ('filled-out survey')
                AND campaign_name IS NULL
            THEN 'Survey: Medium'
            WHEN campaign_type = 'Survey'
                AND LOWER(campaign_member_status) IN ('filled-out survey')
            THEN 'Survey: High'
            WHEN campaign_type = 'Vendor Arranged Meetings'
                AND LOWER(campaign_member_status) IN ('attended','meeting attended')
            THEN 'Vendor Meeting'
            WHEN campaign_type = 'Webcast'
                AND LOWER(campaign_member_status) IN ('attended','attended on-demand')
                AND NOT campaign_name LIKE '%_techdemo_%'
            THEN 'Webcast'
            WHEN campaign_type = 'Webcast'
                AND LOWER(campaign_member_status) IN ('attended','attended on-demand')
                AND campaign_name LIKE '%_techdemo_%'
            THEN 'Webcast: Tech Demo'
            WHEN LOWER(campaign_member_status) = 'sent' 
                AND campaign_name LIKE '%Qualified%'
            THEN 'Web Chat (Qualified)'
            WHEN LOWER(campaign_member_status) = 'sent' 
                AND campaign_name LIKE '%_Drift'
            THEN 'Web Chat (Drift)'
            WHEN campaign_type = 'Workshop'
                AND LOWER(campaign_member_status) IN ('attended','visited booth')
            THEN 'Workshop' 
            WHEN campaign_type = 'Cohort'
                AND LOWER(campaign_member_status) IN ('organic engaged')
            THEN 'Cohort: Organic Engaged'
            WHEN campaign_type = 'Email Send'
               AND LOWER(campaign_member_status) IN ('clicked in-email link')
            THEN 'Email: Clicked Link'
            WHEN campaign_type = 'PF Content'
                AND LOWER(campaign_member_status) IN ('content consumed')
            THEN 'PF: Content'
            WHEN campaign_type = 'PF Content'
                AND LOWER(campaign_member_status) IN ('fast moving buyer')
            THEN 'PF: Fast Moving Buyer boost' 
            WHEN campaign_type = 'Prospecting'
                AND LOWER(campaign_member_status) IN ('filled-out form','member','responded')
            THEN 'Prospecting'
            WHEN campaign_type = 'Self-Service Virtual Event'
                AND LOWER(campaign_member_status) IN ('attended')
            THEN 'Self Service Virtual Event'
            WHEN campaign_type = 'Email Send' 
                AND LOWER(campaign_member_status) IN ('email opened', 'opened', 'member') 
            THEN 'Email: Opened'
            WHEN campaign_type = 'Nurture'
                AND LOWER(campaign_member_status) IN ('influenced')
            THEN 'Nurture: Influenced'
        END AS activity_type
    FROM combined_campaign

), marketo_form_fill AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_fill_out_form_source') }}
    FROM {{ ref('marketo_activity_fill_out_form_source') }}

), inbound_forms_high AS (
  
    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS marketo_form_name,
        primary_attribute_value_id AS marketo_form_id,
        form_fields, 
        webpage_id AS marketo_webpage_id,
        query_parameters AS marketo_query_parameters,
        referrer_url AS marketo_referrer_url, 
        client_ip_address_hash AS marketo_form_client_ip_address_hash,
        campaign_id AS marketo_campaign_id, 
        'Inbound Request: High' AS activity_type,
        TRUE AS in_current_scoring_model,
        'Inbound - High' AS scored_action
    FROM marketo_form_fill
    WHERE primary_attribute_value LIKE 'FORM 3162%'
        OR primary_attribute_value LIKE 'FORM 3245%'
        OR primary_attribute_value LIKE 'FORM 1754%'
        OR primary_attribute_value LIKE 'FORM 1476%'
        OR primary_attribute_value LIKE 'FORM 3226%'
        OR primary_attribute_value LIKE 'FORM 3245%'

 ), subscription AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS marketo_form_name,
    primary_attribute_value_id AS marketo_form_id,
    form_fields, 
    webpage_id AS marketo_webpage_id,
    query_parameters AS marketo_query_parameters,
    referrer_url AS marketo_referrer_url, 
    client_ip_address_hash AS marketo_form_client_ip_address_hash,
    campaign_id AS marketo_campaign_id, 
    'Form - Subscription' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Subscription' AS scored_action
  FROM marketo_form_fill
  WHERE primary_attribute_value LIKE 'FORM 1102%'
    OR primary_attribute_value LIKE 'FORM 1077%'
    OR primary_attribute_value LIKE 'FORM 1546%'
    OR primary_attribute_value LIKE 'FORM 1547%'
    OR primary_attribute_value LIKE 'FORM 2425%'
    OR primary_attribute_value LIKE 'FORM 3119%'
    OR primary_attribute_value LIKE 'FORM 3686%'
    OR primary_attribute_value LIKE 'FORM 3687%'
    OR primary_attribute_value LIKE 'FORM 1364%'
    OR primary_attribute_value LIKE 'FORM 3814%'
    OR primary_attribute_value LIKE 'FORM 1073%'
  
), self_managed_trial AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS marketo_form_name,
    primary_attribute_value_id AS marketo_form_id,
    form_fields, 
    webpage_id AS marketo_webpage_id,
    query_parameters AS marketo_query_parameters,
    referrer_url AS marketo_referrer_url, 
    client_ip_address_hash AS marketo_form_client_ip_address_hash,
    campaign_id AS marketo_campaign_id, 
    'Form - Self-Managed Trial' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Trial' AS scored_action
  FROM marketo_form_fill
  WHERE primary_attribute_value LIKE '%FORM 1318%'
    OR primary_attribute_value LIKE '%FORM 2150%'
    OR primary_attribute_value LIKE '%FORM 3438%'
    OR primary_attribute_value LIKE '%FORM 3648%'
    OR primary_attribute_value LIKE '%FORM 3240%'

), filled_out_form_general AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS marketo_form_name,
        primary_attribute_value_id AS marketo_form_id,
        form_fields, 
        webpage_id AS marketo_webpage_id,
        query_parameters AS marketo_query_parameters,
        referrer_url AS marketo_referrer_url, 
        client_ip_address_hash AS marketo_form_client_ip_address_hash,
        campaign_id AS marketo_campaign_id, 
        'Form - General' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM marketo_form_fill
    WHERE primary_attribute_value NOT LIKE '%FORM 1318%'
        AND primary_attribute_value NOT LIKE '%FORM 2150%'
        AND primary_attribute_value NOT LIKE '%FORM 3438%'
        AND primary_attribute_value NOT LIKE '%FORM 3648%'
        AND primary_attribute_value NOT LIKE '%FORM 3240%'
        AND primary_attribute_value NOT LIKE 'FORM 1102%'
        AND primary_attribute_value NOT LIKE 'FORM 1077%'
        AND primary_attribute_value NOT LIKE 'FORM 1546%'
        AND primary_attribute_value NOT LIKE 'FORM 1547%'
        AND primary_attribute_value NOT LIKE 'FORM 2425%'
        AND primary_attribute_value NOT LIKE 'FORM 3119%'
        AND primary_attribute_value NOT LIKE 'FORM 3686%'
        AND primary_attribute_value NOT LIKE 'FORM 3687%'
        AND primary_attribute_value NOT LIKE 'FORM 1364%'
        AND primary_attribute_value NOT LIKE 'FORM 3814%'
        AND primary_attribute_value NOT LIKE 'FORM 1073%'
        AND primary_attribute_value NOT LIKE 'FORM 3162%'
        AND primary_attribute_value NOT LIKE 'FORM 3245%'
        AND primary_attribute_value NOT LIKE 'FORM 1754%'
        AND primary_attribute_value NOT LIKE 'FORM 1476%'
        AND primary_attribute_value NOT LIKE 'FORM 3226%'
        AND primary_attribute_value NOT LIKE 'FORM 3245%'

), fill_out_li_form AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS linkedin_form_name,
    primary_attribute_value_id AS marketo_form_id,
    campaign_id AS marketo_campaign_id, 
    'LI Form' AS activity_type,
    FALSE AS in_current_scoring_model,
    NULL AS scored_action
  FROM {{ ref('marketo_activity_fill_out_linkedin_lead_gen_form_source') }}

), marketo_visit_web_page AS (

    SELECT {{ hash_sensitive_columns('marketo_activity_visit_webpage_source') }}
    FROM {{ ref('marketo_activity_visit_webpage_source') }}

), visit_key_page AS (
  
  -- SELECT DISTINCT
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS webpage,
    client_ip_address_hash AS marketo_form_client_ip_address_hash,
    activity_type_id,
    campaign_id AS marketo_campaign_id,
    primary_attribute_value_id AS marketo_webpage_id,
    webpage_url,
    search_engine,
    query_parameters AS marketo_query_parameters,
    search_query,
    'Key Page' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Visits Key Webpage' AS scored_action
  FROM marketo_visit_web_page
  WHERE (primary_attribute_value LIKE '%/pricing%'
         OR primary_attribute_value LIKE '%/get-started%'
         OR primary_attribute_value LIKE '%/install%'
         OR primary_attribute_value LIKE '%/free-trial%'
         OR primary_attribute_value LIKE '%/livestream%')
    AND primary_attribute_value NOT LIKE '%docs.gitlab.com%'
  
), visit_low_page AS (
  
  -- SELECT DISTINCT 
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS webpage,
    client_ip_address_hash AS marketo_form_client_ip_address_hash,
    activity_type_id,
    campaign_id AS marketo_campaign_id,
    primary_attribute_value_id AS marketo_webpage_id,
    webpage_url,
    search_engine,
    query_parameters AS marketo_query_parameters,
    search_query,
    'Low Page' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Web: Visits Low Value' AS scored_action
  FROM marketo_visit_web_page
  WHERE primary_attribute_value LIKE '%/jobs%'
    AND primary_attribute_value NOT LIKE '%docs.gitlab.com%'

), visit_multi_pages_base AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    COUNT(DISTINCT marketo_activity_visit_webpage_id) AS visits
  FROM marketo_visit_web_page
  {{dbt_utils.group_by(n=3)}}

), visit_multi_pages AS (
  
  SELECT
    dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    'Multiple Pages' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Visits Mult Webpages' AS scored_action
  FROM visit_multi_pages_base
  WHERE visits >= 7
  
), email_bounced AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id AS marketo_campaign_id,
    campaign_run_id AS marketo_campaign_run_id,
    category AS marketo_email_category,
    details AS marketo_email_bounce_reason, 
    subcategory AS marketo_email_subcategory, 
    step_id AS marketo_email_program_step_id,
    test_variant AS marketo_email_test_variant,
    'Email: Bounced' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Email: Bounce' AS scored_action
  FROM {{ ref('marketo_activity_email_bounced_source') }}
  
), email_unsubscribe AS (
  
  SELECT
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    primary_attribute_value AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id AS marketo_campaign_id,
    campaign_run_id AS marketo_campaign_run_id,
    webform_id AS marketo_form_id, 
    form_fields, 
    webpage_id AS marketo_webpage_id,
    query_parameters AS marketo_query_parameters,
    referrer_url AS marketo_referrer_url, 
    test_variant AS marketo_email_test_variant,
    'Email: Unsubscribe' AS activity_type,
    TRUE AS in_current_scoring_model,
    'Email: Unsubscribed' AS scored_action
  FROM {{ ref('marketo_activity_unsubscribe_email_source') }}
  
), lead_score_decay AS (
  
  SELECT DISTINCT 
    lead_id AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE AS activity_date,
    reason AS marketo_score_decay_reason,
    campaign_id AS marketo_campaign_id,
    primary_attribute_value AS marketo_score_decay_type, 
    'Score Decay' AS activity_type,    
    TRUE AS in_current_scoring_model,
    'No activity in 30 days' AS scored_action,
    SUM(new_value - old_value) AS score_change
  FROM {{ ref('marketo_activity_change_score_source') }}
  WHERE change_value LIKE '%Decay%'
  {{dbt_utils.group_by(n=9)}}

), trial_self_managed_default AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        marketo_form_name,
        marketo_form_id,
        form_fields,
        marketo_webpage_id,
        marketo_query_parameters,
        marketo_referrer_url,
        marketo_form_client_ip_address_hash,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Default' AS scored_action,
        'Self-Managed - Default' AS trial_type
    FROM self_managed_trial
    LEFT JOIN mart_crm_person
        ON self_managed_trial.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    LEFT JOIN mart_marketing_contact_no_pii
        ON mart_crm_person.sfdc_record_id=mart_marketing_contact_no_pii.sfdc_record_id
    WHERE mart_crm_person.email_domain_type NOT IN ('Business email domain', 'Personal email domain')
        AND mart_marketing_contact_no_pii.has_namespace_setup_for_company_use = FALSE


), trial_saas_default AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        campaign_name,
        campaign_id,
        campaign_type,
        campaign_member_status,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Default' AS scored_action,
        'SaaS - Default' AS trial_type
    FROM combined_campaigns_with_activity_type
    LEFT JOIN mart_crm_person
        ON combined_campaigns_with_activity_type.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    LEFT JOIN mart_marketing_contact_no_pii
        ON mart_crm_person.sfdc_record_id=mart_marketing_contact_no_pii.sfdc_record_id
    WHERE mart_crm_person.email_domain_type NOT IN ('Business email domain', 'Personal email domain')
        AND mart_marketing_contact_no_pii.has_namespace_setup_for_company_use = FALSE
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%saas%'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%trial%'

), trial_self_managed_personal AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        marketo_form_name,
        marketo_form_id,
        form_fields,
        marketo_webpage_id,
        marketo_query_parameters,
        marketo_referrer_url,
        marketo_form_client_ip_address_hash,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Personal' AS scored_action,
        'Self-Managed - Personal' AS trial_type
    FROM self_managed_trial
    LEFT JOIN mart_crm_person
        ON self_managed_trial.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    WHERE mart_crm_person.email_domain_type = 'Personal email domain'

), trial_saas_personal AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        campaign_name,
        campaign_id,
        campaign_type,
        campaign_member_status,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Personal' AS scored_action,
        'SaaS - Personal' AS trial_type
    FROM combined_campaigns_with_activity_type
    LEFT JOIN mart_crm_person
        ON combined_campaigns_with_activity_type.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    WHERE mart_crm_person.email_domain_type = 'Personal email domain'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%saas%'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%trial%'

), trial_self_managed_business AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        marketo_form_name,
        marketo_form_id,
        form_fields,
        marketo_webpage_id,
        marketo_query_parameters,
        marketo_referrer_url,
        marketo_form_client_ip_address_hash,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Business' AS scored_action,
        'Self-Managed - Business' AS trial_type
    FROM self_managed_trial
    LEFT JOIN mart_crm_person
        ON self_managed_trial.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    LEFT JOIN mart_marketing_contact_no_pii
        ON mart_crm_person.sfdc_record_id=mart_marketing_contact_no_pii.sfdc_record_id
    WHERE mart_crm_person.email_domain_type = 'Business email domain' 
        OR (mart_marketing_contact_no_pii.has_namespace_setup_for_company_use = TRUE 
            AND mart_crm_person.email_domain_type != 'Personal email domain')
  
), trial_saas_business AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date,
        campaign_name,
        campaign_id,
        campaign_type,
        campaign_member_status,
        marketo_campaign_id,
        activity_type,
        TRUE AS in_current_scoring_model,
        'Trial - Business' AS scored_action,
        'SaaS - Business' AS trial_type
    FROM combined_campaigns_with_activity_type
    LEFT JOIN mart_crm_person
        ON combined_campaigns_with_activity_type.dim_marketo_person_id=mart_crm_person.marketo_lead_id
    LEFT JOIN mart_marketing_contact_no_pii
        ON mart_crm_person.sfdc_record_id=mart_marketing_contact_no_pii.sfdc_record_id
    WHERE mart_crm_person.email_domain_type = 'Business email domain' 
        OR (mart_marketing_contact_no_pii.has_namespace_setup_for_company_use = TRUE 
            AND mart_crm_person.email_domain_type != 'Personal email domain')
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%saas%'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%trial%'

), add_to_nurture AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        campaign_id AS marketo_campaign_id,
        primary_attribute_value AS marketo_nurture,
        primary_attribute_value_id AS marketo_nurture_id,
        track_id AS marketo_nurture_track_id,
        'Add to Nurture' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_add_to_nurture_source') }}

), change_nurture_track AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        campaign_id AS marketo_campaign_id,
        primary_attribute_value AS marketo_nurture,
        primary_attribute_value_id AS marketo_nurture_id,
        previous_track_id AS marketo_nurture_previous_track_id,
        new_track_id AS marketo_nurture_new_track_id,
        'Change Nurture Track' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_change_nurture_track_source') }}
    
), change_score AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS changed_score_field,
        change_value AS scoring_rule,
        old_value AS old_score,
        new_value AS new_score,
        'Score Change' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_change_score_source') }}
    
), click_link AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        campaign_id AS marketo_campaign_id,
        primary_attribute_value AS clicked_url,
        webpage_id AS marketo_webpage_id,
        query_parameters AS marketo_query_parameters, 
        referrer_url AS marketo_referrer_url,
        'Click Email Link' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_click_link_source') }}
    
), email_delivered AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS marketo_email_name,
        primary_attribute_value_id AS marketo_email_id,
        campaign_id AS marketo_campaign_id,
        step_id AS marketo_email_program_step_id,
        test_variant AS marketo_email_test_variant,
        'Email Delivered' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_email_delivered_source') }}
    
), email_opened AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS marketo_email_name,
        primary_attribute_value_id AS marketo_email_id,
        campaign_id AS marketo_campaign_id,
        test_variant AS marketo_email_test_variant,
        is_bot_activity,
        'Email Opened' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_open_email_source') }}
    
), push_lead_to_marketo AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS push_lead_to_marketo_source,
        primary_attribute_value_id AS push_lead_to_marketo_source_id,
        campaign_id AS marketo_campaign_id,
        'Push Lead to Marketo' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_push_lead_to_marketo_source') }}
    
), email_sent AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        primary_attribute_value AS marketo_email_name,
        primary_attribute_value_id AS marketo_email_id,
        campaign_id AS marketo_campaign_id,
        test_variant AS marketo_email_test_variant,
        'Email Sent' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_send_email_source') }}
    
), push_lead_to_sfdc AS (

    SELECT
        lead_id AS dim_marketo_person_id,
        activity_date::TIMESTAMP AS activity_datetime,
        activity_date::DATE AS activity_date,
        campaign_id AS marketo_campaign_id,
        'Sync Lead to Salesforce' AS activity_type,
        FALSE AS in_current_scoring_model,
        NULL AS scored_action
    FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source') }}
    
), activities_date_prep AS (

    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM combined_campaigns_with_activity_type
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM inbound_forms_high
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM subscription
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM fill_out_li_form
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM visit_key_page
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM visit_low_page
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM visit_multi_pages
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM email_bounced
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM email_unsubscribe
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM lead_score_decay
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_self_managed_default
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_saas_default
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_self_managed_personal
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_saas_personal
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_self_managed_business
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM trial_saas_business
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM filled_out_form_general
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM add_to_nurture
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM change_nurture_track
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM change_score
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM click_link
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM email_delivered
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM email_sent
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM email_opened
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM push_lead_to_marketo
    UNION ALL
    SELECT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM push_lead_to_sfdc
        
), activites_date_final AS (

    SELECT DISTINCT
        dim_marketo_person_id,
        activity_datetime,
        activity_date
    FROM activities_date_prep 
        
), activities_final AS (

    SELECT
        activites_date_final.dim_marketo_person_id,
        activites_date_final.activity_datetime,
        activites_date_final.activity_date,
COALESCE(combined_campaigns_with_activity_type.campaign_name,trial_saas_default.campaign_name,trial_saas_personal.campaign_name,trial_saas_business.campaign_name) AS campaign_name,
        COALESCE(combined_campaigns_with_activity_type.campaign_id,trial_saas_default.campaign_id,trial_saas_personal.campaign_id,trial_saas_business.campaign_id) AS campaign_id,
        COALESCE(combined_campaigns_with_activity_type.marketo_campaign_id,inbound_forms_high.marketo_campaign_id,subscription.marketo_campaign_id,fill_out_li_form.marketo_campaign_id,visit_key_page.marketo_campaign_id,visit_low_page.marketo_campaign_id,email_bounced.marketo_campaign_id,email_unsubscribe.marketo_campaign_id,lead_score_decay.marketo_campaign_id,trial_self_managed_default.marketo_campaign_id,trial_self_managed_personal.marketo_campaign_id,trial_self_managed_business.marketo_campaign_id,filled_out_form_general.marketo_campaign_id,change_nurture_track.marketo_campaign_id,click_link.marketo_campaign_id,email_delivered.marketo_campaign_id,email_opened.marketo_campaign_id,push_lead_to_marketo.marketo_campaign_id,email_sent.marketo_campaign_id,push_lead_to_sfdc.marketo_campaign_id) AS marketo_campaign_id,
        COALESCE(combined_campaigns_with_activity_type.campaign_type,trial_saas_default.campaign_type,trial_saas_personal.campaign_type,trial_saas_business.campaign_type) AS campaign_type,
        COALESCE(combined_campaigns_with_activity_type.campaign_member_status,trial_saas_default.campaign_member_status,trial_saas_personal.campaign_member_status,trial_saas_business.campaign_member_status) AS campaign_member_status,
        COALESCE(inbound_forms_high.marketo_form_name,subscription.marketo_form_name,trial_self_managed_default.marketo_form_name,trial_self_managed_personal.marketo_form_name,trial_self_managed_business.marketo_form_name,filled_out_form_general.marketo_form_name) AS marketo_form_name,
        COALESCE(inbound_forms_high.marketo_form_id,subscription.marketo_form_id,fill_out_li_form.marketo_form_id,email_unsubscribe.marketo_form_id,trial_self_managed_default.marketo_form_id,trial_self_managed_personal.marketo_form_id,trial_self_managed_business.marketo_form_id,filled_out_form_general.marketo_form_id) AS marketo_form_id,
        COALESCE(inbound_forms_high.form_fields,subscription.form_fields,email_unsubscribe.form_fields,trial_self_managed_default.form_fields,trial_self_managed_business.form_fields, trial_self_managed_personal.form_fields,filled_out_form_general.form_fields) AS form_fields,
        COALESCE(inbound_forms_high.marketo_webpage_id,subscription.marketo_webpage_id,trial_self_managed_default.marketo_webpage_id,trial_self_managed_personal.marketo_webpage_id,trial_self_managed_business.marketo_webpage_id,filled_out_form_general.marketo_webpage_id,click_link.marketo_webpage_id,visit_key_page.marketo_webpage_id,visit_low_page.marketo_webpage_id,email_unsubscribe.marketo_webpage_id) AS marketo_webpage_id,
        COALESCE(inbound_forms_high.marketo_query_parameters,visit_key_page.marketo_query_parameters,visit_low_page.marketo_query_parameters,email_unsubscribe.marketo_query_parameters,subscription.marketo_query_parameters,trial_self_managed_default.marketo_query_parameters,trial_self_managed_personal.marketo_query_parameters,trial_self_managed_business.marketo_query_parameters,filled_out_form_general.marketo_query_parameters,click_link.marketo_query_parameters) AS marketo_query_parameters,
        COALESCE(inbound_forms_high.marketo_referrer_url,subscription.marketo_referrer_url,email_unsubscribe.marketo_referrer_url,trial_self_managed_default.marketo_referrer_url,trial_self_managed_personal.marketo_referrer_url,trial_self_managed_business.marketo_referrer_url,filled_out_form_general.marketo_referrer_url,click_link.marketo_referrer_url) AS marketo_referrer_url,
        COALESCE(inbound_forms_high.marketo_form_client_ip_address_hash,subscription.marketo_form_client_ip_address_hash,visit_key_page.marketo_form_client_ip_address_hash,visit_low_page.marketo_form_client_ip_address_hash,trial_self_managed_default.marketo_form_client_ip_address_hash,trial_self_managed_personal.marketo_form_client_ip_address_hash,trial_self_managed_business.marketo_form_client_ip_address_hash,filled_out_form_general.marketo_form_client_ip_address_hash) AS marketo_form_client_ip_address_hash,
        
        fill_out_li_form.linkedin_form_name,
        COALESCE(visit_key_page.webpage,visit_low_page.webpage) AS webpage,
        COALESCE(visit_key_page.webpage_url,visit_low_page.webpage_url) AS webpage_url,
        COALESCE(visit_key_page.search_engine,visit_low_page.search_engine) AS search_engine,
        COALESCE(visit_key_page.search_query,visit_low_page.search_query) AS search_query,
        COALESCE(visit_key_page.activity_type_id,visit_low_page.activity_type_id) AS activity_type_id,
        COALESCE(change_nurture_track.marketo_nurture,add_to_nurture.marketo_nurture) AS marketo_nurture,
        COALESCE(change_nurture_track.marketo_nurture_id,add_to_nurture.marketo_nurture_id) AS marketo_nurture_id,
        change_nurture_track.marketo_nurture_previous_track_id,
        change_nurture_track.marketo_nurture_new_track_id,
COALESCE(email_bounced.marketo_email_name,email_unsubscribe.marketo_email_name,email_delivered.marketo_email_name,email_opened.marketo_email_name,email_sent.marketo_email_name) AS marketo_email_name,
        COALESCE(email_bounced.marketo_email_id,email_unsubscribe.marketo_email_id,email_delivered.marketo_email_id,email_opened.marketo_email_id,email_sent.marketo_email_id) AS marketo_email_id,
        COALESCE(email_bounced.marketo_campaign_run_id,email_unsubscribe.marketo_campaign_run_id) AS marketo_campaign_run_id,
        email_bounced.marketo_email_category,
        email_bounced.marketo_email_bounce_reason, 
        email_bounced.marketo_email_subcategory, 
        COALESCE(email_bounced.marketo_email_program_step_id,email_delivered.marketo_email_program_step_id) AS marketo_email_program_step_id,
        COALESCE(email_bounced.marketo_email_test_variant,email_delivered.marketo_email_test_variant,email_opened.marketo_email_test_variant,email_opened.marketo_email_test_variant) AS marketo_email_test_variant,
        change_score.changed_score_field,
        change_score.scoring_rule,
        change_score.old_score,
        change_score.new_score,
        click_link.clicked_url,
        push_lead_to_marketo.push_lead_to_marketo_source,
        push_lead_to_marketo.push_lead_to_marketo_source_id,
        lead_score_decay.marketo_score_decay_reason,
        lead_score_decay.marketo_score_decay_type,
COALESCE(trial_self_managed_default.trial_type,trial_saas_default.trial_type,trial_self_managed_personal.trial_type,trial_saas_personal.trial_type,trial_self_managed_business.trial_type,trial_saas_business.trial_type) AS trial_type,
        COALESCE(combined_campaigns_with_activity_type.activity_type,inbound_forms_high.activity_type,subscription.activity_type,fill_out_li_form.activity_type,visit_key_page.activity_type,visit_low_page.activity_type,visit_multi_pages.activity_type,email_bounced.activity_type,email_unsubscribe.activity_type,lead_score_decay.activity_type,trial_self_managed_default.activity_type,trial_saas_default.activity_type,trial_self_managed_personal.activity_type,trial_saas_personal.activity_type,trial_self_managed_business.activity_type,trial_saas_business.activity_type,filled_out_form_general.activity_type,change_nurture_track.activity_type,change_score.activity_type,click_link.activity_type,email_delivered.activity_type,email_opened.activity_type,push_lead_to_marketo.activity_type,email_sent.activity_type,push_lead_to_sfdc.activity_type) AS activity_type,
        COALESCE(inbound_forms_high.in_current_scoring_model,subscription.in_current_scoring_model,fill_out_li_form.in_current_scoring_model,visit_key_page.in_current_scoring_model,visit_low_page.in_current_scoring_model,visit_multi_pages.in_current_scoring_model,email_bounced.in_current_scoring_model,email_unsubscribe.in_current_scoring_model,lead_score_decay.in_current_scoring_model,trial_self_managed_default.in_current_scoring_model,trial_saas_default.in_current_scoring_model,trial_self_managed_personal.in_current_scoring_model,trial_saas_personal.in_current_scoring_model,trial_self_managed_business.in_current_scoring_model,trial_saas_business.in_current_scoring_model,filled_out_form_general.in_current_scoring_model,change_nurture_track.in_current_scoring_model,change_score.in_current_scoring_model,click_link.in_current_scoring_model,email_delivered.in_current_scoring_model,email_opened.in_current_scoring_model,push_lead_to_marketo.in_current_scoring_model,email_sent.in_current_scoring_model,push_lead_to_sfdc.in_current_scoring_model) AS in_current_scoring_model,
        COALESCE(inbound_forms_high.scored_action,subscription.scored_action,fill_out_li_form.scored_action,visit_key_page.scored_action,visit_low_page.scored_action,visit_multi_pages.scored_action,email_bounced.scored_action,email_unsubscribe.scored_action,lead_score_decay.scored_action,trial_self_managed_default.scored_action,trial_saas_default.scored_action,trial_self_managed_personal.scored_action,trial_saas_personal.scored_action,trial_self_managed_business.scored_action,trial_saas_business.scored_action,filled_out_form_general.scored_action,change_nurture_track.scored_action,change_score.scored_action,click_link.scored_action,email_delivered.scored_action,email_opened.scored_action,push_lead_to_marketo.scored_action,email_sent.scored_action,push_lead_to_sfdc.scored_action) AS scored_action
    FROM activites_date_final
    LEFT JOIN combined_campaigns_with_activity_type
        ON activites_date_final.dim_marketo_person_id=combined_campaigns_with_activity_type.dim_marketo_person_id
            AND activites_date_final.activity_datetime=combined_campaigns_with_activity_type.activity_datetime
    LEFT JOIN inbound_forms_high
        ON activites_date_final.dim_marketo_person_id=inbound_forms_high.dim_marketo_person_id
            AND activites_date_final.activity_datetime=inbound_forms_high.activity_datetime
    LEFT JOIN subscription
        ON activites_date_final.dim_marketo_person_id=subscription.dim_marketo_person_id
            AND activites_date_final.activity_datetime=subscription.activity_datetime
    LEFT JOIN fill_out_li_form
        ON activites_date_final.dim_marketo_person_id=fill_out_li_form.dim_marketo_person_id
            AND activites_date_final.activity_datetime=fill_out_li_form.activity_datetime
    LEFT JOIN visit_key_page
        ON activites_date_final.dim_marketo_person_id=visit_key_page.dim_marketo_person_id
            AND activites_date_final.activity_datetime=visit_key_page.activity_datetime
    LEFT JOIN visit_low_page
        ON activites_date_final.dim_marketo_person_id=visit_low_page.dim_marketo_person_id
            AND activites_date_final.activity_datetime=visit_low_page.activity_datetime
    LEFT JOIN visit_multi_pages
        ON activites_date_final.dim_marketo_person_id=visit_multi_pages.dim_marketo_person_id
            AND activites_date_final.activity_datetime=visit_multi_pages.activity_datetime
    LEFT JOIN email_bounced
        ON activites_date_final.dim_marketo_person_id=email_bounced.dim_marketo_person_id
            AND activites_date_final.activity_datetime=email_bounced.activity_datetime
    LEFT JOIN email_unsubscribe
        ON activites_date_final.dim_marketo_person_id=email_unsubscribe.dim_marketo_person_id
            AND activites_date_final.activity_datetime=email_unsubscribe.activity_datetime
    LEFT JOIN lead_score_decay
        ON activites_date_final.dim_marketo_person_id=lead_score_decay.dim_marketo_person_id
            AND activites_date_final.activity_datetime=lead_score_decay.activity_datetime
    LEFT JOIN trial_self_managed_default
        ON activites_date_final.dim_marketo_person_id=trial_self_managed_default.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_self_managed_default.activity_datetime
    LEFT JOIN trial_saas_default
        ON activites_date_final.dim_marketo_person_id=trial_saas_default.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_saas_default.activity_datetime
    LEFT JOIN trial_self_managed_personal
        ON activites_date_final.dim_marketo_person_id=trial_self_managed_personal.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_self_managed_personal.activity_datetime
    LEFT JOIN trial_saas_personal
        ON activites_date_final.dim_marketo_person_id=trial_saas_personal.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_saas_personal.activity_datetime
    LEFT JOIN trial_self_managed_business
        ON activites_date_final.dim_marketo_person_id=trial_self_managed_business.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_self_managed_business.activity_datetime
    LEFT JOIN trial_saas_business
        ON activites_date_final.dim_marketo_person_id=trial_saas_business.dim_marketo_person_id
            AND activites_date_final.activity_datetime=trial_saas_business.activity_datetime
    LEFT JOIN filled_out_form_general
        ON activites_date_final.dim_marketo_person_id=filled_out_form_general.dim_marketo_person_id
            AND activites_date_final.activity_datetime=filled_out_form_general.activity_datetime
    LEFT JOIN add_to_nurture
        ON activites_date_final.dim_marketo_person_id=add_to_nurture.dim_marketo_person_id
            AND activites_date_final.activity_datetime=add_to_nurture.activity_datetime
    LEFT JOIN change_nurture_track
        ON activites_date_final.dim_marketo_person_id=change_nurture_track.dim_marketo_person_id
            AND activites_date_final.activity_datetime=change_nurture_track.activity_datetime
    LEFT JOIN change_score
        ON activites_date_final.dim_marketo_person_id=change_score.dim_marketo_person_id
            AND activites_date_final.activity_datetime=change_score.activity_datetime
    LEFT JOIN click_link
        ON activites_date_final.dim_marketo_person_id=click_link.dim_marketo_person_id
            AND activites_date_final.activity_datetime=click_link.activity_datetime
    LEFT JOIN email_delivered
        ON activites_date_final.dim_marketo_person_id=email_delivered.dim_marketo_person_id
            AND activites_date_final.activity_datetime=email_delivered.activity_datetime
    LEFT JOIN email_opened
        ON activites_date_final.dim_marketo_person_id=email_opened.dim_marketo_person_id
            AND activites_date_final.activity_datetime=email_opened.activity_datetime
    LEFT JOIN push_lead_to_marketo
        ON activites_date_final.dim_marketo_person_id=push_lead_to_marketo.dim_marketo_person_id
            AND activites_date_final.activity_datetime=push_lead_to_marketo.activity_datetime
    LEFT JOIN email_sent
        ON activites_date_final.dim_marketo_person_id=email_sent.dim_marketo_person_id
            AND activites_date_final.activity_datetime=email_sent.activity_datetime
    LEFT JOIN push_lead_to_sfdc
        ON activites_date_final.dim_marketo_person_id=push_lead_to_sfdc.dim_marketo_person_id
            AND activites_date_final.activity_datetime=push_lead_to_sfdc.activity_datetime

), final AS (

    SELECT DISTINCT
        CONCAT(dim_marketo_person_id,',',activity_datetime,',',activity_type,',',scored_action) AS unique_id,
        activities_final.*
    FROM activities_final
    WHERE dim_marketo_person_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-09-18",
    updated_date="2024-09-19"
) }}
