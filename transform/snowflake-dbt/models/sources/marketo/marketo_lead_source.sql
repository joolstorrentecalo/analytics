WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'lead') }}

), renamed AS (

    SELECT
      --Primary Key
      id::FLOAT                                 AS marketo_lead_id,

      --Info
      email::VARCHAR                            AS email,
      {{ hash_of_column('EMAIL') }}
      sfdc_lead_id::VARCHAR                     AS sfdc_lead_id,
      sfdc_contact_id::VARCHAR                  AS sfdc_contact_id,
      first_name::VARCHAR                       AS first_name,
      last_name::VARCHAR                        AS last_name,
      company::VARCHAR                          AS company_name,
      title::VARCHAR                            AS job_title,
      {{it_job_title_hierarchy('job_title')}},
      country::VARCHAR                          AS country,
      mobile_phone::VARCHAR                     AS mobile_phone,
      sfdc_type::VARCHAR                        AS sfdc_type,
      inactive_lead_c::BOOLEAN                  AS is_lead_inactive,
      inactive_contact_c::BOOLEAN               AS is_contact_inactive,
      sales_segmentation_c::VARCHAR             AS sales_segmentation,
      is_email_bounced::BOOLEAN                 AS is_email_bounced,
      email_bounced_date::DATE                  AS email_bounced_date,
      unsubscribed::BOOLEAN                     AS is_unsubscribed,
      opt_in::BOOLEAN                           AS is_opt_in,
      compliance_segment_value::VARCHAR         AS compliance_segment_value,
      pql_product_qualified_lead_c::BOOLEAN     AS is_pql_marketo,
      cdbispaidtier_c::BOOLEAN                  AS is_paid_tier_marketo,
      ptpt_is_contact_c::BOOLEAN                AS is_ptpt_contact_marketo,
      ptp_is_ptp_contact_c::BOOLEAN             AS is_ptp_contact_marketo,
      cdb_impacted_by_user_limit_c::BOOLEAN     AS is_impacted_by_user_limit_marketo,
      currently_in_trial_c::BOOLEAN             AS is_currently_in_trial_marketo,
      trial_start_date_c::DATE                  AS trial_start_date_marketo,
      trial_end_date_c::DATE                    AS trial_end_date_marketo,
      updated_at::TIMESTAMP                     AS updated_at,
      * exclude(id,email,sfdc_lead_id,sfdc_contact_id,first_name,last_name,company,title,country,mobile_phone,sfdc_type,inactive_lead_c,inactive_contact_c,sales_segmentation_c,is_email_bounced,email_bounced_date,unsubscribed,opt_in,compliance_segment_value,pql_product_qualified_lead_c,cdbispaidtier_c,ptpt_is_contact_c,ptp_is_ptp_contact_c,cdb_impacted_by_user_limit_c,currently_in_trial_c,trial_start_date_c,trial_end_date_c,updated_at)

    FROM source
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1

)

SELECT *
FROM renamed
