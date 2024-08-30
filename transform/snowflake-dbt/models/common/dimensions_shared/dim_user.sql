{{ config({
    "tags": ["product"],
    "post-hook": "{{ missing_member_column(primary_key = 'dim_user_sk', not_null_test_cols = ['dim_user_id', 'user_id']) }}"
}) }}

WITH prep_user AS (

    SELECT 
      --surrogate_key
      dim_user_sk,
      
      --natural_key
      user_id,
      
      --legacy natural_key to be deprecated during change management plan
      dim_user_id,
      
      --Other attributes
      remember_created_at,
      sign_in_count,
      current_sign_in_at,
      last_sign_in_at,
      created_at,
      updated_at,
      is_admin,
      is_blocked_user,
      notification_email_domain,
      notification_email_domain_classification,
      email_domain,
      email_domain_classification,
      IFF(email_domain_classification IS NULL,TRUE,FALSE) AS is_valuable_signup,
      public_email_domain,
      public_email_domain_classification,
      commit_email_domain,
      commit_email_domain_classification,
      identity_provider,
      role,
      last_activity_date,
      last_sign_in_date,
      setup_for_company,
      jobs_to_be_done,
      for_business_use,
      employee_count,
      country,
      state,
      user_type_id,
      user_type,
      is_bot
 
    FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-06-28",
    updated_date="2024-07-31"
) }}
