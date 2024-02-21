{{ config(materialized='table') }}

{{ simple_cte([
('dim_crm_user','dim_crm_user')]) 
}},

final AS (

SELECT
    dim_crm_user_id,
    user_name,
    user_email,
    user_role_name,
    CASE 
        WHEN user_role_name LIKE '%AMER%' THEN 'AMER'
        WHEN user_role_name LIKE '%EMEA%' THEN 'EMEA'
        WHEN user_role_name LIKE '%APJ%' THEN 'APJ'
        WHEN user_role_name LIKE '%APAC%' THEN 'APAC'
        WHEN user_role_name LIKE '%JAPAN%' THEN 'JAPAN'
        WHEN user_role_name LIKE '%PUBSEC%' THEN 'PUBSEC'
        WHEN user_role_name LIKE '%SMB%' THEN 'SMB'
    END AS user_geo
FROM dim_crm_user

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jonglee1218",
    updated_by="@jonglee1218",
    created_date="2024-02-21",
    updated_date="2024-02-21"
) }}