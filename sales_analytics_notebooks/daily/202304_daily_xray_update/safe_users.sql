SELECT DISTINCT
    user.name,
    safe.user_email
FROM prep.tableau_cloud.tableau_cloud_groups_source AS safe
LEFT JOIN prod.workspace_sales.sfdc_users_xf AS user
    ON safe.user_email = user.user_email
WHERE
    group_name LIKE '%SAFE%'
    AND safe.user_email NOT LIKE '%-ext%'
