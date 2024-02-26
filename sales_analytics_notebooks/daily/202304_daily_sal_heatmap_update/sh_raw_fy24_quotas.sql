WITH sfdc_users AS (

    SELECT *
    FROM prod.workspace_sales.sfdc_users_xf
    WHERE
        employee_number IS NOT NULL
        AND is_active = 1

),

quotas AS (

    SELECT
        *,
        CASE
            WHEN hc_type = 'SAL' THEN dateadd('month', 9, start_date)
            WHEN hc_type LIKE 'MM AE%' THEN dateadd('month', 6, start_date)
            WHEN hc_type LIKE 'SMB AE%' THEN dateadd('month', 3, start_date)
        END AS ramped_month,

        -- Static logic is used to keep reps that were assigned to a ramping schedule
        -- in the same group and avoid comparing them, performance wise, with reps that are
        -- already ramped.
        CASE
            WHEN ramped_month <= '2023-08-15' THEN 'Ramped'
            ELSE 'Ramping'
        END AS ramp_status_static,
        CASE
            WHEN ramped_month <= current_date THEN 'Ramped'
            ELSE 'Ramping'
        END AS ramp_status_dynamic
    FROM raw.sales_analytics.ae_quotas_unpivoted
    WHERE hc_type IN ('SAL', 'SAL - FO', 'MM AE', 'MM AE - FO', 'SMB AE')

),

final AS (


    SELECT
        sfdc_users.user_id,
        quotas.employee_id,
        quotas.name                                                   AS sfdc_name,
        quotas.start_date,
        sfdc_users.role_type                                          AS role_type,

        lower(
            sfdc_users.key_bu_subbu_division_asm
            || '_' || sfdc_users.role_type || '_' || to_varchar(sfdc_users.employee_number)
        )                                                             AS sal_heatmap_key,
        lower(sfdc_users.key_bu_subbu)                                AS sal_region_key,
        lower(sfdc_users.business_unit)                               AS business_unit,
        lower(sfdc_users.sub_business_unit)                           AS sub_business_unit,
        lower(sfdc_users.division)                                    AS division,
        lower(sfdc_users.asm)                                         AS asm,

        ramp_status_dynamic                                           AS ramp_status,

        quotas.cfy_q1,
        quotas.cfy_q2,
        quotas.cfy_q3,
        quotas.cfy_q4,


        quotas.cfy_q1 + quotas.cfy_q2 + quotas.cfy_q3 + quotas.cfy_q4 AS cfy_total,

        quotas.nfy_q1,
        quotas.nfy_q2,
        quotas.nfy_q3,
        quotas.nfy_q4,

        quotas.nfy_q1 + quotas.nfy_q2 + quotas.nfy_q3 + quotas.nfy_q4 AS nfy_total,
        quotas.cfy_ytd_target,
        quotas.cfy_ytd_closed,
        quotas.cfy_ytd_attainment,
        quotas.fy23_quota,
        quotas.fy23_credits,
        quotas.fy23_attainment,
        quotas.fy22_quota,
        quotas.fy22_credits,
        quotas.fy22_attainment
    FROM quotas
    INNER JOIN sfdc_users
        ON quotas.employee_id = sfdc_users.employee_number
)

SELECT *
FROM final
