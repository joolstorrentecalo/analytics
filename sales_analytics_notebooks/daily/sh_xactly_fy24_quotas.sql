WITH date_details AS (

    SELECT *
    FROM prod.workspace_sales.date_details

), xc_credit AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_credit --raw.tap_xactly.xc_credit

), xc_quota_assignment AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_quota_assignment 

), xc_quota AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_quota --raw.tap_xactly.xc_quota

), xc_period AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_period --raw.tap_xactly.xc_period

), xc_participant AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_participant
    WHERE is_master = 1
    AND is_active = 1

), xc_position AS (

    SELECT *
    FROM prod.restricted_safe_workspace_sales.xactly_position
    WHERE is_master = 1
    AND is_active = 1

), xc_position_part AS (

    SELECT *,
    RANK() OVER (PARTITION BY position_name_hash ORDER BY modified_date DESC) rank
    FROM prod.restricted_safe_workspace_sales.xactly_pos_part_assignment
    QUALIFY rank = 1

), sfdc_users AS (

    SELECT *,
        CASE
            WHEN lower(title) like '%strategic account%' AND dateadd(month,10,start_date) < '2022-08-15'
              THEN 'Ramped'
            WHEN lower(title) like '%account executive%mid market%' AND dateadd(month,6,start_date) < '2022-06-15'
              THEN 'Ramped'
            WHEN lower(title) like '%smb account executive%' AND dateadd(month,3,start_date) < '2022-05-15'
              THEN 'Ramped'
            ELSE 'Ramping'
        END AS ramp_status,
        LOWER(business_unit  || '_' || sub_business_unit || '_' ||
            division || '_' || asm)                                     AS key_bu_subbu_division_asm,
        LOWER(business_unit  || '_' || sub_business_unit )              AS key_bu_subbu
    FROM prod.workspace_sales.sfdc_users_xf
    WHERE employee_number IS NOT NULL
      AND is_active = 1

), eligible_periods AS (

    SELECT
        xc_period.period_id,
        date_details.fiscal_quarter_name_fy         AS fiscal_quarter_name,
        date_details.first_day_of_fiscal_quarter    AS fiscal_quarter_date,
        date_details.fiscal_year,
    CASE
        WHEN xc_period.period_type_id_fk = 1930693887
            THEN date_details.fiscal_quarter_name_fy
        WHEN xc_period.period_type_id_fk =1930693885
            THEN CAST(fiscal_year AS STRING)
        ELSE NULL
    END                                             AS period_name

    FROM xc_period
        LEFT JOIN date_details
            ON xc_period.start_date = date_details.date_actual
    WHERE date_details.fiscal_year >= 2023
        AND ( xc_period.period_type_id_fk = 1930693887 -- quarterly period type
        OR xc_period.period_type_id_fk =1930693885) --yearly period type

), consolidated AS (

    SELECT
        sfdc_users.name,
        sfdc_users.user_id,
        xc_participant.employee_id,
        xc_participant.bhr_eeid,
        xc_participant.start_date_in_role,
        xc_quota_assignment.amount,
        eligible_periods.period_name

    FROM xc_quota_assignment
    INNER JOIN eligible_periods
        ON   eligible_periods.period_id = xc_quota_assignment.period_id
    INNER JOIN xc_quota
        ON xc_quota_assignment.quota_id = xc_quota.quota_id
    INNER JOIN xc_position_part
        ON   xc_position_part.position_name_hash = xc_quota_assignment.assignment_name_hash
    INNER JOIN xc_participant
        ON xc_participant.participant_id = xc_position_part.participant_id
    INNER JOIN sfdc_users
        ON sfdc_users.employee_number = xc_participant.employee_id
    WHERE xc_quota.name = 'ARR'
        AND (LOWER(sfdc_users.role_name) LIKE '%sal-ent%'
        OR LOWER(sfdc_users.role_name) LIKE '%strategic account leader-public sector%'-- filtering for ARR quotas only
        OR LOWER(title) LIKE '%account executive%')

), pivoted_quotas AS (

    SELECT *
    FROM consolidated
    PIVOT(SUM(amount) FOR period_name IN ('FY24-Q1', 'FY24-Q2', 'FY24-Q3', 'FY24-Q4','2024'))
        AS p (  name,
                user_id,
                employee_id,
                bhr_eeid,
                start_date, fy24_q1, fy24_q2, fy24_q3, fy24_q4,fy24)
                order by name,
                user_id,
                employee_id,
                bhr_eeid,
                start_date

), final AS (


SELECT

    sfdc_users.role_type AS user_role_type,

    LOWER(sfdc_users.key_bu_subbu_division_asm
                || '_' || sfdc_users.role_type || '_' || TO_VARCHAR(sfdc_users.employee_number))   AS sal_heatmap_key,
    LOWER(sfdc_users.key_bu_subbu )                                 AS sal_region_key,

    lower(sfdc_users.business_unit)          AS business_unit,
    lower(sfdc_users.sub_business_unit)      AS sub_business_unit,
    lower(sfdc_users.division)               AS division,
    CONCAT(sal_region_key,'_',lower(sfdc_users.asm))     AS user_area_key,
    sfdc_users.asm         AS user_area_name,
    CASE
        WHEN sfdc_users.is_active = TRUE
            THEN 1
        ELSE 0
    END AS is_active,
    --
    CASE
        WHEN lower(sfdc_users.title) like '%strategic account%' AND dateadd(month,10,pivoted_quotas.start_date) < '2022-08-15'
          THEN 'Ramped'
        WHEN lower(sfdc_users.title) like '%account executive%mid market%' AND dateadd(month,6,pivoted_quotas.start_date) < '2022-06-15'
          THEN 'Ramped'
        WHEN lower(sfdc_users.title) like '%smb account executive%' AND dateadd(month,3,pivoted_quotas.start_date) < '2022-05-15'
          THEN 'Ramped'
        ELSE 'Ramping'
    END AS ramp_status,
    pivoted_quotas.name,
  pivoted_quotas.user_id,
  pivoted_quotas.employee_id,
  pivoted_quotas.bhr_eeid,
  pivoted_quotas.start_date,
  pivoted_quotas.fy24_q1,
  pivoted_quotas.fy24_q2,
  pivoted_quotas.fy24_q3,
  pivoted_quotas.fy24_q4,
  pivoted_quotas.fy24_q1 + pivoted_quotas.fy24_q2 + pivoted_quotas.fy24_q3 + pivoted_quotas.fy24_q4 AS fy24
FROM pivoted_quotas
INNER JOIN sfdc_users
  ON pivoted_quotas.user_id = sfdc_users.user_id
)

SELECT *
FROM final