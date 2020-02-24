{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests_count AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests_count') }}

), handbook_engineering_total_count_department AS (

    SELECT
        DATE_TRUNC('MONTH', merge_request_merged_at)    AS month_merged_at,
        SUM(mr_count_engineering)                       AS is_mr_engineering,
        SUM(mr_count_ux)                                AS is_mr_ux,
        SUM(mr_count_security)                          AS is_mr_security,
        SUM(mr_count_infrastructure)                    AS is_mr_infrastructure,
        SUM(mr_count_development)                       AS is_mr_development,
        SUM(mr_count_quality)                           AS is_mr_quality,
        SUM(mr_count_support)                           AS is_mr_support
    FROM category_handbook_engineering_merge_requests_count
    WHERE merge_request_state = 'merged'
    GROUP BY 1

)

SELECT *
FROM handbook_engineering_total_count_department
