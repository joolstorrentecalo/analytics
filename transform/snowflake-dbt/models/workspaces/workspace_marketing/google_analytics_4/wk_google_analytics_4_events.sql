{{ config({
    "materialized": "view",
    })
}}

SELECT *
FROM {{ ref('google_analytics_4_events_source') }}
