{{ config(
    tags=["mnpi_exception"]
) }}
WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_attendee') }}
)
select *
from base
