{{ config(
    tags=["mnpi_exception"]
) }}

WITH seat_link AS (

    SELECT
      *
    FROM {{ ref('customers_db_license_seat_links_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hostname, uuid ORDER BY report_date DESC, updated_at DESC) = 1

)

{{ dbt_audit(
    cte_ref="seat_link",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2024-03-06",
    updated_date="2024-03-06"
) }}