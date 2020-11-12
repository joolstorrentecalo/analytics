WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_territory') }}

)

SELECT
  KPI_Name::VARCHAR                       AS kpi_name,
  Sales_Territory::VARCHAR                AS sales_territory,
  Target::FLOAT                           AS target,
  Percent_Curve::VARCHAR                  AS percent_curve,
  _updated_at::FLOAT                      AS last_updated_at
FROM source
