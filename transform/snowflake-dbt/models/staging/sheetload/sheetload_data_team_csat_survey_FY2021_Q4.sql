WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_data_team_csat_survey_FY2021_Q4') }}

)

SELECT *
FROM source

