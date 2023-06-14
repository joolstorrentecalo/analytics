{{ config(
    materialized="incremental",
    unique_key="id",
    on_schema_change='append_new_columns'
    )
}}

WITH source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at_gcs' from the parquet filename, i.e:
    -- 'entity_data/20230502/20230502T000025-entity_data-from-19700101T000000-until-20230426T193740.parquet'
    TO_TIMESTAMP(
      SPLIT_PART(
        SPLIT_PART(
          metadata$filename,
          '/',
          3
        ),
        '-',
        1
      ),
      'YYYYMMDDTHH24MISS'
    )                 AS uploaded_at_gcs

  FROM
    {{ source('omamori', 'mitigation_plans_external') }}

  {% if is_incremental() %}
    -- Filter only for records from new files based off uploaded_at_gcs
    -- but first filter on the parititioned column (date_part) to speed up query
    WHERE date_part >= (SELECT COALESCE(MAX(uploaded_at_gcs)::DATE, '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
      AND uploaded_at_gcs > (SELECT COALESCE(MAX(uploaded_at_gcs), '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
  {% endif %}
),

renamed AS (
  SELECT
    json_value['id']::INT                                           AS id,
    json_value['entity_type']::VARCHAR                              AS entity_type,
    json_value['priority']::VARCHAR                                 AS priority,
    json_value['status']::VARCHAR                                   AS status,
    json_value['feature']::VARCHAR                                  AS feature,
    json_value['category']::VARCHAR                                 AS category,
    json_value['last_applied_mitigation_plan_template_id']::INT     AS last_applied_mitigation_plan_template_id,
    json_value['from_rule_evaluation_id']::INT                      AS from_rule_evaluation_id,
    (json_value['created_at'] / 1000000)::NUMBER(36, 3)::TIMESTAMP  AS created_at,
    json_value['created_by_id']::INT                                AS created_by_id,
    (json_value['reviewed_at'] / 1000000)::NUMBER(36, 3)::TIMESTAMP AS reviwed_at,
    json_value['reviewed_by_id']::INT                               AS reviewed_by_id,
    (json_value['executed_at'] / 1000000)::NUMBER(36, 3)::TIMESTAMP AS executed_at,
    json_value['executed_by_id']::INT                               AS executed_by_id,
    (json_value['updated_at'] / 1000000)::NUMBER(36, 3)::TIMESTAMP  AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
