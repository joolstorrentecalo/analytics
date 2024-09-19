WITH source AS (

    SELECT *
    FROM {{ source('gitlab_data_yaml', 'cloud_connector') }}

), renamed AS (
    SELECT DISTINCT
      f.key                                                                       AS environment_name,
      f1.value['backend']::VARCHAR                                                AS backend,
      f1.key                                                                      AS feature_name,
      f2.key                                                                      AS bundled_with_add_on_name,
      f3.value::VARCHAR                                                           AS unit_primitive_name,
      f1.value['cut_off_date']::VARCHAR                                           AS cut_off_date,
      f1.value['min_gitlab_version']::VARCHAR                                     AS min_gitlab_version,
      f1.value['min_gitlab_version_for_free_access']::VARCHAR                     AS min_gitlab_version_for_free_access
    FROM source,
    LATERAL FLATTEN(input => jsontext) f,
    LATERAL FLATTEN(input => f.value['services']) f1,
    LATERAL FLATTEN(input => f1.value['bundled_with']) f2,
    LATERAL FLATTEN(input => f2.value['unit_primitives']) f3
    ORDER BY environment_name, feature_name, bundled_with_add_on_name, unit_primitive_name

)

SELECT *
FROM renamed