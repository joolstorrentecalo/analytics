{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_pipeline_artifacts') }}
{% if is_incremental() %}

  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
