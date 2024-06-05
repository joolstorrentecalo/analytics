{%- macro dedupe_aws_source(source_table, source_schema='aws_billing', condition_column='metadata$file_last_modified', unique_key='id') -%}

{% set source_table_name = source_table %}
{% set source_schema_name = source_schema %}
{% set unique_key_column = unique_key %}

{{ config.set('materialized', 'incremental') }}
{{ config.set('unique_key', unique_key_column) }}
SELECT *
FROM (
SELECT *,
    metadata$file_last_modified AS modified_at_,
    ROW_NUMBER() OVER (PARTITION BY {{ unique_key }} ORDER BY {{ condition_column }} DESC) AS rn
FROM {{ source(source_schema_name, source_table_name) }}
{% if is_incremental() %}

WHERE {{ condition_column }} > (SELECT MAX(modified_at) FROM {{this}})

{% endif %}
) subquery
WHERE rn = 1

{%- endmacro -%}

