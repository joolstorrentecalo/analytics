{%- macro dedupe_and_union_aws_source(source_tables, schema='aws_billing', condition_column='metadata$file_last_modified', unique_key='id') -%}

{% set all_raw_sql %}
    {% for source_table in source_tables %}
            {{ dedupe_aws_source(source_table, schema, condition_column, unique_key) }}
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endset %}

{{ return(all_raw_sql) }}
{%- endmacro -%}