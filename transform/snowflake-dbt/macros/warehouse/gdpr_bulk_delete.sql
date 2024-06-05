{% macro gdpr_bulk_delete() %}

{%- call statement('gdpr_deletions', fetch_result=True) %}
    select SHA2(TRIM(LOWER(email_address)))
    from {{ source('driveload', 'gdpr_delete_requests') }}
{%- endcall -%}

    {%- set value_list = load_result('gdpr_deletions') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}

        {{ gdpr_delete(data_row[0], run_queries='True')}}
        {%- call statement('remove_data', fetch_result=True) %}
            DELETE FROM {{ source('driveload', 'gdpr_delete_requests') }}
            WHERE  SHA2(TRIM(LOWER(email_address))) = '{{data_row[0]}}'
        {%- endcall -%}

      {% endfor %}
    {%- endif -%}

{%- endmacro -%}
