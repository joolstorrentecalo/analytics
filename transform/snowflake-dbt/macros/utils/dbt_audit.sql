{%- macro dbt_audit(cte_ref, created_by, updated_by, created_date, updated_date) -%}

    -- First iteration on removing these fields, as per #19387
    SELECT
      *,
      NULL       AS created_by,
      NULL       AS updated_by,
      NULL       AS model_created_date,
      NULL       AS model_updated_date,
      NULL       AS dbt_updated_at

    FROM {{ cte_ref }}

{%- endmacro -%}
