{% macro get_run_nodes(results) %}

  {% if execute %}

  {% call statement('get_tables', fetch_result=True) %}
    select
  fct_dbt__model_executions.node_id,
  fct_dbt__model_executions.run_started_at,
  was_full_refresh,
  total_node_runtime,
  rows_affected,
  materialization,
  fct_dbt__invocations.full_refresh_flag
from prod.workspace_data.fct_dbt__model_executions
LEFT JOIN prod.workspace_data.fct_dbt__invocations
  ON fct_dbt__model_executions.command_invocation_id = fct_dbt__invocations.command_invocation_id
WHERE fct_dbt__model_executions.run_started_at > DATEADD('day',-35,CURRENT_DATE)
AND fct_dbt__model_executions.status = 'success'
AND fct_dbt__model_executions.node_id in (
  {%- for res in results  %}
      '{{ res.node.unique_id }}'{% if not loop.last %}, {% endif %}        
      {%- endfor -%}
)
--and fct_dbt__invocations.full_refresh_flag
QUALIFY row_number() OVER (PARTITION BY fct_dbt__model_executions.node_id, fct_dbt__invocations.full_refresh_flag ORDER BY fct_dbt__model_executions.run_started_at DESC) = 1

  {% endcall %}

  {% set prod_models = load_result('get_tables')['data'] %}

  {{ log(prod_models, info=True) }}
  {% for to_check in prod_models %}
    {{ log(to_check[0], info=True) }}
  {% endfor %}

  {{ log("========== Begin Summary ==========", info=True) }}
  {% for res in results -%}
    {% for to_check in prod_models %}
      {% if to_check[0] == res.node.unique_id and to_check[6] == flags.FULL_REFRESH  %}
        {% set line -%}
            node: {{ res.node.unique_id }}; execution_time: {{ res.execution_time }} last_prod: {{ to_check[3] }}
        {%- endset %}
        {{ log(line, info=True) }}
      {% endif %}
    {% endfor %}

    
  {% endfor %}
  {{ log("========== End Summary ==========", info=True) }}
  {% endif %}

{% endmacro %}