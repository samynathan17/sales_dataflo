{{ config(
    materialized="table"
) 
}}
{%- for metrics in [(run_metrics_history('upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity'))                 
                   ]  %}
        (
             {{ metrics }}
        )

        {% if not loop.last -%}
            union all
        {% endif -%}
{%- endfor -%} 