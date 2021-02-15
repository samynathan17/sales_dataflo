{{ config(
    materialized="table"
) 
}}
{%- for metrics in [(fact_table("to_date('01/01/2017', 'dd/mm/yyyy')","to_date('31/12/2018', 'dd/mm/yyyy')"))                
                   ]  %}
        (
             {{ metrics }}
        )

        {% if not loop.last -%}
            union all
        {% endif -%}
{%- endfor -%} 
