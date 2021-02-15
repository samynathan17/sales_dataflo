{{ config(
    materialized="table"
) 
}}
{%- for metrics in [(run_metrics_history('upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity')),
                    (run_metrics_history('upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '10', '1','Dim_Opportunity')),
                    (run_metrics_history('upper(lead_to_opp_flag) = '"'"'TRUE'"'"'', '3', '4','Dim_Lead')),
                    (run_metrics_history('1 = 1', '4', '4','Dim_Lead')),
                    (run_metrics_history('upper(IS_CLOSED) = '"'"'FALSE'"'"'', '23', '2','Dim_Opportunity')),
                    (run_metrics_history('1 = 1', '27', '5','Dim_Account')),
                    (run_metrics_history('1 = 1', '29', '6','Dim_Contact'))                    
                   ]  %}
        (
             {{ metrics }}
        )

        {% if not loop.last -%}
            union all
        {% endif -%}
{%- endfor -%} 