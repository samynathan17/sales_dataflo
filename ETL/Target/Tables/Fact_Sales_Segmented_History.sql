{{ config(
    materialized="table"
) 
}}
{%- for metrics in [(run_metrics_segment_history('upper(IS_CLOSED) = '"'"'FALSE'"'"'', '5', '2','Dim_Opportunity','STAGE_NAME')),
                    (run_metrics_segment_history('1 = 1', '7', '3','Dim_Lead','INDUSTRY')),
                    (run_metrics_segment_history('upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '11', '2','Dim_Opportunity','EMPLOYEE_ID')),
                    (run_metrics_segment_history('upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '14', '1','Dim_Opportunity','OPPORTUNITY_NAME')),
                    (run_metrics_segment_history('1 = 1', '18', '3','Dim_Lead','LEAD_SOURCE')),
					(run_metrics_segment_history('1 = 1', '19', '3','Dim_Lead','STATUS')),
                    (run_metrics_segment_history('upper(IS_WON) = '"'"'TRUE'"'"'', '22', '1','Dim_Opportunity','OPPORTUNITY_NAME')),
					(run_metrics_segment_history('upper(IS_CLOSED) = '"'"'FALSE'"'"'', '26', '1','Dim_Opportunity','OPPORTUNITY_NAME')),
                    (run_metrics_segment_history('1 = 1', '28', '5','Dim_Account','EMPLOYEE_ID')),
					(run_metrics_segment_history('1 = 1', '30', '3','Dim_Lead','EMPLOYEE_ID')),
                    (run_metrics_segment_history('1 = 1', '31', '3','Dim_Lead','LEAD_CONTACT_ADDRESS')),
					(run_metrics_segment_history('1 = 1', '32', '2','Dim_Opportunity','OPPORTUNITY_TYPE'))                                       
                   ]  %}
        (
             {{ metrics }}
        )

        {% if not loop.last -%}
            union all
        {% endif -%}
{%- endfor -%} 