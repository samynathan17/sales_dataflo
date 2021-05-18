-- depends_on: {{ ref('Dim_TimeFrame') }}
-- depends_on: {{ ref('Dim_Employee') }}
-- depends_on: {{ ref('Dim_Metrics') }}
-- depends_on: {{ ref('Dim_Opportunity') }}
-- depends_on: {{ ref('Dim_Lead') }}
-- depends_on: {{ ref('Dim_Account') }}
-- depends_on: {{ ref('Dim_Contact') }}
-- depends_on: {{ ref('Dim_Engagement') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE in ('SF','HS')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE):: DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
    ) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}

    {% if  entity_type  == 'SF'  %} 
        {% if  hist_load  == 'true'  %} 

            {%- for metrics in [(run_metrics_hist_sales(entity_name,'upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity','CLOSE_DATE',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '10', '1','Dim_Opportunity','CLOSE_DATE',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(lead_to_opp_flag) = '"'"'TRUE'"'"'', '3', '4','Dim_Lead','lead_CONVERTED_DATE',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'1 = 1', '4', '4','Dim_Lead','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '23', '2','Dim_Opportunity','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'1 = 1', '27', '5','Dim_Account','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'1 = 1', '29', '6','Dim_Contact','initial_create_dt',hist_strt_dt,hist_end_dt)),                    
                            ]  %}
                    (
                        {{ metrics }} 
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%} 
        {% else -%}     
            {%- for metrics in [(run_metrics_sales(entity_name,'upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity','CLOSE_DATE')),
                                (run_metrics_sales(entity_name,'upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '10', '1','Dim_Opportunity','CLOSE_DATE')),
                                (run_metrics_sales(entity_name,'upper(lead_to_opp_flag) = '"'"'TRUE'"'"'', '3', '4','Dim_Lead','initial_create_dt')),
                                (run_metrics_sales(entity_name,'1 = 1', '4', '4','Dim_Lead','initial_create_dt')),
                                (run_metrics_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '23', '2','Dim_Opportunity','initial_create_dt')),
                                (run_metrics_sales(entity_name,'1 = 1', '27', '5','Dim_Account','initial_create_dt')),
                                (run_metrics_sales(entity_name,'1 = 1', '29', '6','Dim_Contact','initial_create_dt')),                 
                            ]  %}
                    (
                        {{ metrics }}
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%}
        {% endif -%}  
    {% elif  entity_type  == 'HS'  %} 
        {% if  hist_load  == 'true'  %} 

            {%- for metrics in [(run_metrics_hist_sales(entity_name,'upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity','CLOSE_DATE',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '10', '1','Dim_Opportunity','CLOSE_DATE',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '23', '2','Dim_Opportunity','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '79', '1','Dim_Opportunity','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(TYPE) = '"'"'CALL'"'"'', '39', '1','Dim_Engagement','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(TYPE) = '"'"'TASK'"'"'', '89', '1','Dim_Engagement','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(TYPE) = '"'"'MEETING'"'"'', '71', '1','Dim_Engagement','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(TYPE) = '"'"'NOTE'"'"'', '76', '1','Dim_Engagement','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                (run_metrics_hist_sales(entity_name,'upper(TYPE) = '"'"'EMAIL'"'"'', '66', '1','Dim_Engagement','initial_create_dt',hist_strt_dt,hist_end_dt)),
                               ]  %}
                    (
                        {{ metrics }}
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%} 
        {% else -%}     
            {%- for metrics in [(run_metrics_sales(entity_name,'upper(IS_WON) = '"'"'TRUE'"'"'', '1', '1','Dim_Opportunity','CLOSE_DATE')),
                                (run_metrics_sales(entity_name,'upper(IS_WON) = '"'"'FALSE'"'"' and upper(IS_CLOSED) = '"'"'TRUE'"'"'', '10', '1','Dim_Opportunity','CLOSE_DATE')),
                                (run_metrics_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '23', '2','Dim_Opportunity','initial_create_dt')) ,                   
                                (run_metrics_hist_sales(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '79', '1','Dim_Opportunity','initial_create_dt',hist_strt_dt,hist_end_dt)),
                                ]  %}
                    (
                        {{ metrics }}
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%}
        {% endif -%}        
    {% endif -%}   
        {% if not loop.last -%}
            union all
        {% endif -%}     
{%- endfor -%} 