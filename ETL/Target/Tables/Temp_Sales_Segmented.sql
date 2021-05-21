-- depends_on: {{ ref('Dim_TimeFrame') }}
-- depends_on: {{ ref('Dim_Employee') }}
-- depends_on: {{ ref('Dim_Metrics') }}
-- depends_on: {{ ref('Dim_Opportunity') }}
-- depends_on: {{ ref('Dim_Lead') }}
-- depends_on: {{ ref('Dim_Account') }}
-- depends_on: {{ ref('Dim_Contact') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE in ('SF','HS')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE):: DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
    ) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}

    {% if  entity_type  == 'SF'  %} 
        {% if  hist_load  == 'true'  %} 

            {%- for metrics in [(run_metrics_hist_sales_segment(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '5', '2','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    (run_metrics_hist_sales_segment(entity_name,'1 = 1', '7', '3','Dim_Lead','INDUSTRY','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    (run_metrics_hist_sales_segment(entity_name,'1 = 1', '18', '3','Dim_Lead','LEAD_SOURCE','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
					(run_metrics_hist_sales_segment(entity_name,'1 = 1', '19', '3','Dim_Lead','STATUS','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    (run_metrics_hist_sales_segment(entity_name,'1 = 1', '28', '5','Dim_Account','EMPLOYEE_ID','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
					(run_metrics_hist_sales_segment(entity_name,'1 = 1', '30', '3','Dim_Lead','EMPLOYEE_ID','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    (run_metrics_hist_sales_segment(entity_name,'1 = 1', '31', '3','Dim_Lead','LEAD_CONTACT_ADDRESS','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
					(run_metrics_hist_sales_segment(entity_name,'1 = 1', '32', '2','Dim_Opportunity','OPPORTUNITY_TYPE','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),                                       
                   ]  %}
                    (
                        {{ metrics }} as
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%} 
        {% else -%}     
            {%- for metrics in [(run_metrics_sales_segment(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '5', '2','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT')),
                    (run_metrics_sales_segment(entity_name,'1 = 1', '7', '3','Dim_Lead','INDUSTRY','INITIAL_CREATE_DT')),
                    (run_metrics_sales_segment(entity_name,'1 = 1', '18', '3','Dim_Lead','LEAD_SOURCE','INITIAL_CREATE_DT')),
					(run_metrics_sales_segment(entity_name,'1 = 1', '19', '3','Dim_Lead','STATUS','INITIAL_CREATE_DT')),
                    (run_metrics_sales_segment(entity_name,'1 = 1', '28', '5','Dim_Account','Account_Type','INITIAL_CREATE_DT')),
					(run_metrics_sales_segment(entity_name,'1 = 1', '30', '3','Dim_Lead','EMPLOYEE_ID','INITIAL_CREATE_DT')),
                    (run_metrics_sales_segment(entity_name,'1 = 1', '31', '3','Dim_Lead','LEAD_CONTACT_ADDRESS','INITIAL_CREATE_DT')),
					(run_metrics_sales_segment(entity_name,'1 = 1', '32', '2','Dim_Opportunity','OPPORTUNITY_TYPE','INITIAL_CREATE_DT')),                                       
                   ]  %}
                    (
                        {{ metrics }}
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%}
        {% endif -%}  
    {% elif entity_type  == 'HS'  %}  
        {% if  hist_load  == 'true'  %} 

            {%- for metrics in [(run_metrics_hist_sales_segment(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '5', '2','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)), 
                    (run_metrics_hist_sales_segment(entity_name,'1=1', '62', '1','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    (run_metrics_hist_sales_segment(entity_name,'1=1','68','1','Dim_Opportunity','FORECAST_CATEGORY','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
				    (run_metrics_hist_sales_segment(entity_name,'1=1', '52', '1','Dim_Opportunity','COMPETITOR','INITIAL_CREATE_DT',hist_strt_dt,hist_end_dt)),
                    ]  %}
                    (
                        {{ metrics }}
                    )

                    {% if not loop.last -%}
                        union all
                    {% endif -%}        
            {%- endfor -%} 
        {% else -%}     
            {%- for metrics in [ (run_metrics_hist_sales_segment(entity_name,'upper(IS_CLOSED) = '"'"'FALSE'"'"'', '5', '2','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT')), 
					(run_metrics_hist_sales_segment(entity_name,'1=1', '62', '1','Dim_Opportunity','STAGE_NAME','INITIAL_CREATE_DT')),
                    (run_metrics_hist_sales_segment(entity_name,'1=1','68','1','Dim_Opportunity','FORECAST_CATEGORY','INITIAL_CREATE_DT')),
				    (run_metrics_hist_sales_segment(entity_name,'1=1', '52', '1','Dim_Opportunity','COMPETITOR','INITIAL_CREATE_DT')),
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