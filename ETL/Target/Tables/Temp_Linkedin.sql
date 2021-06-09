-- depends_on: {{ ref('Dim_LinkedIn') }}
-- depends_on: {{ ref('Dim_Calendar') }}


{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE in ('LI_ADS')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
    {% if entity_type =='LI_ADS' and hist_load  == 'true' %} 
         {%- for metrics in [(run_ads_ns_metrics('1 = 1', '172', '10','Dim_LinkedIn','Sum(spend)')) ,  
                    (run_ads_ns_metrics('1 = 1', '173', '10','Dim_LinkedIn','Sum(clicks)')) , 
                    (run_ads_ns_metrics('1 = 1', '175', '10','Dim_LinkedIn','Sum(impressions)')),   
                    (run_ads_ns_metrics('1 = 1', '177', '10','Dim_LinkedIn','Sum(clicks)/ decode(Sum(impressions),0,1,nvl(Sum(impressions),1))')),
                    (run_ads_ns_metrics('1 = 1', '179', '10','Dim_LinkedIn','Sum(spend)/ decode(Sum(clicks),0,1,nvl(Sum(clicks),1))')),
                   ]  %}
        (
             {{ metrics }}
        )

            {% if not loop.last -%}
                union all
            {% endif -%}
        {%- endfor -%} 

    {% endif -%}
{%- endfor -%} 
