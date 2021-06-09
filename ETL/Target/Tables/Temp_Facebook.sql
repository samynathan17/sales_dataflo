-- depends_on: {{ ref('Dim_Calendar') }}
-- depends_on: {{ ref('Dim_Facebook') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE in ('GA_ADS','LI_ADS','FB_ADS')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
    {% if entity_type =='FB_ADS' and hist_load  == 'true' %} 
          {%- for metrics in [(run_ads_ns_metrics('1 = 1', '172', '10','Dim_Facebook','Sum(spend)')) ,  
                    (run_ads_ns_metrics('1 = 1', '173', '10','Dim_Facebook','Sum(clicks)')) , 
                    (run_ads_ns_metrics('1 = 1', '175', '10','Dim_Facebook','Sum(impressions)')),   
                    (run_ads_ns_metrics('1 = 1', '177', '10','Dim_Facebook','Sum(CTR)')),
                    (run_ads_ns_metrics('1 = 1', '179', '10','Dim_Facebook','Sum(CPC)')),
                    (run_ads_ns_metrics('1 = 1', '188', '10','Dim_Facebook','Sum(CPM)')),
                    (run_ads_ns_metrics('1 = 1', '189', '10','Dim_Facebook','Sum(REACH)')),
                    (run_ads_ns_metrics('1 = 1', '190 ', '10','Dim_Facebook','Sum(FREQUENCY)')),
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
