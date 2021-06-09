-- depends_on: {{ ref('Dim_LinkedIn') }}
-- depends_on: {{ ref('Dim_GA_Ads') }}
-- depends_on: {{ ref('Dim_Calendar') }}
-- depends_on: {{ ref('Dim_Facebook') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE in ('GA_ADS','LI_ADS','FB_ADS')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
    
    {% if entity_type =='LI_ADS' and hist_load  == 'true' %} 
         {%- for metrics in [(run_ad_metrics('1 = 1', '174', '10','Dim_LinkedIn','AD_GROUP_NAME','Sum(spend)')) , 
                    (run_ad_metrics('1 = 1', '176', '10','Dim_LinkedIn','CAMPAIGN_NAME','Sum(impressions)')),   
                    (run_ad_metrics('1 = 1', '178', '10','Dim_LinkedIn','CAMPAIGN_NAME','Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))')),
                    (run_ad_metrics('1 = 1', '180', '10','Dim_LinkedIn','CAMPAIGN_NAME','Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))')),
                   ]  %}
        (
             {{ metrics }}
        )

            {% if not loop.last -%}
                union all
            {% endif -%}
        {%- endfor -%} 
    {% endif -%}

    {% if entity_type =='FB_ADS' and hist_load  == 'true' %} 
          {%- for metrics in [(run_ad_metrics('1 = 1', '176', '10','Dim_Facebook','CAMPAIGN_NAME','Sum(impressions)')),   
                    (run_ad_metrics('1 = 1', '178', '10','Dim_Facebook','CAMPAIGN_NAME','Sum(CTR)')),
                    (run_ad_metrics('1 = 1', '180', '10','Dim_Facebook','CAMPAIGN_NAME','Sum(CPC)')),
                   ]  %}
        (
             {{ metrics }}
        )

            {% if not loop.last -%}
                union all
            {% endif -%}
        {%- endfor -%} 

        {% elif entity_type =='GA_ADS' and hist_load  == 'true' %} 
         {%- for metrics in [(run_ad_metrics('1 = 1', '178', '10','Dim_GA_Ads','CAMPAIGN_NAME','Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))')),
                    (run_ad_metrics('1 = 1', '180', '10','Dim_GA_Ads','CAMPAIGN_NAME','Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))')),
                   ]  %}
        (
             {{ metrics }}
        )

            {% if not loop.last -%}
                union all
            {% endif -%}
        {%- endfor -%} 
  
    {% endif -%}
    {% if not loop.last -%}
                union all
            {% endif -%}

{%- endfor -%} 