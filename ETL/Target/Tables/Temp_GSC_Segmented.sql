-- depends_on: {{ ref('Dim_Session') }}
-- depends_on: {{ ref('Dim_Site') }}
-- depends_on: {{ ref('Dim_Page') }}
-- depends_on: {{ ref('Dim_Keyword_Site') }}
-- depends_on: {{ ref('Dim_Calendar') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE in ('GSC')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
      {% if entity_type =='GSC' and hist_load  == 'true' %} 
          {%- for metrics in [(run_ad_metrics('1 = 1', '196', '10','Dim_Page','PAGE','Sum(CLICKS)')),   
                    (run_ad_metrics('1 = 1', '197', '10','Dim_Keyword_Site','KEYWORD','Sum(CLICKS)')),
                    (run_ad_metrics('1 = 1', '200', '10','Dim_Page','PAGE','Sum(CTR)')),
                    (run_ad_metrics('1 = 1', '201', '10','Dim_Keyword_Site','KEYWORD','Sum(CTR)')),
                    (run_ad_metrics('1 = 1', '202', '10','Dim_Page','PAGE','Sum(IMPRESSIONS)')),
                    (run_ad_metrics('1 = 1', '204', '10','Dim_Keyword_Site','KEYWORD','Sum(IMPRESSIONS)')),
                    (run_ad_metrics('1 = 1', '205', '10','Dim_Site','COUNTRY','Sum(POSITION)')),
                    (run_ad_metrics('1 = 1', '206', '10','Dim_Site','DEVICE','Sum(POSITION)')),
                    (run_ad_metrics('1 = 1', '207', '10','Dim_Keyword_Site','KEYWORD','Sum(POSITION )')),
                    (run_ad_metrics('1 = 1', '208', '10','Dim_Page','PAGE','Sum(POSITION)')),
                    (run_ad_metrics('1 = 1', '209', '10','Dim_Site','DEVICE','Sum(CLICKS)')),
                    (run_ad_metrics('1 = 1', '210', '10','Dim_Site','SEARCH_TYPE','Sum(CLICKS)')),
                    (run_ad_metrics('1 = 1', '211', '10','Dim_Site','COUNTRY','Sum(IMPRESSIONS)')),
                    (run_ad_metrics('1 = 1', '212', '10','Dim_Site','DEVICE','Sum(IMPRESSIONS)')),
                    (run_ad_metrics('1 = 1', '213', '10','Dim_Site','SEARCH_TYPE','Sum(IMPRESSIONS)')),
                    (run_ad_metrics('1 = 1', '214', '10','Dim_Site','SEARCH_TYPE','Sum(POSITION)')),
                    (run_ad_metrics('1 = 1', '215', '10','Dim_Site','COUNTRY','Sum(CTR)')),
                    (run_ad_metrics('1 = 1', '216', '10','Dim_Site','DEVICE','Sum(CTR)')),
                    (run_ad_metrics('1 = 1', '217', '10','Dim_Site','SEARCH_TYPE','Sum(CTR)')),
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