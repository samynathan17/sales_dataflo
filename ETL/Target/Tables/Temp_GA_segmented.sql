-- depends_on: {{ ref('Dim_Session') }}
-- depends_on: {{ ref('Dim_Channel_Traffic') }}
-- depends_on: {{ ref('Dim_Social_Media_Acquisitions') }}
-- depends_on: {{ ref('Dim_Page_Tracking') }}
-- depends_on: {{ ref('Dim_Goal_Conversions') }}
-- depends_on: {{ ref('Dim_Events_Overview') }}
-- depends_on: {{ ref('Dim_Geo_Network') }}
-- depends_on: {{ ref('Dim_Traffic') }}
-- depends_on: {{ ref('Dim_Adwords_Keyword') }}
-- depends_on: {{ ref('Dim_Platform_Device') }}
-- depends_on: {{ ref('Dim_Calendar') }}

{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE in ('GA')", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||HISTORY_LOAD||'#'||TO_VARCHAR(nvl(HISTORY_START_DATE,HISTORY_ACTUAL_START_DATE)::DATE, 'DD/MM/YYYY')||'#'||TO_VARCHAR(HISTORY_END_DATE::DATE, 'DD/MM/YYYY')") %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
{% set entity_name, entity_type,hist_load, hist_strt_dt, hist_end_dt= V_SF_Schema.split('#') %}
    {% if entity_type =='GA' and hist_load  == 'true' %} 
        {%- for metrics in [(run_metrics('1 = 1', '93', '10','Dim_Platform_Device','DEVICE_CATEGORY','Count(*)')) ,  
                    (run_metrics('1 = 1', '94', '10','Dim_Channel_Traffic','CHANNEL_GROUPING','Sum(SESSIONS)')) , 
                    (run_metrics('1 = 1', '95', '10','Dim_Social_Media_Acquisitions','SOCIAL_NETWORK','Count(*)')) , 
                    (run_metrics('1 = 1', '99', '10','Dim_Channel_Traffic','CHANNEL_GROUPING','Sum(SESSIONS)')),   
                    (run_metrics('upper(KEYWORD)='"'"'ORGANIC'"'"'', '101', '10','Dim_Adwords_Keyword','KEYWORD','Sum(SESSIONS)')),
                    (run_metrics('1 = 1', '102', '10','Dim_Channel_Traffic','CHANNEL_GROUPING','Sum(GOAL_COMPLETIONS_ALL)')), 
                    (run_metrics('1 = 1', '108', '10','Dim_Goal_Conversions','GOAL_COMPLETION_LOCATION','Sum(GOAL_COMPLETIONS_ALL)')),  
                    (run_metrics('upper(KEYWORD)='"'"'PAID'"'"'', '109', '10','Dim_Adwords_Keyword','KEYWORD','Sum(SESSIONS)')), 
                    (run_metrics('1 = 1', '110', '10','Dim_Channel_Traffic','CHANNEL_GROUPING','Sum(GOAL_VALUE_ALL)')),
                    (run_metrics('1 = 1', '112', '10','Dim_Goal_Conversions','GOAL_COMPLETION_LOCATION','Sum(GOAL_VALUE_ALL)')),   
                    (run_metrics('1 = 1', '114', '10','Dim_Page_Tracking','LANDING_PAGE_PATH','Sum(PAGEVIEWS_PER_SESSION)')),
                    (run_metrics('1 = 1', '115', '10','Dim_Traffic','PAGE_TITLE','Count(*)')),
                    (run_metrics('1 = 1', '120', '10','Dim_Geo_Network','NETWORK_LOCATION','Sum(SESSIONS)')),                    
                    (run_metrics('1 = 1', '125', '10','Dim_Channel_Traffic','CHANNEL_GROUPING','sum(USERS)')),
                    (run_metrics('1 = 1', '130', '10','Dim_Social_Media_Acquisitions','NEW_USERS','sum(PAGEVIEWS)')),
                    (run_metrics('1 = 1', '131', '10','Dim_Events_Overview','EVENT_CATEGORY','sum(SESSIONS_WITH_EVENT)')),
                    (run_metrics('upper(CHANNEL_GROUPING)='"'"'ORGANIC'"'"'', '139', '10','Dim_Channel_Traffic','USERS','Sum(SESSIONS)'))                  
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