{{ config(
    materialized="table"
) 
}}
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