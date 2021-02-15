{{ config(
    materialized="table"
) 
}}
{%- for metrics in [(run_ns_metrics('1 = 1', '139', '10','Dim_Session','Sum(SESSIONS_PER_USER)')) ,  
                    (run_ns_metrics('1 = 1', '140', '10','Dim_Channel_Traffic','Sum(SESSIONS)')) , 
                    (run_ns_metrics('1 = 1', '141', '10','Dim_Social_Media_Acquisitions','Sum(PERCENT_NEW_SESSIONS)')) , 
                    (run_ns_metrics('1 = 1', '142', '10','Dim_Social_Media_Acquisitions','Sum(NEW_USERS)')),   
                    (run_ns_metrics('1 = 1', '143', '10','Dim_Social_Media_Acquisitions','Sum(PAGEVIEWS)')),
                    (run_ns_metrics('1 = 1', '144', '10','Dim_Page_Tracking','Sum(PAGEVIEWS_PER_SESSION)')), 
                    (run_ns_metrics('1 = 1', '145', '10','Dim_Session','Sum(BOUNCE_RATE)')),  
                    (run_ns_metrics('1 = 1', '147', '10','Dim_Social_Media_Acquisitions','Sum(AVG_SESSION_DURATION)')), 
                    (run_ns_metrics('1 = 1', '151', '10','Dim_Goal_Conversions','Sum(GOAL_COMPLETIONS_ALL)')),
                    (run_ns_metrics('1 = 1', '152', '10','Dim_Goal_Conversions','Sum(GOAL_CONVERSION_RATE_ALL)')),   
                    (run_ns_metrics('1 = 1', '158', '10','Dim_Social_Media_Acquisitions','Sum(TRANSACTIONS_PER_SESSION)')),
                    (run_ns_metrics('1 = 1', '163', '10','Dim_Events_Overview','Sum(TOTAL_EVENTS)')),
                    (run_ns_metrics('1 = 1', '164', '10','Dim_Social_Media_Acquisitions','Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)')),                    
                    (run_ns_metrics('1 = 1', '166', '10','Dim_Page_Tracking','sum(TIME_ON_PAGE)')),
                    (run_ns_metrics('1 = 1', '167', '10','Dim_Page_Tracking','sum(UNIQUE_PAGEVIEWS)')),
                   ]  %}
        (
             {{ metrics }}
        )

        {% if not loop.last -%}
            union all
        {% endif -%}
{%- endfor -%} 