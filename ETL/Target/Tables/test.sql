with base as (

    select *
    from {{ ref('Dim_LinkedIn')}}

),
date_range as 
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2021', 'dd/mm/yyyy')",
        end_date="dateadd(week, 53, current_date)"
       )
    }}
) ,
timeframe as (
  select * from {{ ref('Dim_Calendar') }} 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
select 
    'A' as platform,
        Calendar_ID as Repot_date,
        '100' as Metric_ID,
        'W' as TimeFrame_Type,
        Sum(clicks) as Sum_clicks,
        Sum(impressions) as Sum_impressions,
        Sum(spend) as Sum_spend,
        Sum(clicks)/ Sum(impressions) as CTR_Click
from base
join timeframe 
        on base.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and base.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day         
group by platform, 
Repot_date,
Metric_ID,
TimeFrame_Type