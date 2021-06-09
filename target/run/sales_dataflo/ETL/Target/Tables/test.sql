
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.test  as (
    with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn

),
date_range as 
(

    

/*
call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dateadd(week, 1, current_date)"
)

*/

with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * pow(2, 0)
     + 
    
    p1.generated_number * pow(2, 1)
     + 
    
    p2.generated_number * pow(2, 2)
     + 
    
    p3.generated_number * pow(2, 3)
     + 
    
    p4.generated_number * pow(2, 4)
     + 
    
    p5.generated_number * pow(2, 5)
     + 
    
    p6.generated_number * pow(2, 6)
     + 
    
    p7.generated_number * pow(2, 7)
     + 
    
    p8.generated_number * pow(2, 8)
     + 
    
    p9.generated_number * pow(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 514
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= dateadd(week, 53, current_date)

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
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
  );
