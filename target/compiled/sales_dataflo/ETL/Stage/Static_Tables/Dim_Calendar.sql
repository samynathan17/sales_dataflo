



with source as 
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
     + 
    
    p10.generated_number * pow(2, 10)
     + 
    
    p11.generated_number * pow(2, 11)
    
    
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
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 2701
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2015', 'dd/mm/yyyy')
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


) ,Dim_Calendar as(
select
      d.date_day AS Calendar_ID,
      d.date_day as cldr_date,
      cast(
    date_trunc('month', d.date_day)
 as date) as cldr_mnth_strt_dt,    
       cast(
  cast(
        
  

    dateadd(
        day,
        -1,
        
  

    dateadd(
        month,
        1,
        
    date_trunc('month', d.date_day)

        )



        )



        as date)
 as date) as cldr_mnth_end_dt,
        cast(
    date_trunc('quarter', d.date_day)
 as date) as cldr_qtr_strt_dt,
        cast(
  cast(
        
  

    dateadd(
        day,
        -1,
        
  

    dateadd(
        quarter,
        1,
        
    date_trunc('quarter', d.date_day)

        )



        )



        as date)
 as date) as cldr_qtr_end_dt,
        cast(
    date_trunc('year', d.date_day)
 as date) as cldr_year_start_dt,
        cast(
  cast(
        
  

    dateadd(
        day,
        -1,
        
  

    dateadd(
        year,
        1,
        
    date_trunc('year', d.date_day)

        )



        )



        as date)
 as date) as cldr_year_end_dt,
        cast(
    date_trunc('week', d.date_day)
 as date) as week_start_date,
        cast(
  cast(
        
  

    dateadd(
        day,
        -1,
        
  

    dateadd(
        week,
        1,
        
    date_trunc('week', d.date_day)

        )



        )



        as date)
 as date) as week_end_date,
        to_char(d.date_day, 'Dy') as day_short_name,
      to_char(d.date_day, 'MON') as month_short_name,
        cast(to_char(d.date_day, 'MMMM') as varchar(20))as month_name,
        cast(date_part('day', d.date_day) as 
    int
) as cldr_day_num,
        cast(date_part('week', d.date_day) as 
    int
) as cldr_week_num,
        'Q' || cast(date_part('quarter', d.date_day) as 
    int
) as cldr_qtr,
        cast(date_part('year', d.date_day) as 
    int
) as year,
        cast(
            case
                when date_part('dayofweek', d.date_day) = 0 then 7
                else date_part('dayofweek', d.date_day)
            end as 
    int

            ) as day_of_week,
        case
            when date_part('dayofweek', d.date_day) = 0 then 'N'
            else 'Y'
        end as weekday_flag,   
         case when Calendar_ID = week_end_date then 'TRUE' else 'FALSE' end as Weekend_FLag,
         case when Calendar_ID = cldr_mnth_end_dt then 'TRUE' else 'FALSE' end as  Monthend_FLag,
        case when Calendar_ID = cldr_qtr_end_dt then 'TRUE' else 'FALSE' end as  Quarterend_FLag,
        case when Calendar_ID = cldr_year_end_dt then 'TRUE' else 'FALSE' end as  Yearend_FLag,
      
    current_timestamp::
    timestamp_ntz

 as DW_INS_UPD_DTS,
      'D_CALENDAR_DIM_LOAD' as DW_SESSION_NM
from
    source  d
order by 2)

select * from Dim_Calendar