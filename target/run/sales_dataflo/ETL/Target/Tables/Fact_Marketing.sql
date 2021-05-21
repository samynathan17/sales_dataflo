

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Marketing  as
      (-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Site
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar







    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview  Where 1 = 1 )           
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
        

    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview  Where 1 = 1 )           
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 1550
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/01/2017', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'W' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'M' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
     join timeframe 
        on source.Date  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.Date <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_MKT_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
        

    
        (
             
With Source AS
   ( select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn  Where 1 = 1 )
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
        

    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads  Where 1 = 1 )                
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('172' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads  Where 1 = 1 )                
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('173' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads  Where 1 = 1 )                
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('175' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads  Where 1 = 1 )                
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)/ Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads  Where 1 = 1 )                
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(spend)/ Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
        

    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Site  Where 1 = 1 )   
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('195' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('195' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('195' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('195' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('195' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(clicks)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Site  Where 1 = 1 )   
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('198' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('198' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('198' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('198' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('198' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(impressions)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Site  Where 1 = 1 )   
 
,date_range as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 30
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/03/2021', 'dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021', 'dd/mm/yyyy')

)

select * from filtered


) ,
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('199' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(CTR)  as Value
     from Source 
     join timeframe 
        on source.date_day = Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
         group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('199' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(CTR)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.WEEK_START_DATE and timeframe.WEEK_END_DATE
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('199' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(CTR)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_MNTH_STRT_DT and timeframe.CLDR_MNTH_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('199' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(CTR)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_QTR_STRT_DT and timeframe.CLDR_QTR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('199' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(CTR)  as Value
     from Source 
     join timeframe 
        on source.date_day  between timeframe.CLDR_YEAR_START_DT and timeframe.CLDR_YEAR_END_DT
        and source.date_day <= Calendar_ID
        join date_range
        on Calendar_ID = date_range.date_day    
        group by
        Report_Date,
        entity_code,
        TimeFrame_Type,
        METRIC_ID,
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            
      );
    