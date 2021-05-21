

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Marketing_Segmented  as
      (-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Geo_Network
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Traffic
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Platform_Device
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_LinkedIn
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_GA_Ads
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Site
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar







    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Platform_Device  Where 1 = 1 )   
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='ORGANIC' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='PAID' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Traffic  Where 1 = 1 )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Geo_Network  Where 1 = 1 ) 
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where upper(CHANNEL_GROUPING)='ORGANIC' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            

    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Platform_Device  Where 1 = 1 )   
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='ORGANIC' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='PAID' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Traffic  Where 1 = 1 )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Geo_Network  Where 1 = 1 ) 
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where upper(CHANNEL_GROUPING)='ORGANIC' )
 
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
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'W' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'M' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Q' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'Y' as TimeFrame_Type,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(USERS as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('174' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(AD_GROUP_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('174' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(AD_GROUP_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('174' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(AD_GROUP_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('174' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(AD_GROUP_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('174' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(AD_GROUP_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('176' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('176' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('176' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('176' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('176' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(clicks)/ (decode(Sum(impressions),0,1,Sum(impressions)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('180' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
        Sum(spend)/ (decode(Sum(clicks),0,1,Sum(clicks)))  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            

    
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('196' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('196' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('196' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('196' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('196' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('197' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('197' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('197' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('197' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('197' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('200' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('200' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('200' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('200' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('200' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('201' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('201' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('201' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('201' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('201' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('202' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('202' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('202' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('202' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('202' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('204' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('204' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('204' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('204' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('204' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('205' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('205' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('205' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('205' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('205' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('206' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('206' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('206' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('206' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('206' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Keyword_Site  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('207' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(POSITION )  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('207' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(POSITION )  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('207' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(POSITION )  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('207' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(POSITION )  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('207' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(POSITION )  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            union all
            
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page  Where 1 = 1 ) 
 
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('208' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('208' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('208' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('208' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('208' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(PAGE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('209' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('209' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('209' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('209' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('209' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('210' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('210' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('210' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('210' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('210' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(CLICKS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('211' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('211' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('211' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('211' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('211' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('212' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('212' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('212' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('212' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('212' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('213' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('213' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('213' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('213' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('213' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(IMPRESSIONS)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('214' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('214' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('214' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('214' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('214' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
        Sum(POSITION)  as Value
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('215' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('215' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('215' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('215' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('215' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(COUNTRY as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('216' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('216' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('216' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('216' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('216' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(DEVICE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('217' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('217' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('217' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('217' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('217' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(SEARCH_TYPE as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name            
    )
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
        )

            
      );
    