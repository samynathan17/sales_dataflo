

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_LinkedIn_Segmented  as
      (
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'D' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'W' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'M' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('178' as number) as METRIC_ID,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'D' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'W' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'M' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('177' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'D' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name
    
    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'W' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'M' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
        METRIC_CATEGORY_ID,
		Segment_name

    union
         SELECT
        Calendar_ID as Report_Date,
        'LINKEDIN_ADS_19032021'  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('179' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
		cast(CAMPAIGN_NAME as varchar(1000)) As Segment_name,
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
    