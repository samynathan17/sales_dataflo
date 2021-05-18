

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Sales_Segmented  as
      (--depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame










     
		   
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 18
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 18   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 18       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 18     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 18   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 31
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 31   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 31       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 31     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 31   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 68
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 68   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 68       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 68     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 68   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 52
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 52   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 52       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 52     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 52   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 19
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 19   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 19       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 19     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 19   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 32
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 32   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 32       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 32     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 32   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 28
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 28   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 28       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 28     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 28   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
),  
Source AS
( SELECT  * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'PIT' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt <= timeframe.DAY_START and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'TRUE'  and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 5
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
)
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID,Segment_Name, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type 
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 7
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 7   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 7       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 7     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 7   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 30
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 30   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 30       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 30     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 30   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
    where generated_number <= 1094
    order by generated_number



),

all_periods as (

    select (
        
  

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        to_date('01/02/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/01/2020','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 62
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 62   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 62       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 62     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'SF_RKLIVE_06012021' and Source.METRIC_ID = 62   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
	

     
		   
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 18
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 18   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 18       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 18     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 18   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 31
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 31   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 31       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 31     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 31   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 68
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 68   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 68       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 68     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 68   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 52
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 52   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 52       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 52     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 52   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 19
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 19   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 19       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 19     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 19   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 32
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 32   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 32       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 32     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 32   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 28
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 28   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 28       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 28     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 28   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
),  
Source AS
( SELECT  * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'PIT' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt <= timeframe.DAY_START and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'TRUE'  and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 5
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
)
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID,Segment_Name, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type 
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 7
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 7   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 7       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 7     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 7   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 30
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 30   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 30       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 30     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 30   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								union all
				
		   
					
								  (
								  with date_range as 
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
        to_date('01/01/2017','dd/mm/yyyy')
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= to_date('31/03/2021','dd/mm/yyyy')

)

select * from filtered


),
timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales_Segmented order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type 
        join date_range
        on TimeFrameID = date_range.date_day
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 62
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 62   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 62       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 62     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = 'HS_RKLIVE_01042021' and Source.METRIC_ID = 62   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_Name,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name,TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS  from Metrics_Calc
order by Report_Date, METRIC_ID,Segment_Name, TimeFrame_Type
								  )
							  
								
      );
    