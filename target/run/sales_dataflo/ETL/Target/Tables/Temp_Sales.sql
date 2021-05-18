

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Temp_Sales  as
      (-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Contact
-- depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement








     
        
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('1' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '1' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'FALSE' and upper(IS_CLOSED) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('10' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '10' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where upper(lead_to_opp_flag) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('3' as number) as METRIC_ID,
        cast('4' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( lead_CONVERTED_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '3' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('4' as number) as METRIC_ID,
        cast('4' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '4' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_CLOSED) = 'FALSE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('23' as number) as METRIC_ID,
        cast('2' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '23' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('27' as number) as METRIC_ID,
        cast('5' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '27' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Contact  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('29' as number) as METRIC_ID,
        cast('6' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '29' = metrics.METRIC_ID
          where timeframe.source_type = 'SF_RKLIVE_06012021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type 
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('1' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '1' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'FALSE' and upper(IS_CLOSED) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('10' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '10' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_CLOSED) = 'FALSE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('23' as number) as METRIC_ID,
        cast('2' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '23' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_CLOSED) = 'FALSE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('79' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '79' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement  Where upper(TYPE) = 'CALL' )  
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('39' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '39' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement  Where upper(TYPE) = 'TASK' )  
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('89' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '89' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement  Where upper(TYPE) = 'MEETING' )  
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('71' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '71' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement  Where upper(TYPE) = 'NOTE' )  
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('76' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '76' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
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
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Metrics 
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Engagement  Where upper(TYPE) = 'EMAIL' )  
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('66' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( initial_create_dt as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '66' = metrics.METRIC_ID
          where timeframe.source_type = 'HS_RKLIVE_01042021'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME,
        Reporting_Currency        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
                    )

                    
      );
    