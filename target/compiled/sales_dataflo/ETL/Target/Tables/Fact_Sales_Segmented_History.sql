
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_CLOSED) = 'FALSE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('5' as number) as METRIC_ID,
        cast('2' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(STAGE_NAME as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('7' as number) as METRIC_ID,
        cast('3' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(INDUSTRY as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'FALSE' and upper(IS_CLOSED) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('11' as number) as METRIC_ID,
        cast('2' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(EMPLOYEE_ID as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'FALSE' and upper(IS_CLOSED) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('14' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(OPPORTUNITY_NAME as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('18' as number) as METRIC_ID,
        cast('3' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(LEAD_SOURCE as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('19' as number) as METRIC_ID,
        cast('3' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(STATUS as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_WON) = 'TRUE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('22' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(OPPORTUNITY_NAME as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where upper(IS_CLOSED) = 'FALSE' )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('26' as number) as METRIC_ID,
        cast('1' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(OPPORTUNITY_NAME as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( CLOSE_DATE as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('28' as number) as METRIC_ID,
        cast('5' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(EMPLOYEE_ID as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('30' as number) as METRIC_ID,
        cast('3' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(EMPLOYEE_ID as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Lead  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('31' as number) as METRIC_ID,
        cast('3' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(LEAD_CONTACT_ADDRESS as varchar(1000)) As Segment_name,
        sum( 0   ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
    where generated_number <= 729
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
    where date_day <= to_date('31/12/2018', 'dd/mm/yyyy')

)

select * from filtered


),
 timeframe as (
  select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_TimeFrame 
  join date_range 
  on TimeFrameID = date_range.date_day
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 join timeframe on source_type = timeframe.source_type
),

 Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Opportunity  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('32' as number) as METRIC_ID,
        cast('2' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast(OPPORTUNITY_TYPE as varchar(1000)) As Segment_name,
        sum( Amount  ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( INITIAL_CREATE_DT as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
        )

        