--depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Period
--depends_on: DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar






   
(with year_data as 
(
    select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Period 
    where type ='Year'
    and source_type = 'SF_RKLIVE_06012021'
),
qutr_data as 
(
    select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Period 
    where type ='Quarter'
    and source_type = 'SF_RKLIVE_06012021'
),
 Dates AS 
(
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar
)   
select
    d.Calendar_ID AS TimeFrameID,
    'D' as TimeFrameType,
    d.Calendar_ID as calendar_id,
    d.cldr_date as day_start,
    d.cldr_date as day_end,
    d.week_start_date as week_Start,
    d.week_end_date as week_end,
    'W' || d.cldr_week_num as week_num,
    d.cldr_mnth_strt_dt as month_start,
    d.cldr_mnth_end_dt as month_end, 
    d.month_name as month_name,
     'Q' || s.number as qutr_number,
    s.start_date as quarter_start,
    s.end_date as quarter_end,
    right(y.FULLY_QUALIFIED_LABEL,4) as year,
    y.start_date as year_start,
    y.end_date as year_end,
    'SF_RKLIVE_06012021' as Source_type,
    'D_TIMEFRAME_DIM_LOAD' AS DW_SESSION_NM,
    
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS     
from
    dates d
    left join qutr_data s
    on d.cldr_date between s.start_date and s.end_date
    left join year_data y
    on d.cldr_date between y.start_date and y.end_date
order by 2)
 

            UNION ALL
        


   
(with year_data as 
(
    select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Period 
    where type ='Year'
    and source_type = 'SF_TESTUSER_31122020'
),
qutr_data as 
(
    select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Period 
    where type ='Quarter'
    and source_type = 'SF_TESTUSER_31122020'
),
 Dates AS 
(
    SELECT * FROM DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Calendar
)   
select
    d.Calendar_ID AS TimeFrameID,
    'D' as TimeFrameType,
    d.Calendar_ID as calendar_id,
    d.cldr_date as day_start,
    d.cldr_date as day_end,
    d.week_start_date as week_Start,
    d.week_end_date as week_end,
    'W' || d.cldr_week_num as week_num,
    d.cldr_mnth_strt_dt as month_start,
    d.cldr_mnth_end_dt as month_end, 
    d.month_name as month_name,
     'Q' || s.number as qutr_number,
    s.start_date as quarter_start,
    s.end_date as quarter_end,
    right(y.FULLY_QUALIFIED_LABEL,4) as year,
    y.start_date as year_start,
    y.end_date as year_end,
    'SF_TESTUSER_31122020' as Source_type,
    'D_TIMEFRAME_DIM_LOAD' AS DW_SESSION_NM,
    
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS     
from
    dates d
    left join qutr_data s
    on d.cldr_date between s.start_date and s.end_date
    left join year_data y
    on d.cldr_date between y.start_date and y.end_date
order by 2)
 


