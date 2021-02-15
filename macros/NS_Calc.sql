{% macro fact_table(s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=s_date,
        end_date=e_date
       )
    }}
),
 timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  ),  
Source AS
( SELECT * FROM  {{ ref('Fact_Sales_History') }}
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
        employee_code AS employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
    group by
        Report_Date,
        entity_id,
        employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
        employee_code AS employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        'W' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.WEEK_START and timeframe.WEEK_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day        
    group by
        Report_Date,
        entity_id,
        employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
        employee_code AS employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        'M' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.MONTH_START and timeframe.MONTH_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day        
    group by
        Report_Date,
        entity_id,
        employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
        employee_code AS employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        'Q' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.QUARTER_START and timeframe.QUARTER_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day        
    group by
        Report_Date,
        entity_id,
        employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
     Union
        SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
        employee_code AS employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        'Y' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.YEAR_START and timeframe.YEAR_END
        and Report_Dt <= TimeFrameID and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day        
    group by
        Report_Date,
        entity_id,
        employee_id,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}