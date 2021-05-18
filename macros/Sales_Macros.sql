{% macro fact_table_pit(entity_name,metricid) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }}
  where TimeFrameID = current_date  
),  
Source AS
( SELECT  * FROM   {{ ref('Temp_Sales') }} order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS  varchar (100) ) AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        'PIT' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt <= timeframe.DAY_START 
        and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'TRUE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
    group by
        Report_Date,
        entity_id,
        employee_id,
        source.METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro fact_table(entity_name) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  where TimeFrameID = current_date
),  
Source AS
( SELECT * FROM   {{ ref('Temp_Sales') }} order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS  varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END 
        and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}       
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}      
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}   
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}      
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro fact_table_pit_segmented(entity_name) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  where TimeFrameID = current_date
),  
Source AS
( SELECT  * FROM  {{ ref('Temp_Sales_Segmented') }} order by Report_Dt
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
        Where source.POINT_IN_TIME = 'TRUE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
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
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, Segment_Name, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro fact_table_segmented(entity_name) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  where TimeFrameID = current_date
),  
Source AS
( SELECT * FROM   {{ ref('Temp_Sales_Segmented') }} order by Report_Dt
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}   
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}   
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}    
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}    
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
{%- endmacro %}

{% macro fact_table_pit_hist(entity_name,metricid,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID 
),  
Source AS
( SELECT * FROM  {{ ref('Temp_Sales') }} order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        'PIT' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt <= timeframe.DAY_START 
        and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'TRUE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
    group by
        Report_Date,
        entity_id,
        employee_id,
        source.METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro fact_table_hist(entity_name,metricid,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
),  
Source AS
( SELECT * FROM {{ ref('Temp_Sales') }}  order by Report_Dt
),
Metrics_Calc AS(
    SELECT
        TimeFrameID as Report_Date,
        entity_code  AS entity_id,
       cast (employee_code AS varchar (100) )  AS employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        sum(AMOUNT) as AMOUNT,
        sum(count) as COUNT,  
        'USD'  as Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS
    from Source 
        join timeframe 
        on Report_Dt between timeframe.DAY_START and timeframe.DAY_END 
        and Source.entity_code = timeframe.source_type
        join date_range
        on TimeFrameID = date_range.date_day
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
         Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}     
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}      
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
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
        Where source.POINT_IN_TIME = 'FALSE' and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}    
    group by
        Report_Date,
        entity_id,
        employee_id,
        Source.METRIC_ID,
        METRIC_CATEGORY_ID,
        TimeFrame_Type,
        Reporting_Currency,
        Source.DW_SESSION_NM,
        Source.DW_INS_UPD_DTS                        
 )
 
SELECT Report_Date, entity_id, employee_id, METRIC_ID, METRIC_CATEGORY_ID, TimeFrame_Type, AMOUNT, Count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average, 'USD'  as Reporting_Currency,  DW_SESSION_NM, DW_INS_UPD_DTS from Metrics_Calc
order by Report_Date, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro fact_table_pit_segmented_hist(entity_name,metricid,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
),  
Source AS
( SELECT  * FROM  {{ ref('Temp_Sales_Segmented') }} order by Report_Dt
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
        Where source.POINT_IN_TIME = 'TRUE'  and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
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
{%- endmacro %}

{% macro fact_table_segmented_hist(entity_name,metricid,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
 ),  
Source AS
( SELECT * FROM  {{ ref('Temp_Sales_Segmented') }} order by Report_Dt
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
        Where Source.POINT_IN_TIME = 'FALSE' and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}
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
       Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}   
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}       
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}     
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
        Where source.POINT_IN_TIME = 'FALSE'   and Source.entity_code = '{{ entity_name }}' and Source.METRIC_ID = {{ metricid }}   
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
{%- endmacro %}

{% macro run_metrics_hist_sales(entity_name,clause, metric_id, metric_catagory_id,table_name,date_param,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
 timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  {{ ref('Dim_Employee') }}  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  {{ ref('Dim_Metrics') }} 
),
{% if table_name == 'Dim_Account' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Account') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Contact' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Contact') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Lead' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Lead') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Opportunity' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Opportunity') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Engagement' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Engagement') }}  Where {{ clause }} )  
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where timeframe.source_type = '{{ entity_name }}'
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
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}


{% macro run_metrics_hist_sales_segment(entity_name,clause, metric_id, metric_catagory_id,table_name,segment_name,date_frame,s_date,e_date) -%}

with date_range as 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('" ~ s_date ~ "','dd/mm/yyyy')",
        end_date="to_date('" ~ e_date ~ "','dd/mm/yyyy')"
       )
    }}
),
 timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  join date_range 
  on TimeFrameID = date_range.date_day
  order by TimeFrameID
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  {{ ref('Dim_Employee') }}  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  {{ ref('Dim_Metrics') }} 
),
{% if table_name == 'Dim_Account' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Account') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Contact' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Contact') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Lead' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Lead') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Opportunity' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Opportunity') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Engagement' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Engagement') }}  Where {{ clause }} )    
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.Day_START and timeframe.Day_END 
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where timeframe.source_type = '{{ entity_name }}'
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name,
        Reporting_Currency,
        POINT_IN_TIME
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name, POINT_IN_TIME, nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , TimeFrame_Type,'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro run_metrics_sales(entity_name,clause, metric_id, metric_catagory_id,table_name,date_param) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  where TimeFrameID = current_date
  order by TimeFrameID
  ),
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type,  TimeFrameID as join_Date FROM  {{ ref('Dim_Employee') }}  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  {{ ref('Dim_Metrics') }} 
),
{% if table_name == 'Dim_Account' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Account') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Contact' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Contact') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Lead' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Lead') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Opportunity' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Opportunity') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Engagement' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Engagement') }}  Where {{ clause }} )    
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (100) ) AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.Day_START and timeframe.Day_END 
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where timeframe.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Reporting_Currency,
        POINT_IN_TIME
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (1000)) AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'Y' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.year_START and timeframe.year_END 
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where timeframe.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Reporting_Currency,
        POINT_IN_TIME
        
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'USD'  as Reporting_Currency, 'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}


{% macro run_metrics_sales_segment(entity_name,clause, metric_id, metric_catagory_id,table_name,segment_name,date_frame) -%}

with timeframe as (
  select * from {{ ref('Dim_TimeFrame') }} 
  where TimeFrameID = current_date
  order by TimeFrameID
  ), 
Emp AS
( SELECT source_Emp_id as Emp_id, Entity_id as Emp_Entity_id , source_type, TimeFrameID as join_Date  FROM  {{ ref('Dim_Employee') }}  
 join timeframe on source_type = timeframe.source_type
),
Metrics AS
( SELECT  METRIC_ID , POINT_IN_TIME FROM  {{ ref('Dim_Metrics') }} 
),
{% if table_name == 'Dim_Account' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Account') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Contact' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Contact') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Lead' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Lead') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Opportunity' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Opportunity') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Engagement' %}
 Source AS
   ( SELECT * FROM  {{ ref('Dim_Engagement') }}  Where {{ clause }} )    
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        cast (Emp_id AS varchar (1000)) AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
       'USD' AS   Reporting_Currency,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on cast (Emp.Emp_id AS varchar (1000)) = cast (source.employee_id AS varchar (1000))  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.Day_START and timeframe.Day_END 
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where timeframe.source_type = '{{ entity_name }}' 
         group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name,
        Reporting_Currency,
        POINT_IN_TIME
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name, POINT_IN_TIME, nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , 'D' TimeFrame_Type,'USD'  as Reporting_Currency,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro get_column_values_from_query(query, column) -%}

{%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {% set column_values_sql %}
    with cte as (
        {{ query }}
    )
    select
       distinct {{ column }} 

    from cte    

    {% endset %}

    {%- set results = run_query(column_values_sql) %}
    {{ log(results, info=True) }}
{%- if results -%}
    {% set results_list = results.columns[0].values() %}
    {{ log(results_list, info=True) }}
    {{ return(results_list) }}
{%- else -%}
        {{ return('X') }}
{%- endif -%}

{%- endmacro %}