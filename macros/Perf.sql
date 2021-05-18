{% macro run_metrics_perf_hist_sales(entity_name,clause, metric_id, metric_catagory_id,table_name,date_param,s_date,e_date) -%}

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
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
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
        POINT_IN_TIME
union all

        SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'W' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type 
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.week_START and timeframe.week_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where Emp.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME

        union all

        SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'M' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.month_START and timeframe.month_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where Emp.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME

        union all

        SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'Q' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.QUARTER_START and timeframe.QUARTER_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
        
        where Emp.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME
        union all

        SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'Y' as TimeFrame_Type,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_param }} as date) between timeframe.year_START and timeframe.year_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
          join date_range
          on TimeFrameID = date_range.date_day
          join Metrics
          on '{{ metric_id }}' = metrics.METRIC_ID
          where Emp.source_type = '{{ entity_name }}'
        group by
        Report_Dt,
        entity_code,
        employee_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        POINT_IN_TIME
                )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID,POINT_IN_TIME,nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average ,  TimeFrame_Type, 'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}

{% macro run_metrics_perf_hist_sales_segment(entity_name,clause, metric_id, metric_catagory_id,table_name,segment_name,date_frame,s_date,e_date) -%}

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
{% endif %} 
,Metrics_Calc AS(
    SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'D' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
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
        POINT_IN_TIME

       union all

       SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'W' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.week_START and timeframe.week_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
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
        POINT_IN_TIME
        
        union all

       SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'M' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.month_START and timeframe.month_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
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
        POINT_IN_TIME 

        union all

       SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'Q' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.Quarter_START and timeframe.Quarter_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
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
        POINT_IN_TIME

        union all

       SELECT
        TimeFrameID AS Report_Dt,
        Emp_Entity_id  AS entity_code,
        Emp_id AS employee_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        'Y' as TimeFrame_Type,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        metrics.POINT_IN_TIME as POINT_IN_TIME,
        sum({% if table_name == 'Dim_Opportunity' %} Amount {% else %} 0  {% endif %} ) as AMOUNT,
        count(SOURCE_ID)  as Count
     from Emp 
          join timeframe 
          on join_Date = TimeFrameID and Emp.source_type = timeframe.source_type
          left join Source 
          on Emp.Emp_id = source.employee_id  and Emp.source_type = source.source_type
          and cast( {{ date_frame }} as date) between timeframe.Year_START and timeframe.year_END 
          and cast( {{ date_frame }} as date) <= TimeFrameID
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
        POINT_IN_TIME  
        )
 SELECT Report_Dt, entity_code, employee_code,METRIC_ID,METRIC_CATEGORY_ID, Segment_name, POINT_IN_TIME, nvl(amount,0) as Amount, count, 
        nvl(AMOUNT/decode(count,0,1,count),0) as Average , TimeFrame_Type,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID, TimeFrame_Type
{%- endmacro %}
