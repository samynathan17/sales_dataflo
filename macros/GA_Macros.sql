{% macro run_metrics(clause, metric_id, metric_catagory_id,table_name,segment_name,aggr_value) -%}

{% if table_name == 'Dim_Channel_Traffic' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Channel_Traffic') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Social_Media_Acquisitions' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Social_Media_Acquisitions') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Platform_Device' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Platform_Device') }}  Where {{ clause }} )   
{% elif table_name == 'Dim_Adwords_Keyword' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Adwords_Keyword') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Goal_Conversions' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Goal_Conversions') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Page_Tracking' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Page_Tracking') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Traffic' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Traffic') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Geo_Network' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Geo_Network') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Events_Overview' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Events_Overview') }}  Where {{ clause }} )           
{% endif %} 
,date_range as 
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2017', 'dd/mm/yyyy')",
        end_date="to_date('31/03/2021', 'dd/mm/yyyy')",
       )
    }}
) ,
timeframe as (
  select * from {{ ref('Dim_Calendar') }} 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type AS entity_code,
        'D' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
{%- endmacro %}


{% macro run_ns_metrics(clause, metric_id, metric_catagory_id,table_name,aggr_value) -%}
{% if table_name == 'Dim_Channel_Traffic' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Channel_Traffic') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Social_Media_Acquisitions' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Social_Media_Acquisitions') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Platform_Device' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Platform_Device') }}  Where {{ clause }} )   
{% elif table_name == 'Dim_Adwords_Keyword' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Adwords_Keyword') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Goal_Conversions' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Goal_Conversions') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Page_Tracking' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Page_Tracking') }}  Where {{ clause }} )
{% elif table_name == 'Dim_Session' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Session') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Events_Overview' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Events_Overview') }}  Where {{ clause }} )           
{% endif %} 
,date_range as 
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2017', 'dd/mm/yyyy')",
        end_date="to_date('31/03/2021', 'dd/mm/yyyy')",
       )
    }}
) ,
timeframe as (
  select * from {{ ref('Dim_Calendar') }} 
  join date_range 
  on CALENDAR_ID = date_range.date_day
  order by CALENDAR_ID 
)
,Metrics_Calc AS(
    SELECT
        Calendar_ID as Report_Date,
        Source_type  AS entity_code,
        'D' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
{%- endmacro %}

