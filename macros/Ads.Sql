{% macro run_ad_metrics(clause, metric_id, metric_catagory_id,table_name,segment_name,aggr_value) -%}

{% if table_name == 'Dim_LinkedIn' %}
With Source AS
   ( select * from {{ ref('Dim_LinkedIn')}}  Where {{ clause }} )
{% elif table_name == 'Dim_Facebook' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Facebook') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Site' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Site') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Page' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Page') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Keyword_Site' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Keyword_Site') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Keyword_Page' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Keyword_Page') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_GA_Ads' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_GA_Ads') }}  Where {{ clause }} )               
{% endif %} 
,date_range as 
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2017', 'dd/mm/yyyy')",
        end_date="to_date('31/05/2021', 'dd/mm/yyyy')",
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
		cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
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
 SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type,Segment_name,Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
{%- endmacro %}


{% macro run_ads_ns_metrics(clause, metric_id, metric_catagory_id,table_name,aggr_value) -%}
{% if table_name == 'Dim_LinkedIn' %}
With Source AS
   ( select * from {{ ref('Dim_LinkedIn')}}  Where {{ clause }} )
{% elif table_name == 'Dim_Facebook' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Facebook') }}  Where {{ clause }} ) 
{% elif table_name == 'Dim_Site' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_Site') }}  Where {{ clause }} )   
{% elif table_name == 'Dim_GA_Ads' %}
With Source AS
   ( SELECT * FROM  {{ ref('Dim_GA_Ads') }}  Where {{ clause }} )                
{% endif %} 
,date_range as 
(

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('01/01/2017', 'dd/mm/yyyy')",
        end_date="to_date('31/05/2021', 'dd/mm/yyyy')",
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
        platform  AS entity_code,
        'D' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        METRIC_CATEGORY_ID
    
    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'W' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'M' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Q' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        METRIC_CATEGORY_ID

    union
         SELECT
        Calendar_ID as Report_Date,
        platform  AS entity_code,
        'Y' as TimeFrame_Type,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        {{aggr_value}}  as Value
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
        METRIC_CATEGORY_ID            
    )
        SELECT Report_Date, entity_code, METRIC_ID,METRIC_CATEGORY_ID, TimeFrame_Type, Value,'D_ADS_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Date, METRIC_ID
{%- endmacro %}

