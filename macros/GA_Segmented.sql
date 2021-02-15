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
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('{{ metric_id }}' as number) as METRIC_ID,
        cast('{{ metric_catagory_id }}' as number)  AS METRIC_CATEGORY_ID,
        cast({{ segment_name }} as varchar(1000)) As Segment_name,
        {{aggr_value}}  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
{%- endmacro %}
