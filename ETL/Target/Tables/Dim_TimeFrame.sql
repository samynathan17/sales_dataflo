--depends_on: {{ ref('Stg_Period') }}
--depends_on: {{ ref('Dim_Calendar') }}

{%- set source_relation = adapter.get_relation( var('V_DB'),var('V_Schema'),"DIM_SALES_ENTITY") -%}
{% set results = dbt_utils.get_column_values(source_relation,'entity_name') %}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{ config(
    materialized="table"
) 
}}

{% for V_SF_Schema in results %}
 {% if  V_SF_Schema[0:2] == 'SF'  %}  
(with year_data as 
(
    select * from {{ ref('Stg_Period') }} 
    where type ='Year'
    and source_type = '{{ V_SF_Schema}}'
),
qutr_data as 
(
    select * from {{ ref('Stg_Period') }} 
    where type ='Quarter'
    and source_type = '{{ V_SF_Schema}}'
),
 Dates AS 
(
    SELECT * FROM {{ ref('Dim_Calendar') }}
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
    d.year as year,
    y.start_date as year_start,
    y.end_date as year_end,
    '{{ V_SF_Schema }}' as Source_type,
    'D_TIMEFRAME_DIM_LOAD' AS DW_SESSION_NM,
    {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS     
from
    dates d
    left join qutr_data s
    on d.cldr_date between s.start_date and s.end_date
    left join year_data y
    on d.cldr_date between y.start_date and y.end_date
order by 2)
 {% elif  V_SF_Schema[0:2] == 'HS'  %} 
 (with  Dates AS 
(
    SELECT * FROM {{ ref('Dim_Calendar') }}
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
    cldr_qtr as qutr_number,
    d.cldr_qtr_strt_dt as quarter_start,
    d.cldr_qtr_end_dt as quarter_end,
    d.year as year,
    d.cldr_year_start_dt as year_start,
    d.cldr_qtr_end_dt as year_end,
    '{{ V_SF_Schema }}' as Source_type,
    'D_TIMEFRAME_DIM_LOAD' AS DW_SESSION_NM,
    {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS     
from
    dates d
 order by 2)

{% endif %}
{% if loop.nextitem is defined %}
            UNION ALL
        {% endif %}

{% endfor %}
