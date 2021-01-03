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

with year_data as 
(
    select * from {{ var('V_SF_Schema') }}.period 
    where type ='Year'
),
qutr_data as 
(
    select * from {{ var('V_SF_Schema') }}.period 
    where type ='Quarter'
),
 Dates AS 
(
    SELECT * FROM {{ ref('Dim_Calendar') }}
)   
,Dim_TimeFrame as(
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
    {% if var("V_SF_CRM_ETL") == 'FIVETRAN_SF' %}  'SF' {% endif %} as Source_type,
    'D_TIMEFRAME_DIM_LOAD' AS DW_SESSION_NM,
    {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS     
from
    dates d
    left join qutr_data s
    on d.cldr_date between s.start_date and s.end_date
    left join year_data y
    on d.cldr_date between y.start_date and y.end_date
order by 2)

select * from Dim_TimeFrame
