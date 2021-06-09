{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}



WITH source AS
 (
 select * from {{ ref('Stg_Events_Overview') }}
  ),
DIM_EVENTS_OVERVIEW as (
      select
        ID,
        DATE,
        PROFILE,
        EVENT_CATEGORY,
        EVENT_VALUE,
        TOTAL_EVENTS,
        SESSIONS_WITH_EVENT,
        EVENTS_PER_SESSION_WITH_EVENT,
        AVG_EVENT_VALUE,
        UNIQUE_EVENTS,
        Source_type,
        'D_EVENTS_OVERVIEW_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_EVENTS_OVERVIEW