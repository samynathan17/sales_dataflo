{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}



WITH source AS
 (
 select * from {{ ref('Stg_Platform_Device') }}
  ),
DIM_PLATFORM_DEVICE as (
      select
        ID,
        DATE,
        PROFILE,
        MOBILE_DEVICE_BRANDING,
        DEVICE_CATEGORY,
        MOBILE_DEVICE_MODEL,
        MOBILE_INPUT_SELECTOR,
        OPERATING_SYSTEM,
        DATA_SOURCE,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,
        Source_type,
        'D_PLATFORM_DEVICE_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_PLATFORM_DEVICE