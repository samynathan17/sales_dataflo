{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}



WITH source AS
 (
 select * from {{ ref('Stg_Goal_Conversions') }}
  ),
DIM_GOAL_CONVERSIONS as (
      select
        ID,
        DATE,
        PROFILE,
        GOAL_COMPLETION_LOCATION,
        GOAL_PREVIOUS_STEP_1,
        GOAL_PREVIOUS_STEP_2,
        GOAL_PREVIOUS_STEP_3,
        GOAL_VALUE_ALL,
        GOAL_COMPLETIONS_ALL,
        GOAL_STARTS_ALL,
        GOAL_CONVERSION_RATE_ALL,
        GOAL_ABANDON_RATE_ALL,
        GOAL_ABANDONS_ALL,
        GOAL_VALUE_PER_SESSION,
        Source_type,
        'D_GOAL_CONVERSIONS_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_GOAL_CONVERSIONS 