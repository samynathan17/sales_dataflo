{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}



WITH source AS
 (
 select * from {{ ref('Stg_Adwords_Keyword') }}
  ),
DIM_ADWORDS_KEYWORD as (
      select
        ID,
        DATE,
        PROFILE,
        KEYWORD,
        GOAL_VALUE_ALL,
        SESSIONS,
        GOAL_COMPLETIONS_ALL,
        PAGEVIEWS_PER_SESSION,
        GOAL_CONVERSION_RATE_ALL,
        USERS,
        BOUNCE_RATE,
        AD_CLICKS,
        AD_COST,
        CPC,
        Source_type,
        'D_ADWORDS_KEYWORD_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_ADWORDS_KEYWORD