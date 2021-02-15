



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Adwords_Keyword
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
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_ADWORDS_KEYWORD