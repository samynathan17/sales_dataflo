



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Social_Media_Acquisitions
  ),
DIM_SOCIAL_MEDIA_ACQUISITIONS as (
      select
        ID,
        DATE,
        PROFILE,
        SOCIAL_NETWORK,
        SESSIONS,
        NEW_USERS,
        AVG_SESSION_DURATION,
        TRANSACTION_REVENUE,
        PAGEVIEWS_PER_SESSION,
        TRANSACTIONS,
        BOUNCE_RATE,
        PAGEVIEWS,
        PERCENT_NEW_SESSIONS,
        TRANSACTIONS_PER_SESSION,
        Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_SOCIAL_MEDIA_ACQUISITIONS