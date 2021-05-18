


WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Tracking
  ),
DIM_PAGE_TRACKING as (
      select
        ID,
        DATE,
        PROFILE,
        PAGE_TITLE,
        LANDING_PAGE_PATH,
        PAGE_PATH,
        EXIT_PAGE_PATH,
        PAGE_VALUE,
        EXIT_RATE,
        TIME_ON_PAGE,
        PAGEVIEWS_PER_SESSION,
        UNIQUE_PAGEVIEWS,
        ENTRANCE_RATE,
        Source_type,
        'D_PAGE_TRACKING_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_PAGE_TRACKING