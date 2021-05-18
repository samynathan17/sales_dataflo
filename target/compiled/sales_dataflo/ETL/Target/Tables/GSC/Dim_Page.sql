



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Page_Report
  ),
DIM_PAGE as (
      select
        Page_Rept_ID,
        COUNTRY,
        DATE  as Date_day,
        DEVICE,
        PAGE,
        SEARCH_TYPE,
        SITE,
        CLICKS,
        IMPRESSIONS,
        CTR,
        POSITION,
        _FIVETRAN_SYNCED,
        Source_type as Platform,
        'D_PAGE_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_PAGE