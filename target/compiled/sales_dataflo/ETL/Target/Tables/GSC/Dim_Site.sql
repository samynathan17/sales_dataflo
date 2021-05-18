



WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Site_Report_By_Site
  ),
DIM_SITE as (
      select
        Site_Rept_ID,
        COUNTRY,
        DATE as Date_day,
        DEVICE,
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
select * from DIM_SITE