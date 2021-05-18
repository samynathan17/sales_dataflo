
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Geo_Network  as (
    

WITH source AS
 (
 select * from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Geo_Network
  ),
DIM_GEO_NETWORK as (
      select
        ID,
        DATE,
        PROFILE,
        CONTINENT,
        COUNTRY,
        CITY,
        METRO,
        REGION,
        NETWORK_LOCATION,
        SESSIONS,
        USERS,
        Source_type,
        'D_GEO_NETWORK_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_GEO_NETWORK
  );
