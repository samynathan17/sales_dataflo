{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

WITH source AS
 (
 select * from {{ ref('Stg_Geo_Network') }}
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
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_GEO_NETWORK 