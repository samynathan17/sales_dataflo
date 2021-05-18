{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'Site_Rept_ID'
      )
}}

WITH source AS
 (
 select * from {{ ref('Stg_Keyword_Site_Report_By_Site') }}
  ),
DIM_SITE as (
      select
        Site_Rept_ID,
        COUNTRY,
        DATE  as Date_day,
        DEVICE,
        KEYWORD,
        SEARCH_TYPE,
        SITE,
        CLICKS,
        IMPRESSIONS,
        CTR,
        POSITION,
        _FIVETRAN_SYNCED,
        Source_type as platform,
        'D_PAGE_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_SITE

