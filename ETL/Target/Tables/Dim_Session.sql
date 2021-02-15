{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}



WITH source AS
 (
 select * from {{ ref('Stg_Session') }}
  ),
DIM_SOCIAL_MEDIA_ACQUISITIONS as (
      select
        ID,
        DATE,
        PROFILE,
        SESSION_DURATION_BUCKET,
        USER_TYPE,
        HITS,
        SESSIONS,
        SESSIONS_PER_USER,
        AVG_SESSION_DURATION,
        BOUNCES,
        SESSION_DURATION,
        BOUNCE_RATE,
        Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_DIM_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM source
    )
select * from DIM_SOCIAL_MEDIA_ACQUISITIONS