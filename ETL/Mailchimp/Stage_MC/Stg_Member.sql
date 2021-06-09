{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'MC' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'MEMBER_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_MEMBER WHERE MEMBER_ID IS NULL"
      )
}}


{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'MC'  %}   
      
  select
        {{ dbt_utils.surrogate_key('CAMPAIGN_ID') }}  AS MEMBER_ID,
        ID as Source_ID,
        LIST_ID,
        STATUS,
        LANGUAGE,
        VIP,
        LATITUDE,
        LONGITUDE,
        GMTOFF,
        DSTOFF,
        COUNTRY_CODE,
        TIMEZONE,
        SOURCE,
        EMAIL_ADDRESS,
        UNIQUE_EMAIL_ID,
        EMAIL_TYPE,
        IP_SIGNUP,
        TIMESTAMP_SIGNUP,
        IP_OPT,
        TIMESTAMP_OPT,
        MEMBER_RATING,
        LAST_CHANGED,
        EMAIL_CLIENT,
        UNSUBSCRIBE_REASON,
        MERGE_LNAME,
        MERGE_FNAME,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_MEMBER_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.MEMBER
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as MEMBER_ID,
        null AS Source_ID,
        null AS LIST_ID,
        null AS STATUS,
        null AS LANGUAGE,
        null AS VIP,
        null AS LATITUDE,
        null AS LONGITUDE,
        null AS GMTOFF,
        null AS DSTOFF,
        null AS COUNTRY_CODE,
        null AS TIMEZONE,
        null AS SOURCE,
        null AS EMAIL_ADDRESS,
        null AS UNIQUE_EMAIL_ID,
        null AS EMAIL_TYPE,
        null AS IP_SIGNUP,
        null AS TIMESTAMP_SIGNUP,
        null AS IP_OPT,
        null AS TIMESTAMP_OPT,
        null AS MEMBER_RATING,
        null AS LAST_CHANGED,
        null AS EMAIL_CLIENT,
        null AS UNSUBSCRIBE_REASON,
        null AS MERGE_LNAME,
        null AS MERGE_FNAME,
        null AS _FIVETRAN_SYNCED,
        null as Source_type,
        null AS DW_SESSION_NM,   
    FROM dual    
    {% endif %}
{% endfor %}