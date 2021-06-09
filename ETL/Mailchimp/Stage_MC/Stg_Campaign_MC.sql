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
        unique_key= 'Campaign_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CAMPAIGN WHERE Campaign_ID IS NULL"
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
        {{ dbt_utils.surrogate_key('id') }}  AS Campaign_ID,
        ID as Source_ID,
        TYPE,
        CREATE_TIME,
        ARCHIVE_URL,
        LONG_ARCHIVE_URL,
        STATUS,
        SEND_TIME,
        CONTENT_TYPE,
        LIST_ID,
        SEGMENT_TEXT,
        SEGMENT_ID,
        TITLE,
        TO_NAME,
        AUTHENTICATE,
        TIMEWARP,
        SUBJECT_LINE,
        FROM_NAME,
        REPLY_TO,
        USE_CONVERSATION,
        FOLDER_ID,
        AUTO_FOOTER,
        INLINE_CSS,
        AUTO_TWEET,
        FB_COMMENTS,
        TEMPLATE_ID,
        DRAG_AND_DROP,
        CLICKTALE,
        TRACK_HTML_CLICKS,
        TRACK_TEXT_CLICKS,
        TRACK_GOALS,
        TRACK_OPENS,
        TRACK_ECOMM_360,
        GOOGLE_ANALYTICS,
        _FIVETRAN_DELETED,
        WINNING_COMBINATION_ID,
        WINNING_CAMPAIGN_ID,
        WINNER_CRITERIA,
        WAIT_TIME,
        TEST_SIZE,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.CAMPAIGN
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Campaign_ID,
        null AS Source_ID,
        null AS TYPE,
        null AS CREATE_TIME,
        null AS ARCHIVE_URL,
        null AS LONG_ARCHIVE_URL,
        null AS STATUS,
        null AS SEND_TIME,
        null AS CONTENT_TYPE,
        null AS LIST_ID,
        null AS SEGMENT_TEXT,
        null AS SEGMENT_ID,
        null AS TITLE,
        null AS TO_NAME,
        null AS AUTHENTICATE,
        null AS TIMEWARP,
        null AS SUBJECT_LINE,
        null AS FROM_NAME,
        null AS REPLY_TO,
        null AS USE_CONVERSATION,
        null AS FOLDER_ID,
        null AS AUTO_FOOTER,
        null AS INLINE_CSS,
        null AS AUTO_TWEET,
        null AS FB_COMMENTS,
        null AS TEMPLATE_ID,
        null AS DRAG_AND_DROP,
        null AS CLICKTALE,
        null AS TRACK_HTML_CLICKS,
        null AS TRACK_TEXT_CLICKS,
        null AS TRACK_GOALS,
        null AS TRACK_OPENS,
        null AS TRACK_ECOMM_360,
        null AS GOOGLE_ANALYTICS,
        null AS _FIVETRAN_DELETED,
        null AS WINNING_COMBINATION_ID,
        null AS WINNING_CAMPAIGN_ID,
        null AS WINNER_CRITERIA,
        null AS WAIT_TIME,
        null AS TEST_SIZE,
        null AS _FIVETRAN_SYNCED,
        null as Source_type,
        null AS DW_SESSION_NM,   
    FROM dual    
    {% endif %}
{% endfor %}