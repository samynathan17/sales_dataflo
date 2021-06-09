{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'FB_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% for V_SF_Schema in results %}


{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'FB_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key('AD_ID') }}  AS ADC_ID,
        POST_OBJECT,
OFFSITE_PIXEL,
OFFER_CREATOR,
PAGE_PARENT,
ACTION_TYPE,
PAGE,
EVENT_CREATOR,
EVENT_TYPE,
INDEX,
QUESTION,
APPLICATION,
DATASET,
OBJECT_DOMAIN,
OBJECT,
POST_OBJECT_WALL,
AD_ID,
LEADGEN,
EVENT,
QUESTION_CREATOR,
CREATIVE,
RESPONSE,
POST,
FB_PIXEL,
POST_WALL,
OFFER,
SUBTYPE,
AD_UPDATED_TIME,
FB_PIXEL_EVENT,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.AD_CONVERSION
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        NULL AS PAGE,
NULL AS EVENT_CREATOR,
NULL AS EVENT_TYPE,
NULL AS INDEX,
NULL AS QUESTION,
NULL AS APPLICATION,
NULL AS DATASET,
NULL AS OBJECT_DOMAIN,
NULL AS OBJECT,
NULL AS POST_OBJECT_WALL,
NULL AS AD_ID,
NULL AS LEADGEN,
NULL AS EVENT,
NULL AS QUESTION_CREATOR,
NULL AS CREATIVE,
NULL AS RESPONSE,
NULL AS POST,
NULL AS FB_PIXEL,
NULL AS POST_WALL,
NULL AS OFFER,
NULL AS SUBTYPE,
NULL AS AD_UPDATED_TIME,
NULL AS FB_PIXEL_EVENT,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}