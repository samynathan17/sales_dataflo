{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'GSC' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'GSC'  %} 
      
  select
        {{ dbt_utils.surrogate_key('COUNTRY','DATE','PAGE') }}  AS Page_Rept_ID,
        COUNTRY,
        DATE,
        DEVICE,
        KEYWORD,
        PAGE,
        SEARCH_TYPE,
        SITE,
        CLICKS,
        IMPRESSIONS,
        CTR,
        POSITION,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_PAGE_REPORT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.KEYWORD_PAGE_REPORT
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
       NULL AS Page_Rept_ID,
       NULL AS COUNTRY,
       NULL AS DATE,
       NULL AS DEVICE,
       NULL AS KEYWORD,
       NULL AS PAGE,
       NULL AS SEARCH_TYPE,
       NULL AS SITE,
       NULL AS CLICKS,
       NULL AS IMPRESSIONS,
       NULL AS CTR,
       NULL AS POSITION,
       NULL AS _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Source_type,
        'D_PAGE_REPORT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     
    {% endif %}
{% endfor %}