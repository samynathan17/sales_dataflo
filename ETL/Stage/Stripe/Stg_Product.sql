{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'STR' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

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
        unique_key= 'PRODUCT_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_PRODUCT WHERE PRODUCT_ID IS NULL"
      )
}}


{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'STR'  %}   
      
  select
        {{ dbt_utils.surrogate_key('ID') }}  AS PRODUCT_ID,
        ID as SOURCE_ID,
        ACTIVE,
        CAPTION,
        CREATED,
        DESCRIPTION,
        LIVEMODE,
        NAME,
        PACKAGE_DIMENSIONS_HEIGHT,
        PACKAGE_DIMENSIONS_LENGTH,
        PACKAGE_DIMENSIONS_WEIGHT,
        PACKAGE_DIMENSIONS_WIDTH,
        SHIPPABLE,
        STATEMENT_DESCRIPTOR,
        TYPE,
        UNIT_LABEL,
        UPDATED,
        URL,
        IS_DELETED,
        METADATA,
        _FIVETRAN_SYNCED,
        '{{ schema_nm }}' as Entity_type,
        'D_PRODUCT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.PRODUCT
         {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as PRODUCT_ID,
        null as SOURCE_ID,
        null as ACTIVE,
        null as CAPTION,
        null as CREATED,
        null as DESCRIPTION,
        null as LIVEMODE,
        null as NAME,
        null as PACKAGE_DIMENSIONS_HEIGHT,
        null as PACKAGE_DIMENSIONS_LENGTH,
        null as PACKAGE_DIMENSIONS_WEIGHT,
        null as PACKAGE_DIMENSIONS_WIDTH,
        null as SHIPPABLE,
        null as STATEMENT_DESCRIPTOR,
        null as TYPE,
        null as UNIT_LABEL,
        null as UPDATED,
        null as URL,
        null as IS_DELETED,
        null as METADATA,
        null as _FIVETRAN_SYNCED,
        null as Entity_type,
        null AS DW_SESSION_NM,
        null AS DW_INS_UPD_DTS
    FROM dual    
    {% endif %}
{% endfor %}