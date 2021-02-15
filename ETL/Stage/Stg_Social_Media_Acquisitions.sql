{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".DIM_MKT_ENTITY where ENTITY_TYPE = 'GA'", 'ENTITY_NAME')%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}


{% for V_SF_Schema in results %}

 {% if  V_SF_Schema[0:2] == 'GA'  %} 
      
  select
        {{ dbt_utils.surrogate_key('_FIVETRAN_ID') }}  AS ID,
        DATE,
        PROFILE,
        SOCIAL_NETWORK,
        SESSIONS,
        NEW_USERS,
        AVG_SESSION_DURATION,
        TRANSACTION_REVENUE,
        PAGEVIEWS_PER_SESSION,
        TRANSACTIONS,
        BOUNCE_RATE,
        PAGEVIEWS,
        PERCENT_NEW_SESSIONS,
        TRANSACTIONS_PER_SESSION,
       
        '{{ V_SF_Schema }}' as Source_type,
        'D_SOCIAL_MEDIA_ACQUISITIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ V_SF_Schema }}.SOCIAL_MEDIA_ACQUISITIONS
          {% if not loop.last %}
            UNION ALL
        {% endif %}        
    {% endif %}
{% endfor %}