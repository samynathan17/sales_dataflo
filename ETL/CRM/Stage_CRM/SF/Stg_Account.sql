{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Entity_Table')~" where DATASOURCE_TYPE = 'SF' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASOURCE_ID||'#'||ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE||'#'||nvl(FIRST_TIME_LOAD,'FALSE')||'#'||nvl(HISTORY_LOAD,'FALSE')")%}

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
        unique_key= 'Account_ID',
        pre_hook = "Insert into " ~var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".High_Water_Mark(HWM_ID,HWM_Name,HMW_Description,Low_DT,active_flag,DW_SESSION_NM,DW_INS_UPD_DTS) 
                    Select nvl(MAX(HWM_ID) + 1,1),'Stg_Account','Salsforce Account table',nvl(MAX(High_DT),'1990-01-01'),'Y','LOAD_HIGH_WATER_MARK',current_timestamp from " 
                    ~var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".High_Water_Mark where HWM_Name ='Stg_Account'", 
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ACCOUNT WHERE ACCOUNT_ID IS NULL"
    )                       
}}

{{
    config(
        post_hook="UPDATE " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".HIGH_WATER_MARK 
                     set Low_DT = (SELECT MIN(CREATED_DATE) FROM "~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ACCOUNT) 
                     where Low_DT ='1990-01-01 00:00:00.000' and HWM_Name ='Stg_Account')"
           )                       
}}

{{
    config(
        post_hook="UPDATE " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".HIGH_WATER_MARK 
                     set High_DT = (SELECT MAX(CREATED_DATE) FROM "~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_ACCOUNT) 
                     where High_DT is null and HWM_Name ='Stg_Account')"             
           )                       
}}

{% set result_dates = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".HIGH_WATER_MARK
                      where HWM_Name ='Stg_Account' and HWM_ID = (select MAX(HWM_ID) from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ ".HIGH_WATER_MARK
                      where HWM_Name ='Stg_Account')", "Low_DT")%}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set app_id,schema_nm,entity_typ,firsttime,history = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

{% set startdate = result_dates %}

 {% if  entity_typ == 'SF'  %}   
    {% if  firsttime == 'true' or history== 'true' %}     
            select
            {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
            ID as Source_ID,
            IS_DELETED,
            MASTER_RECORD_ID,
            NAME,
            TYPE,
            PARENT_ID,
            BILLING_STREET,
            BILLING_CITY,
            BILLING_STATE,
            BILLING_POSTAL_CODE,
            BILLING_COUNTRY,
            SHIPPING_STREET,
            SHIPPING_CITY,
            SHIPPING_STATE,
            SHIPPING_POSTAL_CODE,
            SHIPPING_COUNTRY,
            PHONE,
            FAX,
            WEBSITE,
            SIC,
            INDUSTRY,
            ANNUAL_REVENUE,
            NUMBER_OF_EMPLOYEES,
            OWNERSHIP,
            DESCRIPTION,
            OWNER_ID,
            CREATED_DATE,
            CREATED_BY_ID,
            LAST_MODIFIED_DATE,
            LAST_MODIFIED_BY_ID,
            ACCOUNT_SOURCE,
            SIC_DESC,
            null AS CUSTOMER_TEXT_1,
            NULL AS CUSTOMER_TEXT_2,
            NULL AS CUSTOMER_TEXT_3,
            NULL AS CUSTOMER_TEXT_4,
            NULL AS CUSTOMER_TEXT_5,
            NULL AS CUSTOMER_TEXT_6,
            NULL AS CUSTOMER_NUMBER_1,
            NULL AS CUSTOMER_NUMBER_2,
            NULL AS CUSTOMER_NUMBER_3,
            NULL AS CUSTOMER_DATE_1,
            NULL AS CUSTOMER_DATE_2,
            NULL AS CUSTOMER_DATE_3,
            '{{ app_id }}' as Source_type,
            'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
            {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
        FROM {{ schema_nm }}.Account
        where CREATED_DATE between (select min(CREATED_DATE) from {{ schema_nm }}.Account) and 
        (select max(CREATED_DATE) from {{ schema_nm }}.Account)
            {% if not loop.last %}
                UNION ALL
            {% endif %}
      {% else %} 
              select
            {{ dbt_utils.surrogate_key('id') }}  AS Account_ID,
            ID as Source_ID,
            IS_DELETED,
            MASTER_RECORD_ID,
            NAME,
            TYPE,
            PARENT_ID,
            BILLING_STREET,
            BILLING_CITY,
            BILLING_STATE,
            BILLING_POSTAL_CODE,
            BILLING_COUNTRY,
            SHIPPING_STREET,
            SHIPPING_CITY,
            SHIPPING_STATE,
            SHIPPING_POSTAL_CODE,
            SHIPPING_COUNTRY,
            PHONE,
            FAX,
            WEBSITE,
            SIC,
            INDUSTRY,
            ANNUAL_REVENUE,
            NUMBER_OF_EMPLOYEES,
            OWNERSHIP,
            DESCRIPTION,
            OWNER_ID,
            CREATED_DATE,
            CREATED_BY_ID,
            LAST_MODIFIED_DATE,
            LAST_MODIFIED_BY_ID,
            ACCOUNT_SOURCE,
            SIC_DESC,
            null AS CUSTOMER_TEXT_1,
            NULL AS CUSTOMER_TEXT_2,
            NULL AS CUSTOMER_TEXT_3,
            NULL AS CUSTOMER_TEXT_4,
            NULL AS CUSTOMER_TEXT_5,
            NULL AS CUSTOMER_TEXT_6,
            NULL AS CUSTOMER_NUMBER_1,
            NULL AS CUSTOMER_NUMBER_2,
            NULL AS CUSTOMER_NUMBER_3,
            NULL AS CUSTOMER_DATE_1,
            NULL AS CUSTOMER_DATE_2,
            NULL AS CUSTOMER_DATE_3,
            '{{ app_id }}' as Source_type,
            'D_ACCOUNT_STG_LOAD' AS DW_SESSION_NM,
            {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
        FROM {{ schema_nm }}.Account
        where nvl(LAST_MODIFIED_DATE,CREATED_DATE) between '{{ startdate }}' and 
        (select max(CREATED_DATE) from {{ schema_nm }}.Account)
            {% if not loop.last %}
                UNION ALL
            {% endif %}  
      {% endif %}   
    {% elif  entity_type == 'X'  %}     
       select
        null as Account_ID,
        null as Source_ID,
        null as IS_DELETED,
        null as MASTER_RECORD_ID,
        null as NAME,
        null as TYPE,
        null as PARENT_ID,
        null as BILLING_STREET,
        null as BILLING_CITY,
        null as BILLING_STATE,
        null as BILLING_POSTAL_CODE,
        null as BILLING_COUNTRY,
        null as SHIPPING_STREET,
        null as SHIPPING_CITY,
        null as SHIPPING_STATE,
        null as SHIPPING_POSTAL_CODE,
        null as SHIPPING_COUNTRY,
        null as PHONE,
        null as FAX,
        null as WEBSITE,
        null as SIC,
        null as INDUSTRY,
        null as ANNUAL_REVENUE,
        null as NUMBER_OF_EMPLOYEES,
        null as OWNERSHIP,
        null as DESCRIPTION,
        null as OWNER_ID,
        null as CREATED_DATE,
        null as CREATED_BY_ID,
        null as LAST_MODIFIED_DATE,
        null as LAST_MODIFIED_BY_ID,
        null as ACCOUNT_SOURCE,
        null as SIC_DESC,
        null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS, 
    FROM dual    
    {% endif %}
{% endfor %}