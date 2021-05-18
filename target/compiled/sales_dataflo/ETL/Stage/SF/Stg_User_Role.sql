







 



   
select
        md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
))  AS User_Role_id,
        ID as Source_ID,
        NAME,
        PARENT_ROLE_ID,
        ROLLUP_DESCRIPTION,
        OPPORTUNITY_ACCESS_FOR_ACCOUNT_OWNER,
        CASE_ACCESS_FOR_ACCOUNT_OWNER,
        CONTACT_ACCESS_FOR_ACCOUNT_OWNER,
        FORECAST_USER_ID,
        MAY_FORECAST_MANAGER_SHARE,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        SYSTEM_MODSTAMP,
        DEVELOPER_NAME,
        PORTAL_ACCOUNT_ID,
        PORTAL_TYPE,
        PORTAL_ACCOUNT_OWNER_ID,
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
        'SF_RKLIVE_06012021' as Source_type,
        'D_USER_ROLE_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM SF_RKLIVE_06012021.user_role
              

