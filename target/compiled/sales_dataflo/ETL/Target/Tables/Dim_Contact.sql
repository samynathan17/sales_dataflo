



WITH contacts AS (
       select *  from DBT_TEST_LIVEDATA_RK.Contact 
    ),
Dim_Contact as(
      SELECT 
        md5(cast(
    
    coalesce(cast(contacts.id as 
    varchar
), '')

 as 
    varchar
)) AS contact_id, 
        contacts.salutation AS salutation, 
        contacts.NAME AS contact_name, 
        contacts.last_name AS last_name, 
        contacts.first_name AS first_name, 
        contacts.phone AS contact_number, 
        contacts.mobile_phone AS mobile_phone, 
        contacts.home_phone AS home_phone, 
        contacts.email AS contact_email, 
        contacts.id AS source_id, 
        contacts.department AS department, 
        contacts.lead_source AS lead_source, 
        null AS organization_id, 
        contacts.owner_ID AS employee_id, 
        contacts.account_id AS account_id, 
        NULL AS contact_age_group, 
        NULL AS contact_income, 
        NULL AS dependent, 
        NULL AS contact_type, 
        contacts.IS_DELETED AS active,
        CREATED_DATE as INITIAL_CREATE_DT,
          'SF'  as Source_type,
        'D_CONTACT_DIM_LOAD' AS dw_session_nm, 
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
     FROM 
       contacts
    )    
    
select * from Dim_Contact