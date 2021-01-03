



WITH source AS (
       select *  from DBT_TEST_LIVEDATA_RK.Contact 
    ),Dim_Address AS(
        SELECT 
            md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
)) AS  address_id,
            account_id AS account_id,
            id as source_id,
            MAILING_STREET AS STREET,
            MAILING_CITY AS CITY,
            MAILING_STATE AS STATE,
            MAILING_POSTAL_CODE AS POSTAL_CODE,
            MAILING_COUNTRY AS COUNTRY,
            'Billing' as address_type,
              'SF'  as Source_type,
             'D_ADDRESS_DIM_LOAD' AS DW_SESSION_NM,
            
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
            from source
        union 
        SELECT
            md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
)) AS  address_id,
            account_id AS account_id,
            id as source_id,
            OTHER_STREET AS STREET,
            OTHER_CITY AS CITY,
            OTHER_STATE AS STATE,
            OTHER_POSTAL_CODE AS POSTAL_CODE,
            OTHER_COUNTRY As COUNTRY, 
            'Shipping' as address_type,
              'SF'  as Source_type,
             'D_ADDRESS_DIM_LOAD' AS DW_SESSION_NM,
            
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
             from source
    )
    
select * from Dim_Address