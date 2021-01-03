
/*



    WITH source AS (
        SELECT * from DBT_TEST_LIVEDATA_RK.product
    ),Dim_Product as(
        SELECT
            NULL AS account_id,
            md5(cast(
    
    coalesce(cast(id as 
    varchar
), '')

 as 
    varchar
)) AS product_id,
            ID AS Source_id,
            PRODUCT_CODE AS product_code,
            NAME AS product_name,
            IS_ACTIVE AS active_flag,
            QUANTITY_UNIT_OF_MEASURE AS Quantity_UOM,
               'SF'  as Source_type,
            'D_PRODUCT_DIM_LOAD'  AS DW_SESSION_NM,
            
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
         FROM
            source 
     
    )    
select * from Dim_Product
*/