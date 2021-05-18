








 



    
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS PRODUCT_ID,
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
        'STRIPE_RKLIVE_01042021' as Entity_type,
        'D_PRODUCT_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM STRIPE_RKLIVE_01042021.PRODUCT
         
    
