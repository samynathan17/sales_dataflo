

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_Recipient  as
      (








 



    
      
  select
        md5(cast(
    
    coalesce(cast(CAMPAIGN_ID as 
    varchar
), '')

 as 
    varchar
))  AS Campaign_Recpt_ID,
        CAMPAIGN_ID,
        MEMBER_ID,
        LIST_ID,
        COMBINATION_ID,
        _FIVETRAN_SYNCED,
        'MC_ANANDLIVE_16042021' as Source_type,
        'D_CAMPAIGN_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM MC_ANANDLIVE_16042021.CAMPAIGN_RECIPIENT
         
    

      );
    