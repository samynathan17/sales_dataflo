
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_Group_History  as (
    






 



  
      
  select
        md5(cast(
    
    coalesce(cast(ID as 
    varchar
), '')

 as 
    varchar
))  AS CAMP_GRP_ID,
        BACKFILLED,
        STATUS,
        LAST_MODIFIED_TIME,
        RUN_SCHEDULE_START,
        ACCOUNT_ID,
        ID,
        NAME,
        RUN_SCHEDULE_END,
        CREATED_TIME,
        'LI_ADS_DATAFLO_07042021' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
    FROM LI_ADS_DATAFLO_07042021.CAMPAIGN_GROUP_HISTORY
           
        

  );
