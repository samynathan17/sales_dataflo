



(
WITH OPPORTUNITY AS (
       select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity  
    ),contact AS(
        select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Contact    
    ),
    emp AS(
        select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_User    
    ),
    OPPORTUNITY_STAGE AS(
        select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity_Stage   
    )
    ,Dim_Opportunity as(
    SELECT
        opportunity_id,
        OPPORTUNITY.NAME AS opportunity_NAME,
        OPPORTUNITY.TYPE AS opportunity_Type, 
        cast ( OPPORTUNITY.ACCOUNT_ID as varchar(50)) AS ACCOUNT_ID,        
        cast ( OPPORTUNITY.OWNER_ID  as varchar(50)) AS employee_id,
        cast ( OPPORTUNITY.Source_id as varchar(50)) as Source_id,
        OPPORTUNITY_STAGE.SORT_ORDER AS stage_id,
        OPPORTUNITY.IS_WON as IS_WON,
        OPPORTUNITY.IS_CLOSED as IS_CLOSED,
        OPPORTUNITY.stage_name AS stage_name,
        --   OPPORTUNITY.PROBABILITY as PROBABILITY,        
        OPPORTUNITY.FORECAST_CATEGORY as FORECAST_CATEGORY,
        OPPORTUNITY.AMOUNT AS AMOUNT,
        NULL AS amount_without_disc,
        NULL AS expectd_Clouser_Dt,
        OPPORTUNITY.CONTACT_ID AS Contact_id,
        contact.NAME AS contact_name,
        contact.PHONE AS contact_number,
        contact.EMAIL AS contact_email,
        contact.MAILING_STREET AS contact_address,
        OPPORTUNITY.CREATED_DATE AS initial_create_dt,
        OPPORTUNITY.LAST_MODIFIED_DATE AS last_updated_dt,
        OPPORTUNITY.CLOSE_DATE AS CLOSE_DATE,
        NULL AS prospect_Dt,
        NULL AS stage_calc_id,
        OPPORTUNITY_STAGE.CREATED_DATE AS stage_start_dt,
        NULL AS stage_end_dt,        
        NULL AS lead_lost_reason,
        NULL AS competitor,
        NULL AS on_hold_flag,
        NULL AS sub_product_id,
        NULL AS sub_product_name,
        NULL AS prd_amount_without_disc,
        NULL AS prd_discount,
        OPPORTUNITY. IS_DELETED AS active_flag,
        NULL AS DW_CURR_FLG,
        NULL AS EFFCT_START_DATE,
        NULL AS EFFCT_END_DATE,
        OPPORTUNITY.Source_type as Source_type,
        'D_OPPORTUNITY_DIM_LOAD'  AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS
      FROM
        OPPORTUNITY 
        left join emp on OPPORTUNITY.OWNER_ID  =  emp.Source_ID
        left join contact on emp.contact_id  =  contact.Source_ID
        left join OPPORTUNITY_STAGE on OPPORTUNITY.stage_name = OPPORTUNITY_STAGE.MASTER_LABEL 
        and OPPORTUNITY_STAGE.Source_type = OPPORTUNITY.Source_type
    ) 
select * from Dim_Opportunity )