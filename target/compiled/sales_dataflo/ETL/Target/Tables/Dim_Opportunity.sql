



    WITH OPPORTUNITY AS (
       select *  from DBT_TEST_LIVEDATA_RK.opportunity 
    ),contact AS(
        select *  from DBT_TEST_LIVEDATA_RK.contact    
    ),
    emp AS(
        select *  from DBT_TEST_LIVEDATA_RK.user    
    ),
    --OPPORTUNITY_LINE_ITEM AS(
    --      select OPPORTUNITY_ID,PRODUCT_2_ID ,PRODUCT_CODE,TOTAL_PRICE from DBT_TEST_LIVEDATA_RK.OPPORTUNITY_LINE_ITEM
     --),
    OPPORTUNITY_STAGE AS(
        select *  from DBT_TEST_LIVEDATA_RK.opportunity_stage  
    )
    ,Dim_Opportunity as(
    SELECT
        md5(cast(
    
    coalesce(cast(OPPORTUNITY.id as 
    varchar
), '')

 as 
    varchar
)) AS opportunity_id,
        OPPORTUNITY.NAME AS opportunity_NAME,
        OPPORTUNITY.TYPE AS opportunity_Type, 
        OPPORTUNITY.ACCOUNT_ID AS ACCOUNT_ID,        
        OPPORTUNITY.OWNER_ID AS employee_id,
        OPPORTUNITY.ID AS Source_id,
        OPPORTUNITY_STAGE.SORT_ORDER AS stage_id,
        OPPORTUNITY.IS_WON as IS_WON,
        OPPORTUNITY.IS_CLOSED as IS_CLOSED,
        OPPORTUNITY.stage_name AS stage_name,
        --   OPPORTUNITY.PROBABILITY as PROBABILITY,        
        OPPORTUNITY.FORECAST_CATEGORY as FORECAST_CATEGORY,
        OPPORTUNITY.AMOUNT AS AMOUNT,
        --OPPORTUNITY.EXPECTED_REVENUE as EXPECTED_REVENUE,
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
        --OPPORTUNITY.CAMPAIGN_ID AS campaign_id,
        NULL AS competitor,
        NULL AS on_hold_flag,
        --OPPORTUNITY_LINE_ITEM. PRODUCT_2_ID AS product_id,
        --OPPORTUNITY_LINE_ITEM. PRODUCT_CODE AS product_name,
        NULL AS sub_product_id,
        NULL AS sub_product_name,
        --OPPORTUNITY_LINE_ITEM. TOTAL_PRICE AS product_amount,
        NULL AS prd_amount_without_disc,
        NULL AS prd_discount,
        OPPORTUNITY. IS_DELETED AS active_flag,
        NULL AS DW_CURR_FLG,
        NULL AS EFFCT_START_DATE,
        NULL AS EFFCT_END_DATE,
          'SF'  as Source_type,
        'D_OPPORTUNITY_DIM_LOAD'  AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
      FROM
        OPPORTUNITY 
        left join emp on OPPORTUNITY.Owner_id  =  emp.id
        left join contact on emp.contact_id  =  contact.id
        --left join OPPORTUNITY_LINE_ITEM on OPPORTUNITY.ID = OPPORTUNITY_LINE_ITEM.OPPORTUNITY_ID
        left join OPPORTUNITY_STAGE on OPPORTUNITY.stage_name = OPPORTUNITY_STAGE.MASTER_LABEL
     
    )    
    

select * from Dim_Opportunity