



    WITH lead AS (
       select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Lead 
    ),opportunity as(
        select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Opportunity 
    ),Dim_Lead as(
    SELECT
        lead_id,
        lead.LEAD_SOURCE AS LEAD_SOURCE,
        lead.source_id as Source_ID,
        concat(lead.street,' ',lead.city,' ',lead.state,' ',lead.postal_code,' ',lead.country) AS lead_contact_address,
        lead.country AS country,
        lead.TITLE AS TITLE,
        lead.STATUS AS STATUS,
        opportunity.STAGE_NAME AS STAGE_NAME,
        NULL AS product_id,
        NULL AS campaign_id,
        lead.IS_CONVERTED AS lead_to_opp_flag,
        NULL AS lead_lost_flag,
        lead.CONVERTED_DATE AS lead_CONVERTED_DATE,
        lead.CONVERTED_OPPORTUNITY_ID AS CONVERTED_OPPORTUNITY_ID,
        NULL AS lead_lost_dt,
        NULL AS lead_lost_reason,
        lead.ANNUAL_REVENUE,
        lead.NUMBER_OF_EMPLOYEES,
        lead.INDUSTRY AS Industry,
        LEAD.owner_id AS employee_id,
        lead.CREATED_DATE as INITIAL_CREATE_DT,
        lead.LAST_MODIFIED_DATE as lead_LAST_MODIFIED_DATE,
        lead.Source_type AS Source_type,
        'D_LEAD_DIM_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
        FROM
          lead left join opportunity on lead.CONVERTED_OPPORTUNITY_ID = opportunity.Source_ID 
          and lead.Source_type = opportunity.Source_type
    )    
 
select * from Dim_Lead