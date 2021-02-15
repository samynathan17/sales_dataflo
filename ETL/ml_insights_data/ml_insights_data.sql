With Campaign AS(
    select *  from {{ ref('Dim_Campaign') }}
),Employee AS(
    select *  from {{ ref('Dim_Employee') }}
),Opportunity AS(

 select *  from {{ ref('Dim_Opportunity') }}

 ),lead AS(

 select *  from {{ ref('Dim_Lead') }}

 ),insight AS(

    select 
      oppo.OPPORTUNITY_ID
      ,oppo.OPPORTUNITY_NAME
      ,oppo.OPPORTUNITY_TYPE
      ,oppo.ACCOUNT_ID
      ,oppo.EMPLOYEE_ID
      ,oppo.SOURCE_ID as oppoSOURCE_ID
      ,oppo.STAGE_ID
      ,oppo.IS_WON
      ,oppo.IS_CLOSED
      ,oppo.STAGE_NAME as oppoSTAGE_NAME
      ,oppo.FORECAST_CATEGORY
      ,oppo.AMOUNT
      ,oppo.AMOUNT_WITHOUT_DISC
      ,oppo.EXPECTD_CLOUSER_DT
      ,oppo.CONTACT_ID
      ,oppo.CONTACT_NAME
      ,oppo.CONTACT_NUMBER
      ,oppo.CONTACT_EMAIL
      ,oppo.CONTACT_ADDRESS
      ,oppo.INITIAL_CREATE_DT as oppoINITIAL_CREATE_DT
      ,oppo.LAST_UPDATED_DT
      ,oppo.CLOSE_DATE
      ,oppo.PROSPECT_DT
      ,oppo.STAGE_CALC_ID
      ,oppo.STAGE_START_DT
      ,oppo.STAGE_END_DT
      ,oppo.LEAD_LOST_REASON as oppoLEAD_LOST_REASON 
      ,oppo.COMPETITOR
      ,oppo.ON_HOLD_FLAG
      ,oppo.SUB_PRODUCT_ID
      ,oppo.SUB_PRODUCT_NAME
      ,oppo.PRD_AMOUNT_WITHOUT_DISC
      ,oppo.PRD_DISCOUNT
      ,oppo.ACTIVE_FLAG as oppoACTIVE_FLAG
      ,oppo.DW_CURR_FLG
      ,oppo.EFFCT_START_DATE
      ,oppo.EFFCT_END_DATE
      ,oppo.SOURCE_TYPE as oppoSOURCE_TYPE
      ,oppo.DW_SESSION_NM
      ,oppo.DW_INS_UPD_DTS

      ,emp.EMPLOYEE_ID as empid
      ,emp.SOURCE_EMP_ID
      ,emp.ENTITY_ID
      ,emp.ORG_NAME
      ,emp.EMPLOYEE_CODE
      ,emp.FIRST_NAME
      ,emp.MIDDLE_NAME
      ,emp.LAST_NAME
      ,emp.EMP_FULL_NM
      ,emp.EMP_ROLE_ID
      ,emp.EMP_POSITION_LEVEL
      ,emp.EMP_GENDER
      ,emp.EMP_PHONE_NUMBER
      ,emp.EMP_EMAIL
      ,emp.SALES_BRANCH_ID
      ,emp.SALES_BRANCH_NAME
      ,emp.SALES_REGION_ID
      ,emp.SALES_REGION_NAME
      ,emp.SALES_ZONE_ID
      ,emp.SALES_ZONE_NAME
      ,emp.BUSINESS_UNIT_ID
      ,emp.BUSINESS_UNIT_NAME
      ,emp.EMP_CREATE_DT
      ,emp.EMP_LAST_UPDATE_DT
      ,emp.MNGR_EMP_ID
      ,emp.MNGR_POSITION_LEVEL
      ,emp.MNGR_ROLE_ID
      ,emp.EMP_FINANCIAL_YEAR_START
      ,emp.EMP_START_OF_WEEK
      ,emp.WEEKLY_WORKING_DAYS
      ,emp.EMP_ACTIVE
      ,emp.DW_SESSION_NM as empDW_SESSION_NM
      ,emp.DW_INS_UPD_DTS as empDW_INS_UPD_DTS

      ,lead.LEAD_ID
      ,lead.LEAD_SOURCE
      ,lead.SOURCE_ID as leadSOURCE_ID
      ,lead.LEAD_CONTACT_ADDRESS
      ,lead.STATUS as leadSTATUS
      ,lead.STAGE_NAME as leadSTAGE_NAME
      ,lead.PRODUCT_ID
      ,lead.LEAD_TO_OPP_FLAG
      ,lead.LEAD_LOST_FLAG
      ,lead.LEAD_CONVERTED_DATE
      ,lead.CONVERTED_OPPORTUNITY_ID
      ,lead.LEAD_LOST_DT
      ,lead.LEAD_LOST_REASON
      ,lead.INDUSTRY
      ,lead.EMPLOYEE_ID as leadEMPLOYEE_ID
      ,lead.INITIAL_CREATE_DT as leadINITIAL_CREATE_DT
      ,lead.LEAD_LAST_MODIFIED_DATE
      ,lead.SOURCE_TYPE as leadSOURCE_TYPE
      ,lead.DW_SESSION_NM as leadDW_SESSION_NM
      ,lead.DW_INS_UPD_DTS as leadDW_INS_UPD_DTS

      ,camp.CAMPAIGN_ID
      ,camp.CAMPAIGN_OWNER_ID
      ,camp.CAMPAIGN_NAME
      ,camp.ACTIVE_FLAG as campACTIVE_FLAG
      ,camp.SOURCE_ID as campSOURCE_ID
      ,camp.TYPE as campTYPE
      ,camp.STATUS as campSTATUS
      ,camp.START_DATE
      ,camp.END_DATE
      ,camp.EXPECTED_REVENUE
      ,camp.BUDGETED_COST
      ,camp.ACTUAL_COST
      ,camp.EXPECTED_RESPONSE
      ,camp.NUMBER_SENT
      ,camp.NUMBER_OF_LEADS
      ,camp.NUMBER_OF_CONVERTED_LEADS
      ,camp.NUMBER_OF_CONTACTS
      ,camp.NUMBER_OF_RESPONSES
      ,camp.NUMBER_OF_OPPORTUNITIES
      ,camp.NUMBER_OF_WON_OPPORTUNITIES
      ,camp.AMOUNT_ALL_OPPORTUNITIES
      ,camp.AMOUNT_WON_OPPORTUNITIES
      ,camp.SOURCE_TYPE as campSOURCE_TYPE
      ,camp.DW_SESSION_NM as campDW_SESSION_NM
      ,camp.DW_INS_UPD_DTS as campDW_INS_UPD_DTS
     
    from  Opportunity as oppo
    left join Employee as EMP on oppo.employee_id  =  EMP.source_Emp_id
    left join lead on oppo.employee_id = lead.employee_id
    left join Campaign as camp on oppo.EMPLOYEE_ID = camp.CAMPAIGN_OWNER_ID
    


 )
select *  from insight --where CAMPAIGN_OWNER_ID ='0055g000000Y5auAAC' 0055g000000Y5auAAC

