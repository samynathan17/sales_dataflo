



With user AS(
    select *  from DBT_TEST_LIVEDATA_RK.user
),usr_role AS(
    select *  from DBT_TEST_LIVEDATA_RK.user_role
),Dim_Employee AS(
 SELECT
   md5(cast(
    
    coalesce(cast(user.id as 
    varchar
), '')

 as 
    varchar
)) AS employee_id,
   ACCOUNT_ID AS emp_account_id,
   user.id AS source_Emp_id,
   cast('FIVETRAN_SF' as varchar(50)) AS Entity_id, 
   COMPANY_NAME AS org_name,
   EMPLOYEE_NUMBER AS employee_code,
   FIRST_NAME AS first_name,
   NULL AS middle_name,
   LAST_NAME AS last_name,
   user.NAME AS emp_full_nm,
   USER_ROLE_ID AS emp_role_id,
   usr_role. NAME AS emp_position_level,
   NULL  AS emp_gender,
   PHONE AS emp_phone_number,
   EMAIL AS emp_email,
   NULL AS sales_branch_id,
   NULL AS sales_branch_name,
   NULL AS sales_region_id,
   NULL AS sales_region_name,
   NULL AS sales_zone_id,
   NULL AS sales_zone_name,
   NULL AS business_unit_id,
   DEPARTMENT AS business_unit_name,
   CREATED_DATE AS emp_create_dt,
   user.LAST_MODIFIED_DATE AS emp_last_update_dt,
   MANAGER_ID AS mngr_emp_id,
   usr_role.NAME AS mngr_position_level,
   USER_ROLE_ID AS mngr_role_id,
   NULL AS emp_financial_year_start,
   NULL AS emp_start_of_week,
   NULL AS Weekly_working_days,
   IS_ACTIVE AS  emp_active,
     'SF'  as Source_type,
   'D_EMPLOYEE_DIM_LOAD' AS DW_SESSION_NM,
   
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
  FROM
      user left join usr_role  on user.USER_ROLE_ID = usr_role.ID
)

select * from Dim_Employee