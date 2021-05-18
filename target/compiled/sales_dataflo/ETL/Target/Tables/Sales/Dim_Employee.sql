


(With user AS(
    select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_User
),usr_role AS(
    select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_User_Role
),Dim_Employee AS(
 SELECT
   cast(user.Source_ID || user.Source_type as varchar(100)) AS employee_id,
   cast(user.Source_ID as varchar(100)) AS source_Emp_id,
   user.Source_type AS Entity_id, 
   Null AS org_name,
   NULL AS employee_code,
   FIRST_NAME AS first_name,
   NULL AS middle_name,
   LAST_NAME AS last_name,
   NULL AS emp_full_nm,
   NULL AS emp_role_id,
   NULL AS emp_position_level,
   NULL  AS emp_gender,
   NULL AS emp_phone_number,
   EMAIL AS emp_email,
   NULL AS sales_branch_id,
   NULL AS sales_branch_name,
   NULL AS sales_region_id,
   NULL AS sales_region_name,
   NULL AS sales_zone_id,
   NULL AS sales_zone_name,
   NULL AS business_unit_id,
   NULL AS business_unit_name,
   NULL AS emp_create_dt,
   NULL AS emp_last_update_dt,
   NULL AS mngr_emp_id,
   NULL AS mngr_position_level,
   NULL AS mngr_role_id,
   NULL AS emp_financial_year_start,
   NULL AS emp_start_of_week,
   NULL AS Weekly_working_days,
   IS_ACTIVE AS  emp_active,
   'D_EMPLOYEE_DIM_LOAD' AS DW_SESSION_NM,
   
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
  FROM
      user left join usr_role  on user.USER_ROLE_ID = usr_role.Source_ID
      and user.source_type = usr_role.source_type
)

select * from Dim_Employee)

union all

(With user AS(
    select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Owner
),Dim_Employee AS(
 SELECT
   cast(Source_OWNER_ID || Source_type  as varchar(100)) AS employee_id,
   cast (Source_OWNER_ID as varchar(100)) AS source_Emp_id,
   Source_type AS Entity_id, 
   Null AS org_name,
   NULL AS employee_code,
   FIRST_NAME AS first_name,
   NULL AS middle_name,
   LAST_NAME AS last_name,
   NULL AS emp_full_nm,
   NULL AS emp_role_id,
   NULL AS emp_position_level,
   NULL  AS emp_gender,
   NULL AS emp_phone_number,
   EMAIL AS emp_email,
   NULL AS sales_branch_id,
   NULL AS sales_branch_name,
   NULL AS sales_region_id,
   NULL AS sales_region_name,
   NULL AS sales_zone_id,
   NULL AS sales_zone_name,
   NULL AS business_unit_id,
   NULL AS business_unit_name,
   NULL AS emp_create_dt,
   NULL AS emp_last_update_dt,
   NULL AS mngr_emp_id,
   NULL AS mngr_position_level,
   NULL AS mngr_role_id,
   NULL AS emp_financial_year_start,
   NULL AS emp_start_of_week,
   NULL AS Weekly_working_days,
   IS_ACTIVE AS  emp_active,
   'D_EMPLOYEE_DIM_LOAD' AS DW_SESSION_NM,
   
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
  FROM
      user
)

select * from Dim_Employee)