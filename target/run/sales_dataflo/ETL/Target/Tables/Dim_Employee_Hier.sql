

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee_Hier  as
      (

 select entity_id,
 CONNECT_BY_ROOT source_Emp_id "Manager",
 CONNECT_BY_ROOT emp_full_nm "Manager_Name",
 LEVEL as hierarchy_level,
 source_Emp_id as employee_id,
 emp_full_nm as employee_Name,
 mngr_emp_id as Manager_id,
 emp_role_id ,
 emp_position_level,
 SYS_CONNECT_BY_PATH(emp_full_nm, '/') "Hierarachy"
 from  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Employee  
 START WITH mngr_emp_id is null
 CONNECT BY PRIOR employee_id = mngr_emp_id
      );
    