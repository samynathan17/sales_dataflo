



With Engagement AS(
    select *  from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Engagement     
),Dim_Engagement AS(
 SELECT
   md5(cast(
    
    coalesce(cast(ENGAGEMENT_ID as 
    varchar
), '')

 as 
    varchar
)) AS ENGAGEMENT_ID,
   Source_ID ,
   Source_type , 
   OWNER_ID as employee_id,
   TYPE,
   cast (CREATED_AT as date) AS initial_create_dt,
   cast (LAST_UPDATED as date) AS LAST_UPDATED,
   ACTIVE AS  Is_active,
   'D_ENGAGEMENT_DIM_LOAD' AS DW_SESSION_NM,
   
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS 
  FROM
      Engagement 
)

select * from Dim_Engagement