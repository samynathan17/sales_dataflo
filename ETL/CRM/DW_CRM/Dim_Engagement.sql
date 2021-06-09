{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'ENGAGEMENT_ID'
      )
}}

With Engagement AS(
    select *  from {{ ref('Stg_Engagement') }}     
),Dim_Engagement AS(
 SELECT
   {{ dbt_utils.surrogate_key('ENGAGEMENT_ID') }} AS ENGAGEMENT_ID,
   Source_ID ,
   Source_type , 
   OWNER_ID as employee_id,
   TYPE,
   cast (CREATED_AT as date) AS initial_create_dt,
   cast (LAST_UPDATED as date) AS LAST_UPDATED,
   ACTIVE AS  Is_active,
   'D_ENGAGEMENT_DIM_LOAD' AS DW_SESSION_NM,
   {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
  FROM
      Engagement 
)

select * from Dim_Engagement
