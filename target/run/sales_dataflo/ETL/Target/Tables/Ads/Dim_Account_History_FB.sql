
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Account_History_FB  as (
    with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Account_History_FB

),

fields_xf as (
    
    select 
        id as account_id,
        name as account_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from base

)

select * from fields_xf
  );
