
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Campaign_History_FB  as (
    with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Campaign_History_FB

),

fields_xf as (
    
    select 
        campaign_id,
        account_id,
        name as campaign_name,
        row_number() over (partition by campaign_id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from base

)

select * from fields_xf
  );
