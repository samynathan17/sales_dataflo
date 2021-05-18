
with base as (

    select * 
    from {{ ref('Stg_Campaign_History_FB') }}

),

fields_xf as (
    
    select 
        id as campaign_id,
        account_id,
        name as campaign_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from base

)

select * from fields_xf
