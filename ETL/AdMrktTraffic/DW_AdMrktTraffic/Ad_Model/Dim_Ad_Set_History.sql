
with base as (

    select * 
    from {{ ref('Stg_Ad_Set_History') }}

),

fields_xf as (
    
    select 
        ad_set_id,
        account_id,
        campaign_id,
        ad_set_name,
        row_number() over (partition by ad_set_id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from base

)

select * from fields_xf
