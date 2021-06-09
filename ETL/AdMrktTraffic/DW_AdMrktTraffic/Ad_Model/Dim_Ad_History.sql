with base as (

    select * 
    from {{ ref('Stg_Ad_History') }}

),

fields_xf as (
    
    select 
        id as ad_id,
        account_id,
        ad_set_id,
        campaign_id,
        creative_id,
        name as ad_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from base

)

select * from fields_xf
