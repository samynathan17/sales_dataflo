with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__basic_ad_tmp

),

fields as (

    select
        
    
    
    _fivetran_id
    
 as 
    
    _fivetran_id
    
, 
    
    
    _fivetran_synced
    
 as 
    
    _fivetran_synced
    
, 
    
    
    account_id
    
 as 
    
    account_id
    
, 
    
    
    ad_id
    
 as 
    
    ad_id
    
, 
    
    
    ad_name
    
 as 
    
    ad_name
    
, 
    
    
    adset_name
    
 as 
    
    adset_name
    
, 
    
    
    cpc
    
 as 
    
    cpc
    
, 
    
    
    cpm
    
 as 
    
    cpm
    
, 
    
    
    ctr
    
 as 
    
    ctr
    
, 
    
    
    date
    
 as 
    
    date
    
, 
    
    
    frequency
    
 as 
    
    frequency
    
, 
    
    
    impressions
    
 as 
    
    impressions
    
, 
    
    
    inline_link_clicks
    
 as 
    
    inline_link_clicks
    
, 
    
    
    reach
    
 as 
    
    reach
    
, 
    
    
    spend
    
 as 
    
    spend
    



        
    from base
),

final as (
    
    select 
        ad_id,
        date as date_day,
        account_id,
        impressions,
        inline_link_clicks as clicks,
        spend
    from fields
)

select * from final