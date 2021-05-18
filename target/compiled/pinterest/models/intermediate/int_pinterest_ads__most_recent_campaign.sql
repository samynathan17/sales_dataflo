with base as (

    select *
    from DATAFLOTEST_DATABASE.FB_ADS_ANAND_013032021.campaign_history

), row_num as (

    select 
        *,
        row_number() over (partition by campaign_id order by _fivetran_synced desc) as rn
    from base

), filtered as (

    select *
    from row_num
    where rn = 1
    
)

select *
from filtered