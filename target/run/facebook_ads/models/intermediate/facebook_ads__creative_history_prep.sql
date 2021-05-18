
  create or replace  view DATAFLOTEST_DATABASE.dbt_salesdataflo.facebook_ads__creative_history_prep  as (
    

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__creative_history
    where is_most_recent_record = true

), url_tags as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__url_tag

), url_tags_pivoted as (

    select 
        _fivetran_id,
        creative_id,
        min(case when key = 'utm_source' then value end) as utm_source,
        min(case when key = 'utm_medium' then value end) as utm_medium,
        min(case when key = 'utm_campaign' then value end) as utm_campaign,
        min(case when key = 'utm_content' then value end) as utm_content,
        min(case when key = 'utm_term' then value end) as utm_term
    from url_tags
    group by 1,2

), fields as (

    select
        _fivetran_id,
        creative_id,
        account_id,
        creative_name,
        coalesce(page_link,template_page_link) as url,
        
  

    split_part(
        coalesce(page_link,template_page_link),
        '?',
        1
        )


 as base_url,
        
  
    try_cast(
  

    split_part(
        
  

    split_part(
        

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    


,
        '/',
        1
        )


,
        '?',
        1
        )


 as 
    varchar
)

 as url_host,
        '/' || 
  
    try_cast(
  

    split_part(
        

    case when 
    
    length(
        

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
-coalesce(
            nullif(

    position(
        '/' in 

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
, 0),
            

    position(
        '?' in 

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
 - 1
            ) = 0 
        then ''
    else 
        right(
            

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    


,
            
    
    length(
        

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
-coalesce(
            nullif(

    position(
        '/' in 

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
, 0),
            

    position(
        '?' in 

    replace(
        

    replace(
        coalesce(page_link,template_page_link),
        'http://',
        ''
    )
    


,
        'https://',
        ''
    )
    



    )
 - 1
            )
        )
    end
,
        '?',
        1
        )


 as 
    varchar
)

 as url_path,
        coalesce(url_tags_pivoted.utm_source, nullif(
  

    split_part(
        
  

    split_part(
        coalesce(page_link,template_page_link),
        'utm_source=',
        2
        )


,
        '&',
        1
        )


,'')) as utm_source,
        coalesce(url_tags_pivoted.utm_medium, nullif(
  

    split_part(
        
  

    split_part(
        coalesce(page_link,template_page_link),
        'utm_medium=',
        2
        )


,
        '&',
        1
        )


,'')) as utm_medium,
        coalesce(url_tags_pivoted.utm_campaign, nullif(
  

    split_part(
        
  

    split_part(
        coalesce(page_link,template_page_link),
        'utm_campaign=',
        2
        )


,
        '&',
        1
        )


,'')) as utm_campaign,
        coalesce(url_tags_pivoted.utm_content, nullif(
  

    split_part(
        
  

    split_part(
        coalesce(page_link,template_page_link),
        'utm_content=',
        2
        )


,
        '&',
        1
        )


,'')) as utm_content,
        coalesce(url_tags_pivoted.utm_term, nullif(
  

    split_part(
        
  

    split_part(
        coalesce(page_link,template_page_link),
        'utm_term=',
        2
        )


,
        '&',
        1
        )


,'')) as utm_term
    from base
    left join url_tags_pivoted
        using (_fivetran_id, creative_id)

)

select *
from fields
  );
