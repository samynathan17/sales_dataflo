with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Creative_History

), fields as (

    select
        id as creative_id,
        last_modified_time as last_modified_at,
        created_time as created_at,
        campaign_id,
        type as creative_type,
        cast(version_tag as numeric) as version_tag,
        status as creative_status,
        click_uri
    from base

), url_fields as (

    select 
        *,
        
  

    split_part(
        click_uri,
        '?',
        1
        )


 as base_url,
        
  
    try_cast(
  

    split_part(
        
  

    split_part(
        

    replace(
        

    replace(
        click_uri,
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
        click_uri,
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
        click_uri,
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
        click_uri,
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
        click_uri,
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
        click_uri,
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
        click_uri,
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
        click_uri,
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
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_source=',
        2
        )


,
        '&',
        1
        )


,'') as utm_source,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_medium=',
        2
        )


,
        '&',
        1
        )


,'') as utm_medium,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_campaign=',
        2
        )


,
        '&',
        1
        )


,'') as utm_campaign,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_content=',
        2
        )


,
        '&',
        1
        )


,'') as utm_content,
        nullif(
  

    split_part(
        
  

    split_part(
        click_uri,
        'utm_term=',
        2
        )


,
        '&',
        1
        )


,'') as utm_term
    from fields

), valid_dates as (

    select 
        *,
        case 
            when row_number() over (partition by creative_id order by version_tag) = 1 then created_at
            else last_modified_at
        end as valid_from,
        lead(last_modified_at) over (partition by creative_id order by version_tag) as valid_to
    from url_fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(creative_id as 
    varchar
), '') || '-' || coalesce(cast(version_tag as 
    varchar
), '')

 as 
    varchar
)) as creative_version_id
    from valid_dates

)

select *
from surrogate_key