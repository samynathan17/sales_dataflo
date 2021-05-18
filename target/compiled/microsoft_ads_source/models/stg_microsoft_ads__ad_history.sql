with base as (

    select *
    from DATAFLOTEST_DATABASE.bingads.ad_history

), fields as (

    select 
        id as ad_id,
        final_url,
        ad_group_id,
        modified_time as modified_timestamp
    from base

), url_fields as (

    select 
        *,
        
  

    split_part(
        final_url,
        '?',
        1
        )


 as base_url,
        
  
    try_cast(
  

    split_part(
        
  

    split_part(
        

    replace(
        

    replace(
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
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
        final_url,
        'utm_term=',
        2
        )


,
        '&',
        1
        )


,'') as utm_term
    from fields

), surrogate_key as (

    select 
        *,
        md5(cast(
    
    coalesce(cast(ad_id as 
    varchar
), '') || '-' || coalesce(cast(modified_timestamp as 
    varchar
), '')

 as 
    varchar
)) as ad_version_id
    from url_fields

), most_recent_record as (

    select
        *,
        row_number() over (partition by ad_id order by modified_timestamp desc) = 1 as is_most_recent_version
    from surrogate_key

)

select *
from most_recent_record