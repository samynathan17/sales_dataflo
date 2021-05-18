

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__carousel_media_url_tags  as
      (

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.int__facebook_ads__carousel_media_prep
  
), unnested as (

    select 
    
        base._fivetran_id,
        base.creative_id,
        base.index,
        url_tags.value:key::string as key,
        url_tags.value:value::string as value

    from base, 
    lateral flatten( input => url_tags ) as url_tags

)

select *
from unnested
      );
    