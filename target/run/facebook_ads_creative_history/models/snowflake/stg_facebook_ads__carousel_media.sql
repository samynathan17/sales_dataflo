

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__carousel_media  as
      (

with base as (

    select *
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.int__facebook_ads__carousel_media_prep
  
), fields as (

    select 
        _fivetran_id,
        creative_id,
        caption, 
        description, 
        message,
        link,
        index
    from base

)

select *
from fields
      );
    