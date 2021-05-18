

with base as (

  select *
  from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__creative_history

), required_fields as (

  select 
    _fivetran_id, 
    creative_id, 
    parse_json(template_app_link_spec_ios) as template_app_link_spec_ios,
    parse_json(template_app_link_spec_ipad) as template_app_link_spec_ipad,
    parse_json(template_app_link_spec_android) as template_app_link_spec_android,
    parse_json(template_app_link_spec_iphone) as template_app_link_spec_iphone
  from base


  
), flattened_ios as (

  select 
    _fivetran_id,
    creative_id,
    'ios' as app_type,
    element.value:index::string as index,
    element.value:app_name::string as app_name,
    element.value:app_store_id::string as app_store_id,
    element.value:class_name::string as class_name,
    element.value:package_name::string as package_name,
    element.value:template_page::string as template_page
  from required_fields,
  lateral flatten( input => template_app_link_spec_ios ) as element


  
), flattened_ipad as (

  select 
    _fivetran_id,
    creative_id,
    'ipad' as app_type,
    element.value:index::string as index,
    element.value:app_name::string as app_name,
    element.value:app_store_id::string as app_store_id,
    element.value:class_name::string as class_name,
    element.value:package_name::string as package_name,
    element.value:template_page::string as template_page
  from required_fields,
  lateral flatten( input => template_app_link_spec_ipad ) as element


  
), flattened_android as (

  select 
    _fivetran_id,
    creative_id,
    'android' as app_type,
    element.value:index::string as index,
    element.value:app_name::string as app_name,
    element.value:app_store_id::string as app_store_id,
    element.value:class_name::string as class_name,
    element.value:package_name::string as package_name,
    element.value:template_page::string as template_page
  from required_fields,
  lateral flatten( input => template_app_link_spec_android ) as element


  
), flattened_iphone as (

  select 
    _fivetran_id,
    creative_id,
    'iphone' as app_type,
    element.value:index::string as index,
    element.value:app_name::string as app_name,
    element.value:app_store_id::string as app_store_id,
    element.value:class_name::string as class_name,
    element.value:package_name::string as package_name,
    element.value:template_page::string as template_page
  from required_fields,
  lateral flatten( input => template_app_link_spec_iphone ) as element



), unioned as (

    select * from flattened_ios
    union all
    select * from flattened_iphone
    union all
    select * from flattened_ipad
    union all
    select * from flattened_android

)

select *
from unioned