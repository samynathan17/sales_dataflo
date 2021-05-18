with base as (

    select * 
    from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads__account_history_tmp

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
    
    
    account_status
    
 as 
    
    account_status
    
, 
    
    
    age
    
 as 
    
    age
    
, 
    
    
    agency_client_declaration_agency_representing_client
    
 as 
    
    agency_client_declaration_agency_representing_client
    
, 
    
    
    agency_client_declaration_client_based_in_france
    
 as 
    
    agency_client_declaration_client_based_in_france
    
, 
    
    
    agency_client_declaration_client_city
    
 as 
    
    agency_client_declaration_client_city
    
, 
    
    
    agency_client_declaration_client_country_code
    
 as 
    
    agency_client_declaration_client_country_code
    
, 
    
    
    agency_client_declaration_client_email_address
    
 as 
    
    agency_client_declaration_client_email_address
    
, 
    
    
    agency_client_declaration_client_name
    
 as 
    
    agency_client_declaration_client_name
    
, 
    
    
    agency_client_declaration_client_postal_code
    
 as 
    
    agency_client_declaration_client_postal_code
    
, 
    
    
    agency_client_declaration_client_province
    
 as 
    
    agency_client_declaration_client_province
    
, 
    
    
    agency_client_declaration_client_street
    
 as 
    
    agency_client_declaration_client_street
    
, 
    
    
    agency_client_declaration_client_street_2
    
 as 
    
    agency_client_declaration_client_street_2
    
, 
    
    
    agency_client_declaration_has_written_mandate_from_advertiser
    
 as 
    
    agency_client_declaration_has_written_mandate_from_advertiser
    
, 
    
    
    agency_client_declaration_is_client_paying_invoices
    
 as 
    
    agency_client_declaration_is_client_paying_invoices
    
, 
    
    
    amount_spent
    
 as 
    
    amount_spent
    
, 
    
    
    balance
    
 as 
    
    balance
    
, 
    
    
    business_city
    
 as 
    
    business_city
    
, 
    
    
    business_country_code
    
 as 
    
    business_country_code
    
, 
    
    
    business_manager_created_by
    
 as 
    
    business_manager_created_by
    
, 
    
    
    business_manager_created_time
    
 as 
    
    business_manager_created_time
    
, 
    
    
    business_manager_manager_id
    
 as 
    
    business_manager_manager_id
    
, 
    
    
    business_manager_name
    
 as 
    
    business_manager_name
    
, 
    
    
    business_manager_primary_page
    
 as 
    
    business_manager_primary_page
    
, 
    
    
    business_manager_timezone_id
    
 as 
    
    business_manager_timezone_id
    
, 
    
    
    business_manager_update_time
    
 as 
    
    business_manager_update_time
    
, 
    
    
    business_manager_updated_by
    
 as 
    
    business_manager_updated_by
    
, 
    
    
    business_name
    
 as 
    
    business_name
    
, 
    
    
    business_state
    
 as 
    
    business_state
    
, 
    
    
    business_street
    
 as 
    
    business_street
    
, 
    
    
    business_street_2
    
 as 
    
    business_street_2
    
, 
    
    
    business_zip
    
 as 
    
    business_zip
    
, 
    
    
    can_create_brand_lift_study
    
 as 
    
    can_create_brand_lift_study
    
, 
    
    
    capabilities
    
 as 
    
    capabilities
    
, 
    
    
    created_time
    
 as 
    
    created_time
    
, 
    
    
    currency
    
 as 
    
    currency
    
, 
    
    
    disable_reason
    
 as 
    
    disable_reason
    
, 
    
    
    end_advertiser
    
 as 
    
    end_advertiser
    
, 
    
    
    end_advertiser_name
    
 as 
    
    end_advertiser_name
    
, 
    
    
    has_migrated_permissions
    
 as 
    
    has_migrated_permissions
    
, 
    
    
    id
    
 as 
    
    id
    
, 
    
    
    io_number
    
 as 
    
    io_number
    
, 
    
    
    is_attribution_spec_system_default
    
 as 
    
    is_attribution_spec_system_default
    
, 
    
    
    is_direct_deals_enabled
    
 as 
    
    is_direct_deals_enabled
    
, 
    
    
    is_notifications_enabled
    
 as 
    
    is_notifications_enabled
    
, 
    
    
    is_personal
    
 as 
    
    is_personal
    
, 
    
    
    is_prepay_account
    
 as 
    
    is_prepay_account
    
, 
    
    
    is_tax_id_required
    
 as 
    
    is_tax_id_required
    
, 
    
    
    media_agency
    
 as 
    
    media_agency
    
, 
    
    
    min_campaign_group_spend_cap
    
 as 
    
    min_campaign_group_spend_cap
    
, 
    
    
    min_daily_budget
    
 as 
    
    min_daily_budget
    
, 
    
    
    name
    
 as 
    
    name
    
, 
    
    
    next_bill_date
    
 as 
    
    next_bill_date
    
, 
    
    
    offsite_pixels_tos_accepted
    
 as 
    
    offsite_pixels_tos_accepted
    
, 
    
    
    owner
    
 as 
    
    owner
    
, 
    
    
    partner
    
 as 
    
    partner
    
, 
    
    
    salesforce_invoice_group_id
    
 as 
    
    salesforce_invoice_group_id
    
, 
    
    
    spend_cap
    
 as 
    
    spend_cap
    
, 
    
    
    tax_id
    
 as 
    
    tax_id
    
, 
    
    
    tax_id_status
    
 as 
    
    tax_id_status
    
, 
    
    
    tax_id_type
    
 as 
    
    tax_id_type
    
, 
    
    
    timezone_id
    
 as 
    
    timezone_id
    
, 
    
    
    timezone_name
    
 as 
    
    timezone_name
    
, 
    
    
    timezone_offset_hours_utc
    
 as 
    
    timezone_offset_hours_utc
    



        
    from base
),

fields_xf as (
    
    select 
        id as account_id,
        name as account_name,
        row_number() over (partition by id order by _fivetran_synced desc) = 1 as is_most_recent_record
    from fields

)

select * from fields_xf