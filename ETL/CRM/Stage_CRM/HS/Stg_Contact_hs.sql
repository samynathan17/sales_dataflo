{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Sales')~" where DATASOURCE_TYPE = 'HS'  and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false             
    )
}}
{% endif %}

{{
    config(
        materialized='incremental',
        unique_key= 'Contact_ID',
        post_hook="DELETE FROM " ~ var('V_DB') ~ "." ~ var('V_Schema')~ ".STG_CONTACT_HS WHERE CONTACT_ID IS NULL"
      )
}}

{% for V_SF_Schema in results %}

{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'HS'  %}     
   select
        {{ dbt_utils.surrogate_key('id') }}  AS Contact_ID,
        ID as Source_ID,
        CANONICAL_VID,
        MERGED_VIDS,
        PROFILE_URL,
        _FIVETRAN_DELETED,
        PROPERTY_COMPANY_SIZE,
        PROPERTY_DATE_OF_BIRTH,
        PROPERTY_DEGREE,
        PROPERTY_FIELD_OF_STUDY,
        PROPERTY_GENDER,
        PROPERTY_GRADUATION_DATE,
        PROPERTY_HS_ANALYTICS_FIRST_TOUCH_CONVERTING_CAMPAIGN,
        PROPERTY_HS_ANALYTICS_LAST_TOUCH_CONVERTING_CAMPAIGN,
        PROPERTY_HS_AVATAR_FILEMANAGER_KEY,
        PROPERTY_HS_BUYING_ROLE,
        PROPERTY_HS_CONTENT_MEMBERSHIP_NOTES,
        PROPERTY_HS_CONTENT_MEMBERSHIP_REGISTRATION_DOMAIN_SENT_TO,
        PROPERTY_HS_CONTENT_MEMBERSHIP_STATUS,
        PROPERTY_HS_CONVERSATIONS_VISITOR_EMAIL,
        PROPERTY_HS_EMAIL_CUSTOMER_QUARANTINED_REASON,
        PROPERTY_HS_EMAIL_HARD_BOUNCE_REASON,
        PROPERTY_HS_EMAIL_HARD_BOUNCE_REASON_ENUM,
        PROPERTY_HS_EMAIL_QUARANTINED_REASON,
        PROPERTY_HS_EMAILCONFIRMATIONSTATUS,
        PROPERTY_HS_FACEBOOK_CLICK_ID,
        PROPERTY_HS_FACEBOOKID,
        PROPERTY_HS_FEEDBACK_LAST_NPS_FOLLOW_UP,
        PROPERTY_HS_FEEDBACK_LAST_NPS_RATING,
        PROPERTY_HS_GOOGLE_CLICK_ID,
        PROPERTY_HS_GOOGLEPLUSID,
        PROPERTY_HS_IP_TIMEZONE,
        PROPERTY_HS_LEAD_STATUS,
        PROPERTY_HS_LEGAL_BASIS,
        PROPERTY_HS_LINKEDINID,
        PROPERTY_HS_MARKETABLE_REASON_ID,
        PROPERTY_HS_MARKETABLE_REASON_TYPE,
        PROPERTY_HS_MARKETABLE_STATUS,
        PROPERTY_HS_MARKETABLE_UNTIL_RENEWAL,
        PROPERTY_HS_MERGED_OBJECT_IDS,
        PROPERTY_HS_PREDICTIVESCORINGTIER,
        PROPERTY_HS_TESTPURGE,
        PROPERTY_HS_TESTROLLBACK,
        PROPERTY_HS_TWITTERID,
        PROPERTY_HS_USER_IDS_OF_ALL_OWNERS,
        PROPERTY_IP_CITY,
        PROPERTY_IP_COUNTRY,
        PROPERTY_IP_COUNTRY_CODE,
        PROPERTY_IP_LATLON,
        PROPERTY_IP_STATE,
        PROPERTY_IP_STATE_CODE,
        PROPERTY_IP_ZIPCODE,
        PROPERTY_JOB_FUNCTION,
        PROPERTY_MARITAL_STATUS,
        PROPERTY_MILITARY_STATUS,
        PROPERTY_RELATIONSHIP_STATUS,
        PROPERTY_SCHOOL,
        PROPERTY_SENIORITY,
        PROPERTY_START_DATE,
        PROPERTY_WORK_EMAIL,
        PROPERTY_FIRSTNAME,
        PROPERTY_HS_ANALYTICS_FIRST_URL,
        PROPERTY_TWITTERHANDLE,
        PROPERTY_CURRENTLYINWORKFLOW,
        PROPERTY_HS_ANALYTICS_LAST_URL,
        PROPERTY_LASTNAME,
        PROPERTY_SALUTATION,
        PROPERTY_TWITTERPROFILEPHOTO,
        PROPERTY_EMAIL,
        PROPERTY_HS_PERSONA,
        PROPERTY_MOBILEPHONE,
        PROPERTY_PHONE,
        PROPERTY_FAX,
        PROPERTY_HS_EMAIL_LAST_EMAIL_NAME,
        PROPERTY_ADDRESS,
        PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_CAMPAIGN,
        PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_MEDIUM,
        PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_SOURCE,
        PROPERTY_HUBSPOT_OWNER_ID,
        PROPERTY_OWNEREMAIL,
        PROPERTY_OWNERNAME,
        PROPERTY_CITY,
        PROPERTY_HUBSPOT_TEAM_ID,
        PROPERTY_LINKEDINBIO,
        PROPERTY_TWITTERBIO,
        PROPERTY_HS_ALL_OWNER_IDS,
        PROPERTY_STATE,
        PROPERTY_HS_ALL_TEAM_IDS,
        PROPERTY_HS_ANALYTICS_SOURCE,
        PROPERTY_ZIP,
        PROPERTY_COUNTRY,
        PROPERTY_HS_ALL_ACCESSIBLE_TEAM_IDS,
        PROPERTY_HS_ANALYTICS_SOURCE_DATA_1,
        PROPERTY_HS_ANALYTICS_SOURCE_DATA_2,
        PROPERTY_HS_LANGUAGE,
        PROPERTY_HS_ANALYTICS_FIRST_REFERRER,
        PROPERTY_JOBTITLE,
        PROPERTY_PHOTO,
        PROPERTY_HS_ANALYTICS_LAST_REFERRER,
        PROPERTY_MESSAGE,
        PROPERTY_LIFECYCLESTAGE,
        PROPERTY_COMPANY,
        PROPERTY_WEBSITE,
        PROPERTY_NUMEMPLOYEES,
        PROPERTY_ANNUALREVENUE,
        PROPERTY_INDUSTRY,
        PROPERTY_HS_PREDICTIVECONTACTSCOREBUCKET,
        PROPERTY_HS_ANALYTICS_REVENUE,
        PROPERTY_HS_IS_UNWORKED,
        PROPERTY_HS_ANALYTICS_AVERAGE_PAGE_VIEWS,
        PROPERTY_ASSOCIATEDCOMPANYID,
        PROPERTY_HS_ANALYTICS_NUM_EVENT_COMPLETIONS,
        PROPERTY_HS_ANALYTICS_FIRST_TIMESTAMP,
        PROPERTY_HS_SOCIAL_FACEBOOK_CLICKS,
        PROPERTY_HS_SOCIAL_GOOGLE_PLUS_CLICKS,
        PROPERTY_HS_SOCIAL_LINKEDIN_CLICKS,
        PROPERTY_HS_ANALYTICS_NUM_PAGE_VIEWS,
        PROPERTY_LASTMODIFIEDDATE,
        PROPERTY_HS_SOCIAL_NUM_BROADCAST_CLICKS,
        PROPERTY_HS_ANALYTICS_NUM_VISITS,
        PROPERTY_HS_LIFECYCLESTAGE_LEAD_DATE,
        PROPERTY_CREATEDATE,
        PROPERTY_HS_SOCIAL_TWITTER_CLICKS,
        PROPERTY_HS_COUNT_IS_WORKED,
        PROPERTY_HS_COUNT_IS_UNWORKED,
        PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        PROPERTY_HS_LIFECYCLESTAGE_SUBSCRIBER_DATE,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3,        
        '{{ schema_nm }}' as Source_type,
        'D_CONTACT_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS    
    FROM {{ schema_nm }}.Contact
    {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% elif  V_SF_Schema[0:1] == 'X'  %}     
       select
        null as Contact_ID,
        null as  Source_ID,
        null as CANONICAL_VID,
        null as MERGED_VIDS,
        null as PROFILE_URL,
        null as _FIVETRAN_DELETED,
        null as PROPERTY_COMPANY_SIZE,
        null as PROPERTY_DATE_OF_BIRTH,
        null as PROPERTY_DEGREE,
        null as PROPERTY_FIELD_OF_STUDY,
        null as PROPERTY_GENDER,
        null as PROPERTY_GRADUATION_DATE,
        null as PROPERTY_HS_ANALYTICS_FIRST_TOUCH_CONVERTING_CAMPAIGN,
        null as PROPERTY_HS_ANALYTICS_LAST_TOUCH_CONVERTING_CAMPAIGN,
        null as PROPERTY_HS_AVATAR_FILEMANAGER_KEY,
        null as PROPERTY_HS_BUYING_ROLE,
        null as PROPERTY_HS_CONTENT_MEMBERSHIP_NOTES,
        null as PROPERTY_HS_CONTENT_MEMBERSHIP_REGISTRATION_DOMAIN_SENT_TO,
        null as PROPERTY_HS_CONTENT_MEMBERSHIP_STATUS,
        null as PROPERTY_HS_CONVERSATIONS_VISITOR_EMAIL,
        null as PROPERTY_HS_EMAIL_CUSTOMER_QUARANTINED_REASON,
        null as PROPERTY_HS_EMAIL_HARD_BOUNCE_REASON,
        null as PROPERTY_HS_EMAIL_HARD_BOUNCE_REASON_ENUM,
        null as PROPERTY_HS_EMAIL_QUARANTINED_REASON,
        null as PROPERTY_HS_EMAILCONFIRMATIONSTATUS,
        null as PROPERTY_HS_FACEBOOK_CLICK_ID,
        null as PROPERTY_HS_FACEBOOKID,
        null as PROPERTY_HS_FEEDBACK_LAST_NPS_FOLLOW_UP,
        null as PROPERTY_HS_FEEDBACK_LAST_NPS_RATING,
        null as PROPERTY_HS_GOOGLE_CLICK_ID,
        null as PROPERTY_HS_GOOGLEPLUSID,
        null as PROPERTY_HS_IP_TIMEZONE,
        null as PROPERTY_HS_LEAD_STATUS,
        null as PROPERTY_HS_LEGAL_BASIS,
        null as PROPERTY_HS_LINKEDINID,
        null as PROPERTY_HS_MARKETABLE_REASON_ID,
        null as PROPERTY_HS_MARKETABLE_REASON_TYPE,
        null as PROPERTY_HS_MARKETABLE_STATUS,
        null as PROPERTY_HS_MARKETABLE_UNTIL_RENEWAL,
        null as PROPERTY_HS_MERGED_OBJECT_IDS,
        null as PROPERTY_HS_PREDICTIVESCORINGTIER,
        null as PROPERTY_HS_TESTPURGE,
        null as PROPERTY_HS_TESTROLLBACK,
        null as PROPERTY_HS_TWITTERID,
        null as PROPERTY_HS_USER_IDS_OF_ALL_OWNERS,
        null as PROPERTY_IP_CITY,
        null as PROPERTY_IP_COUNTRY,
        null as PROPERTY_IP_COUNTRY_CODE,
        null as PROPERTY_IP_LATLON,
        null as PROPERTY_IP_STATE,
        null as PROPERTY_IP_STATE_CODE,
        null as PROPERTY_IP_ZIPCODE,
        null as PROPERTY_JOB_FUNCTION,
        null as PROPERTY_MARITAL_STATUS,
        null as PROPERTY_MILITARY_STATUS,
        null as PROPERTY_RELATIONSHIP_STATUS,
        null as PROPERTY_SCHOOL,
        null as PROPERTY_SENIORITY,
        null as PROPERTY_START_DATE,
        null as PROPERTY_WORK_EMAIL,
        null as PROPERTY_FIRSTNAME,
        null as PROPERTY_HS_ANALYTICS_FIRST_URL,
        null as PROPERTY_TWITTERHANDLE,
        null as PROPERTY_CURRENTLYINWORKFLOW,
        null as PROPERTY_HS_ANALYTICS_LAST_URL,
        null as PROPERTY_LASTNAME,
        null as PROPERTY_SALUTATION,
        null as PROPERTY_TWITTERPROFILEPHOTO,
        null as PROPERTY_EMAIL,
        null as PROPERTY_HS_PERSONA,
        null as PROPERTY_MOBILEPHONE,
        null as PROPERTY_PHONE,
        null as PROPERTY_FAX,
        null as PROPERTY_HS_EMAIL_LAST_EMAIL_NAME,
        null as PROPERTY_ADDRESS,
        null as PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_CAMPAIGN,
        null as PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_MEDIUM,
        null as PROPERTY_ENGAGEMENTS_LAST_MEETING_BOOKED_SOURCE,
        null as PROPERTY_HUBSPOT_OWNER_ID,
        null as PROPERTY_OWNEREMAIL,
        null as PROPERTY_OWNERNAME,
        null as PROPERTY_CITY,
        null as PROPERTY_HUBSPOT_TEAM_ID,
        null as PROPERTY_LINKEDINBIO,
        null as PROPERTY_TWITTERBIO,
        null as PROPERTY_HS_ALL_OWNER_IDS,
        null as PROPERTY_STATE,
        null as PROPERTY_HS_ALL_TEAM_IDS,
        null as PROPERTY_HS_ANALYTICS_SOURCE,
        null as PROPERTY_ZIP,
        null as PROPERTY_COUNTRY,
        null as PROPERTY_HS_ALL_ACCESSIBLE_TEAM_IDS,
        null as PROPERTY_HS_ANALYTICS_SOURCE_DATA_1,
        null as PROPERTY_HS_ANALYTICS_SOURCE_DATA_2,
        null as PROPERTY_HS_LANGUAGE,
        null as PROPERTY_HS_ANALYTICS_FIRST_REFERRER,
        null as PROPERTY_JOBTITLE,
        null as PROPERTY_PHOTO,
        null as PROPERTY_HS_ANALYTICS_LAST_REFERRER,
        null as PROPERTY_MESSAGE,
        null as PROPERTY_LIFECYCLESTAGE,
        null as PROPERTY_COMPANY,
        null as PROPERTY_WEBSITE,
        null as PROPERTY_NUMEMPLOYEES,
        null as PROPERTY_ANNUALREVENUE,
        null as PROPERTY_INDUSTRY,
        null as PROPERTY_HS_PREDICTIVECONTACTSCOREBUCKET,
        null as PROPERTY_HS_ANALYTICS_REVENUE,
        null as PROPERTY_HS_IS_UNWORKED,
        null as PROPERTY_HS_ANALYTICS_AVERAGE_PAGE_VIEWS,
        null as PROPERTY_ASSOCIATEDCOMPANYID,
        null as PROPERTY_HS_ANALYTICS_NUM_EVENT_COMPLETIONS,
        null as PROPERTY_HS_ANALYTICS_FIRST_TIMESTAMP,
        null as PROPERTY_HS_SOCIAL_FACEBOOK_CLICKS,
        null as PROPERTY_HS_SOCIAL_GOOGLE_PLUS_CLICKS,
        null as PROPERTY_HS_SOCIAL_LINKEDIN_CLICKS,
        null as PROPERTY_HS_ANALYTICS_NUM_PAGE_VIEWS,
        null as PROPERTY_LASTMODIFIEDDATE,
        null as PROPERTY_HS_SOCIAL_NUM_BROADCAST_CLICKS,
        null as PROPERTY_HS_ANALYTICS_NUM_VISITS,
        null as PROPERTY_HS_LIFECYCLESTAGE_LEAD_DATE,
        null as PROPERTY_CREATEDATE,
        null as PROPERTY_HS_SOCIAL_TWITTER_CLICKS,
        null as PROPERTY_HS_COUNT_IS_WORKED,
        null as PROPERTY_HS_COUNT_IS_UNWORKED,
        null as PROPERTY_HUBSPOT_OWNER_ASSIGNEDDATE,
        null as PROPERTY_HS_LIFECYCLESTAGE_SUBSCRIBER_DATE,        
        null as Source_type,
        null as DW_SESSION_NM,
        null as DW_INS_UPD_DTS,
         null AS CUSTOMER_TEXT_1,
        NULL AS CUSTOMER_TEXT_2,
        NULL AS CUSTOMER_TEXT_3,
        NULL AS CUSTOMER_TEXT_4,
        NULL AS CUSTOMER_TEXT_5,
        NULL AS CUSTOMER_TEXT_6,
        NULL AS CUSTOMER_NUMBER_1,
        NULL AS CUSTOMER_NUMBER_2,
        NULL AS CUSTOMER_NUMBER_3,
        NULL AS CUSTOMER_DATE_1,
        NULL AS CUSTOMER_DATE_2,
        NULL AS CUSTOMER_DATE_3    
    FROM dual    
    {% endif %}
{% endfor %}