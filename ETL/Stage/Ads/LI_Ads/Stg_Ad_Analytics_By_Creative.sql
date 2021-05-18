{% set results = get_column_values_from_query("select * from " ~ var('V_DB') ~ "." ~ var('V_Entity_Schema')~ "." ~ var('V_Mkt')~" where DATASOURCE_TYPE = 'LI_ADS' and READY_TO_PROCESS = 'TRUE'", "ENTITY_DATASORUCE_NAME||'#'||DATASOURCE_TYPE")%}

{% if not var("enable_SF_source") %}
{{
    config(
        enabled=false
    )
}}
{% endif %}

{% for V_SF_Schema in results %}


{% if V_SF_Schema != 'X' %} 
{% set schema_nm,entity_typ = V_SF_Schema.split('#') %}
{% else %}
{% set entity_typ = 'X' %}
{% endif %}

 {% if  entity_typ == 'LI_ADS'  %} 
      
  select
        {{ dbt_utils.surrogate_key(['day','creative_id']) }} as daily_creative_id,
SHARES,
COMPANY_PAGE_CLICKS,
VIRAL_SHARES,
VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS,
OTHER_ENGAGEMENTS,
VIRAL_CARD_CLICKS,
VIDEO_VIEWS,
VIRAL_VIDEO_MIDPOINT_COMPLETIONS,
VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
VIRAL_ONE_CLICK_LEADS,
CARD_CLICKS,
VIDEO_MIDPOINT_COMPLETIONS,
VIDEO_COMPLETIONS,
VIRAL_IMPRESSIONS,
VIRAL_COMMENT_LIKES,
VIRAL_LANDING_PAGE_CLICKS,
FULL_SCREEN_PLAYS,
VIDEO_STARTS,
VIRAL_COMMENTS,
LIKES,
VIRAL_OTHER_ENGAGEMENTS,
DAY,
VIRAL_FULL_SCREEN_PLAYS,
APPROXIMATE_UNIQUE_IMPRESSIONS,
COMMENT_LIKES,
EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
COST_IN_LOCAL_CURRENCY,
ONE_CLICK_LEADS,
VIRAL_EXTERNAL_WEBSITE_CONVERSIONS,
LEAD_GENERATION_MAIL_INTERESTED_CLICKS,
OPENS,
VIDEO_FIRST_QUARTILE_COMPLETIONS,
VIRAL_CLICKS,
EXTERNAL_WEBSITE_CONVERSIONS,
VIRAL_CARD_IMPRESSIONS,
ONE_CLICK_LEAD_FORM_OPENS,
VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
CREATIVE_ID,
EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS,
VIRAL_VIDEO_VIEWS,
LANDING_PAGE_CLICKS,
ACTION_CLICKS,
CONVERSION_VALUE_IN_LOCAL_CURRENCY,
COST_IN_USD,
VIRAL_ONE_CLICK_LEAD_FORM_OPENS,
VIRAL_TOTAL_ENGAGEMENTS,
VIRAL_VIDEO_STARTS,
CLICKS,
LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES,
TEXT_URL_CLICKS,
VIRAL_LIKES,
IMPRESSIONS,
AD_UNIT_CLICKS,
CARD_IMPRESSIONS,
VIRAL_FOLLOWS,
FOLLOWS,
TOTAL_ENGAGEMENTS,
VIDEO_THIRD_QUARTILE_COMPLETIONS,
COMMENTS,
VIRAL_COMPANY_PAGE_CLICKS,
VIRAL_VIDEO_COMPLETIONS,

        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM {{ schema_nm }}.AD_ANALYTICS_BY_CREATIVE
          {% if not loop.last %}
            UNION ALL
        {% endif %} 
        {% elif  entity_typ == 'X'  %} 
         select
        NULL AS SHARES,
NULL AS COMPANY_PAGE_CLICKS,
NULL AS VIRAL_SHARES,
NULL AS VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS,
NULL AS OTHER_ENGAGEMENTS,
NULL AS VIRAL_CARD_CLICKS,
NULL AS VIDEO_VIEWS,
NULL AS VIRAL_VIDEO_MIDPOINT_COMPLETIONS,
NULL AS VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
NULL AS VIRAL_ONE_CLICK_LEADS,
NULL AS CARD_CLICKS,
NULL AS VIDEO_MIDPOINT_COMPLETIONS,
NULL AS VIDEO_COMPLETIONS,
NULL AS VIRAL_IMPRESSIONS,
NULL AS VIRAL_COMMENT_LIKES,
NULL AS VIRAL_LANDING_PAGE_CLICKS,
NULL AS FULL_SCREEN_PLAYS,
NULL AS VIDEO_STARTS,
NULL AS VIRAL_COMMENTS,
NULL AS LIKES,
NULL AS VIRAL_OTHER_ENGAGEMENTS,
NULL AS DAY,
NULL AS VIRAL_FULL_SCREEN_PLAYS,
NULL AS APPROXIMATE_UNIQUE_IMPRESSIONS,
NULL AS COMMENT_LIKES,
NULL AS EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS,
NULL AS COST_IN_LOCAL_CURRENCY,
NULL AS ONE_CLICK_LEADS,
NULL AS VIRAL_EXTERNAL_WEBSITE_CONVERSIONS,
NULL AS LEAD_GENERATION_MAIL_INTERESTED_CLICKS,
NULL AS OPENS,
NULL AS VIDEO_FIRST_QUARTILE_COMPLETIONS,
NULL AS VIRAL_CLICKS,
NULL AS EXTERNAL_WEBSITE_CONVERSIONS,
NULL AS VIRAL_CARD_IMPRESSIONS,
NULL AS ONE_CLICK_LEAD_FORM_OPENS,
NULL AS VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
NULL AS CREATIVE_ID,
NULL AS EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS,
NULL AS VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS,
NULL AS VIRAL_VIDEO_VIEWS,
NULL AS LANDING_PAGE_CLICKS,
NULL AS ACTION_CLICKS,
NULL AS CONVERSION_VALUE_IN_LOCAL_CURRENCY,
NULL AS COST_IN_USD,
NULL AS VIRAL_ONE_CLICK_LEAD_FORM_OPENS,
NULL AS VIRAL_TOTAL_ENGAGEMENTS,
NULL AS VIRAL_VIDEO_STARTS,
NULL AS CLICKS,
NULL AS LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES,
NULL AS TEXT_URL_CLICKS,
NULL AS VIRAL_LIKES,
NULL AS IMPRESSIONS,
NULL AS AD_UNIT_CLICKS,
NULL AS CARD_IMPRESSIONS,
NULL AS VIRAL_FOLLOWS,
NULL AS FOLLOWS,
NULL AS TOTAL_ENGAGEMENTS,
NULL AS VIDEO_THIRD_QUARTILE_COMPLETIONS,
NULL AS COMMENTS,
NULL AS VIRAL_COMPANY_PAGE_CLICKS,
NULL AS VIRAL_VIDEO_COMPLETIONS,















        '{{ schema_nm }}' as Source_type,
        'D_BASIC_AD_ACTIONS_STG_LOAD' AS DW_SESSION_NM,
        {{ dbt_utils.current_timestamp() }} AS DW_INS_UPD_DTS 
    FROM dual     

    {% endif %}
{% endfor %}