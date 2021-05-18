

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.ad_reporting  as
      (

with unioned as (

    

        (
            select

                cast('DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin_ads' as 
    varchar
) as _dbt_source_relation,
                
                    cast("PLATFORM" as character varying(12)) as "PLATFORM" ,
                    cast("DATE_DAY" as DATE) as "DATE_DAY" ,
                    cast("ACCOUNT_NAME" as character varying(256)) as "ACCOUNT_NAME" ,
                    cast("ACCOUNT_ID" as NUMBER(38,0)) as "ACCOUNT_ID" ,
                    cast("CAMPAIGN_NAME" as character varying(256)) as "CAMPAIGN_NAME" ,
                    cast("CAMPAIGN_ID" as character varying(16777216)) as "CAMPAIGN_ID" ,
                    cast("AD_GROUP_NAME" as character varying(256)) as "AD_GROUP_NAME" ,
                    cast("AD_GROUP_ID" as character varying(16777216)) as "AD_GROUP_ID" ,
                    cast("BASE_URL" as character varying(256)) as "BASE_URL" ,
                    cast("URL_HOST" as character varying(16777216)) as "URL_HOST" ,
                    cast("URL_PATH" as character varying(16777216)) as "URL_PATH" ,
                    cast("UTM_SOURCE" as character varying(16777216)) as "UTM_SOURCE" ,
                    cast("UTM_MEDIUM" as character varying(16777216)) as "UTM_MEDIUM" ,
                    cast("UTM_CAMPAIGN" as character varying(16777216)) as "UTM_CAMPAIGN" ,
                    cast("UTM_CONTENT" as character varying(16777216)) as "UTM_CONTENT" ,
                    cast("UTM_TERM" as character varying(16777216)) as "UTM_TERM" ,
                    cast("CLICKS" as NUMBER(38,0)) as "CLICKS" ,
                    cast("IMPRESSIONS" as NUMBER(38,0)) as "IMPRESSIONS" ,
                    cast("SPEND" as NUMBER(18,5)) as "SPEND" 

            from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_linkedin_ads
        )

        union all
        

        (
            select

                cast('DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads' as 
    varchar
) as _dbt_source_relation,
                
                    cast("PLATFORM" as character varying(12)) as "PLATFORM" ,
                    cast("DATE_DAY" as DATE) as "DATE_DAY" ,
                    cast(null as character varying(256)) as "ACCOUNT_NAME" ,
                    cast(null as NUMBER(38,0)) as "ACCOUNT_ID" ,
                    cast("CAMPAIGN_NAME" as character varying(256)) as "CAMPAIGN_NAME" ,
                    cast("CAMPAIGN_ID" as character varying(16777216)) as "CAMPAIGN_ID" ,
                    cast("AD_GROUP_NAME" as character varying(256)) as "AD_GROUP_NAME" ,
                    cast("AD_GROUP_ID" as character varying(16777216)) as "AD_GROUP_ID" ,
                    cast("BASE_URL" as character varying(256)) as "BASE_URL" ,
                    cast("URL_HOST" as character varying(16777216)) as "URL_HOST" ,
                    cast("URL_PATH" as character varying(16777216)) as "URL_PATH" ,
                    cast("UTM_SOURCE" as character varying(16777216)) as "UTM_SOURCE" ,
                    cast("UTM_MEDIUM" as character varying(16777216)) as "UTM_MEDIUM" ,
                    cast("UTM_CAMPAIGN" as character varying(16777216)) as "UTM_CAMPAIGN" ,
                    cast("UTM_CONTENT" as character varying(16777216)) as "UTM_CONTENT" ,
                    cast("UTM_TERM" as character varying(16777216)) as "UTM_TERM" ,
                    cast("CLICKS" as NUMBER(38,0)) as "CLICKS" ,
                    cast("IMPRESSIONS" as NUMBER(38,0)) as "IMPRESSIONS" ,
                    cast("SPEND" as NUMBER(18,5)) as "SPEND" 

            from DATAFLOTEST_DATABASE.dbt_salesdataflo.stg_facebook_ads
        )

        

)

select *
from unioned
      );
    