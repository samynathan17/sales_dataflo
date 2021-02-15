
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Platform_Device  Where 1 = 1 )   
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('93' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(DEVICE_CATEGORY as varchar(1000)) As Segment_name,
        Count(*)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('94' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('95' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(SOCIAL_NETWORK as varchar(1000)) As Segment_name,
        Count(*)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('99' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='ORGANIC' )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('101' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('102' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('108' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Adwords_Keyword  Where upper(KEYWORD)='PAID' )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('109' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(KEYWORD as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('110' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Goal_Conversions  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('112' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(GOAL_COMPLETION_LOCATION as varchar(1000)) As Segment_name,
        Sum(GOAL_VALUE_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Page_Tracking  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('114' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(LANDING_PAGE_PATH as varchar(1000)) As Segment_name,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('115' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(PAGE_TITLE as varchar(1000)) As Segment_name,
        Count(*)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Geo_Network  Where 1 = 1 ) 
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('120' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(NETWORK_LOCATION as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('125' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(CHANNEL_GROUPING as varchar(1000)) As Segment_name,
        sum(USERS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Social_Media_Acquisitions  Where 1 = 1 )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('130' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(NEW_USERS as varchar(1000)) As Segment_name,
        sum(PAGEVIEWS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Events_Overview  Where 1 = 1 )           
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('131' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(EVENT_CATEGORY as varchar(1000)) As Segment_name,
        sum(SESSIONS_WITH_EVENT)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Channel_Traffic  Where upper(CHANNEL_GROUPING)='ORGANIC' )
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        cast(USERS as varchar(1000)) As Segment_name,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID,
        Segment_name
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Segment_name,Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        