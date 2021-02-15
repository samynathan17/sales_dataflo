

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Fact_Marketing  as
      (
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('139' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS_PER_USER)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('140' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('141' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PERCENT_NEW_SESSIONS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('142' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(NEW_USERS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('143' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('144' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(PAGEVIEWS_PER_SESSION)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        union all
        
        (
             
With Source AS
   ( SELECT * FROM  DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Session  Where 1 = 1 ) 
 
,Metrics_Calc AS(
    SELECT
        Date AS Report_Dt,
        --hourly_slot,
        Source_type  AS entity_code,
        cast('145' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(BOUNCE_RATE)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('147' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(AVG_SESSION_DURATION)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('151' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_COMPLETIONS_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('152' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(GOAL_CONVERSION_RATE_ALL)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('158' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS_PER_SESSION)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('163' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TOTAL_EVENTS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('164' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        Sum(TRANSACTIONS)/decode(Sum(NEW_USERS),0,1)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('166' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(TIME_ON_PAGE)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
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
        cast('167' as number) as METRIC_ID,
        cast('10' as number)  AS METRIC_CATEGORY_ID,
        sum(UNIQUE_PAGEVIEWS)  as Value
     from Source 
         group by
        Report_Dt,
        entity_code,
        METRIC_ID,
        METRIC_CATEGORY_ID
        )
 SELECT Report_Dt, entity_code, METRIC_ID,METRIC_CATEGORY_ID, Value,'D_SALES_FACT_LOAD' AS DW_SESSION_NM,
        
    current_timestamp::
    timestamp_ntz

 AS DW_INS_UPD_DTS from Metrics_Calc
      order by Report_Dt, METRIC_ID
        )

        
      );
    