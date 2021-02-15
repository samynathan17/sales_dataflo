

      create or replace transient table DATAFLOTEST_DATABASE.dbt_salesdataflo.Dim_Mkt_Metrics  as
      (

select * from(
Select 1 METRIC_ID, 'Sessions by Device Type' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get the number of website sessions by device type: desktop, mobile, and tablet (for the last year).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 2 METRIC_ID, 'Traffic By Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get the number of sessions by channel grouping (for the last 2 years).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 3 METRIC_ID, 'Sessions by Social Network' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during specified Date Range split up by Social Networks.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 4 METRIC_ID, 'Top Pages by Pageviews' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Pageviews to each Page during the specified Date Range. This metric is collecting data for the top 500 Pages in the connected Google Analytics Account. To gather data for more than 500 Pages, please use the Query Builder tool.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 5 METRIC_ID, 'Avg. Quantity by Sources' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average number of Products Sold per Transaction during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 6 METRIC_ID, 'Product revenue by Product name' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Individual Product Sales during specified Date Range split up by Product Name.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 7 METRIC_ID, 'Sessions by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 8 METRIC_ID, 'Sessions by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 9 METRIC_ID, 'Sessions by Organic Keyword' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during specified Date Range split up by Organic Keywords.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 10 METRIC_ID, 'Goal Completion by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Conversions during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 11 METRIC_ID, 'Goal Completion by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Conversions during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 12 METRIC_ID, 'Quantity by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Units Sold in Ecommerce transactions during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 13 METRIC_ID, 'Quantity by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Units Sold in Ecommerce transactions during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 14 METRIC_ID, 'Revenue by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Web Ecommerce or In-app Transactions during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 15 METRIC_ID, 'Revenue by landing page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Web Ecommerce or In-app Transactions during the specified Date Range split up by Landing Page.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 16 METRIC_ID, 'Goal Completion by Goal' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Conversions during specified Date Range split up by Goals.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 17 METRIC_ID, 'Sessions by Paid Keyword' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified Date Range split up by Top Paid Keywords.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 18 METRIC_ID, 'Goal Value by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Value produced by Goal Conversions on your Site during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 19 METRIC_ID, 'Revenue by Organic Keyword' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Web Ecommerce or In-app Transactions during specified Date Range split up by Organic Keywords.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 20 METRIC_ID, 'Goal Value by Goal' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Value produced by Goal Conversions on your Site during specified Date Range split up by Goals.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 21 METRIC_ID, 'Screen Views by Screen Name' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Screens Viewed. Repeated Views of a Single Screen are counted during specified Date Range split up by Screen Name.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 22 METRIC_ID, 'Sessions by Landing Page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified Date Range split up by Landing Page.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 23 METRIC_ID, 'Sessions by Page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions to each Page during the specified Date Range. This metric is collecting data for the top 500 Pages in the connected Google Analytics Account. To gather data for more than 500 Pages, please use the Query Builder tool.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 24 METRIC_ID, 'Sessions by New vs Returning' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during specified Date Range split up by New vs Returning.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 25 METRIC_ID, 'Goal Completion by New vs Returning' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Conversions during specified Date Range split up by New vs Returning.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 26 METRIC_ID, 'Top Source/Medium by Sessions' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified Date Range split up by Top Sources/Mediums.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 27 METRIC_ID, 'Top Sources By Revenue' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Web Ecommerce or In-app Transactions during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 28 METRIC_ID, 'Sessions by Geo Location' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified date range split up by Geo Location.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 29 METRIC_ID, 'Sessions by Top Geo Location' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during the specified date range split up by Geo Location.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 30 METRIC_ID, 'Transactions by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Completed Purchases on your Site during specified Date Range split up by Channels.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 31 METRIC_ID, 'Goal Value by New vs Returning' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Value produced by Goal Conversions on your Site during specified Date Range split up by New vs Returning.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 32 METRIC_ID, 'Transactions by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Completed Purchases on your Site during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 33 METRIC_ID, 'Transactions per User by Channel' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Transactions split up by Channels during the specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 34 METRIC_ID, 'Transactions per User by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Transactions split up by original Source during the specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 35 METRIC_ID, 'Goal Value by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Value produced by Goal Conversions on your Site during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 36 METRIC_ID, 'Users by Landing Page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Users who have initiated at least one Session during the specified Date Range split up by Landing Page.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 37 METRIC_ID, 'Users by Organic Keyword' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Users who have initiated at least one Session during specified Date Range split up by Organic Keywords.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 38 METRIC_ID, 'Users by Page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Users who have initiated at least one Session to each Page during the specified Date Range. This metric is collecting data for the top 500 Pages in the connected Google Analytics Account. To gather data for more than 500 Pages, please use the Query Builder tool.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 39 METRIC_ID, 'Top Events by Sessions by Label' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Total number of Sessions during the specified date range split up by top Events by Label.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 40 METRIC_ID, 'Users by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Users who have initiated at least one Session during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 41 METRIC_ID, 'Ecommerce Conv Rate by Source' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, 'Y' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Percentage of Sessions that resulted in an Ecommerce Transaction during the specified Date Range split up by Sources.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 42 METRIC_ID, 'Audience Metrics' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get a list of total monthly users, sessions, and page views (for the last year).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 43 METRIC_ID, 'Impressions, Cost, Clicks' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get Google Ads data by distribution network and ad group including impressions, cost, and clicks (for the last 14 days).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 44 METRIC_ID, 'Organic Search Landing Page Performance (Last 30 Days)' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get a list of landing pages including average time on page, number of users, sessions, and more (for the last 30 days).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 45 METRIC_ID, 'Web Property Metrics (Last 30 Days)' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get daily web property metrics including users, sessions, and page views (for the last 30 days).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 46 METRIC_ID, 'Website Performance' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Get website performance details including users, bounce rate, page views per session, and more (for the last 2 years).' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 47 METRIC_ID, 'Users' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Users who have initiated at least one Session during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 48 METRIC_ID, 'Sessions' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during specified Date Range. A session is the period time a user is actively engaged with your website, app, etc. 
' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 49 METRIC_ID, '% New Sessions' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'The Procentage of the Sessions that are created by New Users (first-time visits) during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 50 METRIC_ID, 'New Users' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of First-time Users during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 51 METRIC_ID, 'Pageviews' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Pages Viewed during specified Date Range. Repeated Views of a Single Page are counted.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 52 METRIC_ID, 'Pages / Sessions' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average Number of Pages Viewed during a Session during specified Date Range. Repeated Views of a Single Page are counted.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 53 METRIC_ID, 'Bounce Rate' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Percentage of Single-Page Sessions in which there was no Interaction with the Page during specified Date Range. A bounced Session has a duration of 0 seconds.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 54 METRIC_ID, 'Screens / Session' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average number of Screens Viewed per Session during specified Date Range. Every View of a Single Screen is counted individually, including repeated Siews of the same Screen.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 55 METRIC_ID, 'Average Session Duration' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average Length of a Session during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 56 METRIC_ID, 'Audience Overview' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'This is a multi-metric Datablock with the following metrics: Avg. Session Duration, Bounce Rate, % New Sessions, Pages / Session, Pageviews, Sessions, Users.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 57 METRIC_ID, 'Audience Overview (mobile)' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'This is a multi-metric Datablock with the following metrics: Avg. Session Duration, Bounce Rate, % New Sessions, Pages / Session, Pageviews, Sessions, Users.This is a multi-metric Datablock with the following metrics: Avg. Session Duration, % New Sessions, Screens / Session, Screen View, Sessions, Users.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 58 METRIC_ID, 'Goal Overview' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'This is a multi-metric Datablock with the following metrics: Goal Completion, Goal Conversion Rate.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 59 METRIC_ID, 'Goal Completion' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Conversions during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 60 METRIC_ID, 'Goal Conversion Rate' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Sum of all individual Goal Conversion Rates during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 61 METRIC_ID, 'Behavior Overview' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'This is a multi-metric Datablock with the following metrics: Avg. Time on Page, Bounce Rate, Pageviews, Unique Pageviews.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 62 METRIC_ID, 'Revenue' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Revenue from Web Ecommerce or In-app Transactions during specified Date Range. Depending on your implementation, this can include Tax and Shipping.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 63 METRIC_ID, 'Transactions' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Completed Purchases on your Site during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 64 METRIC_ID, 'Ecommerce Conversion Rate' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Percentage of Sessions that resulted in an Ecommerce Transaction during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 65 METRIC_ID, 'Ecommerce overview' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'This is a multi-metric Datablock with the following metrics: Avg. Order Value, Ecommerce Conversion Rate, Revenue, Quantity, Revenue per Visit, Transactions, Unique Purchases.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 66 METRIC_ID, 'Avg. Order Value' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average Value of transactions during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 67 METRIC_ID, 'Avg. Quantity' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average number of Products Sold per Transaction during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 68 METRIC_ID, 'Avg. revenue per user' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average amount of Money generated split up by User during the specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 69 METRIC_ID, 'Unique Purchases' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Times a specified Pooduct (or set of Products) was a part of a Transaction during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 70 METRIC_ID, 'Quantity' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Units Sold in Ecommerce transactions during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 71 METRIC_ID, 'Events' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Events occurred during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 72 METRIC_ID, 'Transactions per user' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Transactions divided by total number of Users during the specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 73 METRIC_ID, 'Avg. Time on Page' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average Amount of Time Users Spent Viewing a specified Page/Screen, or Set of Pages/Screens during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 74 METRIC_ID, 'Avg. Time on Screen' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Average Amount of Time Users Spent Viewing a specified Screen, or Set of Screens during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 75 METRIC_ID, 'Unique Pageviews' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Sessions during which the specified Page was Viewed at least once. A unique pageview is counted for each page URL + page Title combination.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 76 METRIC_ID, 'Unique Screen Views' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Unique Screen Views is the number of Sessions during which the specified Screen was Viewed at least once during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 77 METRIC_ID, 'Screen Views' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Number of Screens Viewed. Repeated Views of a Single Screen are counted during specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 78 METRIC_ID, 'Revenue per Visit' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'The amount of Money generated each time a Customer visits your Website during the specified Date Range.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
Union
Select 79 METRIC_ID, 'Goal Value' METRIC_NAME, 10 METRIC_CATEGORY_ID, 'Y' ACTIVE_FLAG, '' SEGMENT_FLAG, 'count' RESULT_TYPE,'GA' ENTITY_TYPE,'Value produced by Goal Conversions on your Site during specified Date Range. This value is calculated by multiplying the number of Goal Conversionsby the value that you assigned to each Goal.' METRICCRITERIA,'' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS  
)
      );
    