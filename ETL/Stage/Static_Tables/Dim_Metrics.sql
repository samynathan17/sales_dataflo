{{ config(
    materialized="table"
) 
}}

select * from(
Select 1 METRIC_ID,'Opportunities Won Revenue' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage ""is equal to Closed Won"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Opportunities Won Revenue during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 2 METRIC_ID,'Key Metrics' METRIC_NAME,5 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'' METRICCRITERIA,'This is a multi-metric Datablock with the following metrics: Avg Case Close Time, Converted Leads, Expected revenue, New Cases, New Leads, Opportunities Lost Count, Open Opportunities Count, Opportunities Won Count, Opportunities Won Revenue.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 3 METRIC_ID,'Converted Leads' METRIC_NAME,4 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Status ""is equal to Converted"" and Lead owner ""is equal to logged in employee""' METRICCRITERIA,'Number of Converted Leads during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 4 METRIC_ID,'New Leads' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Name ""is not empty"" and Lead owner ""is equal to logged in employee""' METRICCRITERIA,'Number of New Leads during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 5 METRIC_ID,'Open Opportunity by stage' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Sales stage ""is not equal to Closed Won"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Total Open Opportunities Count during the specified Date Range split up by Owner. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 6 METRIC_ID,'Top Sales Rep' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Sum ' RESULT_TYPE,'' METRICCRITERIA,'This is a multi-metric Datablock with the following metrics: Opportunities Won Amount by Owner, Opportunities Won Count by Owner.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 7 METRIC_ID,'New Leads by industry' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'' METRICCRITERIA,'Number of New Leads during the specified Date Range split up by Industry.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 8 METRIC_ID,'Return On Investment Amount by Campaign' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'' METRICCRITERIA,'Total Return On Investment Amount during the specified Date Range split up by Campaign Type. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 9 METRIC_ID,'Return On Investment by Campaign Type' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'' METRICCRITERIA,'Total Return On Investment during the specified Date Range split up by Campaign Type. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 10 METRIC_ID,'Opportunities Lost' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Sales stage ""is equal to Closed Lost"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Opportunities Lost Count during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 11 METRIC_ID,'Opportunities Lost Amount by Owner' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage ""is equal to Closed Lost""' METRICCRITERIA,'Opportunities Lost Amount during the specified Date Range split up by Owner.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 12 METRIC_ID,'Opportunities Lost by Owner' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage ""is equal to Closed Lost""' METRICCRITERIA,'Opportunities Lost Count during the specified Date Range split up by Owner.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 13 METRIC_ID,'Average Case Close Time' METRIC_NAME,6 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'' METRICCRITERIA,'Average Case Close Time during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 14 METRIC_ID,'Opportunities Lost Amount by Opp Name' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage ""is equal to Closed Lost"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Opportunities Lost Amount during the specified Date Range split up by Opportunity Name.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 15 METRIC_ID,'Closed Cases' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Status ""is equal to Closed"" and case owner ""is equal to logged in employee""' METRICCRITERIA,'Number of Closed Cases during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 16 METRIC_ID,'Opportunities Won Revenue by Product Family' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'' METRICCRITERIA,'Opportunities Won Revenue during the specified Date Range split up by Product Family.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 17 METRIC_ID,'Expected Revenue' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Closed Date "is not empty" and Sales stage "is not equal to Closed Won, closed Lost"' METRICCRITERIA,'Expected Revenue during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 18 METRIC_ID,'New Leads by Lead Source' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Lead Source ""is not empty""' METRICCRITERIA,'Number of New Leads during the specified Date Range split up by Lead Source.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 19 METRIC_ID,'New Leads by Lead Status' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Lead Status ""is not empty""' METRICCRITERIA,'Number of New Leads during the specified Date Range split up by Lead Status.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 20 METRIC_ID,'New Cases' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Case # ""is not empty""' METRICCRITERIA,'Number of New Cases during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 21 METRIC_ID,'Opportunities Won' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Sales stage ""is equal to Closed Won"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Opportunities Won Count during the specified Date Range.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 22 METRIC_ID,'Opportunities Won Amount by Opp Name' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage ""is equal to Closed Won"" and Opportunity owner ""is equal to logged in employee""' METRICCRITERIA,'Opportunities Won Amount during the specified Date Range split up by Opportunity Name.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 23 METRIC_ID,'Open Opportunities' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Sales stage "is not equal to Closed Won, closed Lost" and Opportunity owner "is equal to logged in employee"' METRICCRITERIA,'Total Open Opportunities Count during the specified Date Range. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 24 METRIC_ID,'Opportunities Overview' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'' METRICCRITERIA,'This is a multi-metric Datablock with the following metrics: Opportunities Lost Count, Open Opportunities Count, Opportunities Won Count.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 25 METRIC_ID,'Open Opportunities Amount by Stage Name' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'Sales stage "is not equal to Closed Won, closed Lost" and Opportunity owner "is equal to logged in employee"' METRICCRITERIA,'Total Open Opportunities Amount during the specified Date Range split up by Owner. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 26 METRIC_ID,'Open Opportunities Amount by Opp Name' METRIC_NAME,1 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Sum' RESULT_TYPE,'' METRICCRITERIA,'Total Open Opportunities Amount during the specified Date Range split up by Opportunity Name. Daily totals are not available for this metric. Instead, the total current value of this metric will be visualized cumulatively. No historical data is available from the initial connection.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 27 METRIC_ID,'Accounts' METRIC_NAME,5 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Account Name "is not empty" and Account Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of accounts by annual revenue.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 28 METRIC_ID,'Accounts by Type' METRIC_NAME,5 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Account Type "is not empty" and Account Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of accounts by type.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 29 METRIC_ID,'Contacts' METRIC_NAME,6 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,' Name "is not empty" and Contact Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of all your contacts by department including name, phone, title, and more.' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 30 METRIC_ID,'Leads' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,' Name "is not empty" and Lead Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of leads (for this quarter).' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 31 METRIC_ID,'Leads by location' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,' Address "is not empty" and Lead Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of leads by location (for the last 7 days).' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 32 METRIC_ID,'Opportunities by type' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Type "is not empty" and Opportunity Owner is equal to "Logged in employee"' METRICCRITERIA,'Get the number of opportunities by type (for this year or for the last two quarters).' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 33 METRIC_ID,'Leads by status' METRIC_NAME,3 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'TRUE' SEGMENT_FLAG,'Count' RESULT_TYPE,'Lead status "is not empty" and Lead Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of new leads by status (for this month).' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
union
Select 34 METRIC_ID,'Opportunities (for this quarter)' METRIC_NAME,2 METRIC_CATEGORY_ID,'TRUE' ACTIVE_FLAG,'' SEGMENT_FLAG,'Count' RESULT_TYPE,'Opportunity Name "is not empty" and Opportunity Owner is equal to "Logged in employee"' METRICCRITERIA,'Get a list of opportunities (for this quarter).' DESCRIPTION,'D_METRICS_DIM_LOAD' DW_SESSION_NM,CURRENT_TIMESTAMP DW_INS_UPD_DTS
)