
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Price
    where (PRICE_ID) in (
        select (PRICE_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Price__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Price ("PRICE_ID", "SOURCE_ID", "ACTIVE", "CURRENCY", "NICKNAME", "RECURRING_AGGREGATE_USAGE", "RECURRING_INTERVAL", "RECURRING_INTERVAL_COUNT", "RECURRING_USAGE_TYPE", "TYPE", "UNIT_AMOUNT", "BILLING_SCHEME", "CREATED", "LIVEMODE", "LOOKUP_KEY", "TIERS_MODE", "TRANSFORM_QUANTITY_DIVIDE_BY", "TRANSFORM_QUANTITY_ROUND", "UNIT_AMOUNT_DECIMAL", "IS_DELETED", "PRODUCT_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "PRICE_ID", "SOURCE_ID", "ACTIVE", "CURRENCY", "NICKNAME", "RECURRING_AGGREGATE_USAGE", "RECURRING_INTERVAL", "RECURRING_INTERVAL_COUNT", "RECURRING_USAGE_TYPE", "TYPE", "UNIT_AMOUNT", "BILLING_SCHEME", "CREATED", "LIVEMODE", "LOOKUP_KEY", "TIERS_MODE", "TRANSFORM_QUANTITY_DIVIDE_BY", "TRANSFORM_QUANTITY_ROUND", "UNIT_AMOUNT_DECIMAL", "IS_DELETED", "PRODUCT_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Price__dbt_tmp
    );
