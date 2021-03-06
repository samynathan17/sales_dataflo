
    delete from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Payment_Intent
    where (PAYMENT_INTENT_ID) in (
        select (PAYMENT_INTENT_ID)
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Payment_Intent__dbt_tmp
    );
    

    insert into DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Payment_Intent ("PAYMENT_INTENT_ID", "SOURCE_PAYMENT_ID", "AMOUNT", "AMOUNT_CAPTURABLE", "AMOUNT_RECEIVED", "APPLICATION", "APPLICATION_FEE_AMOUNT", "CANCELED_AT", "CANCELLATION_REASON", "CAPTURE_METHOD", "CONFIRMATION_METHOD", "CREATED", "CURRENCY", "DESCRIPTION", "LAST_PAYMENT_ERROR_TYPE", "LAST_PAYMENT_ERROR_CODE", "LAST_PAYMENT_ERROR_DECLINE_CODE", "LAST_PAYMENT_ERROR_DOC_URL", "LAST_PAYMENT_ERROR_MESSAGE", "LAST_PAYMENT_ERROR_PARAM", "LAST_PAYMENT_ERROR_SOURCE_ID", "LAST_PAYMENT_ERROR_CHARGE_ID", "LIVEMODE", "ON_BEHALF_OF", "RECEIPT_EMAIL", "STATEMENT_DESCRIPTOR", "STATUS", "TRANSFER_DATA_DESTINATION", "TRANSFER_GROUP", "PAYMENT_METHOD_ID", "CUSTOMER_ID", "METADATA", "SOURCE_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS")
    (
        select "PAYMENT_INTENT_ID", "SOURCE_PAYMENT_ID", "AMOUNT", "AMOUNT_CAPTURABLE", "AMOUNT_RECEIVED", "APPLICATION", "APPLICATION_FEE_AMOUNT", "CANCELED_AT", "CANCELLATION_REASON", "CAPTURE_METHOD", "CONFIRMATION_METHOD", "CREATED", "CURRENCY", "DESCRIPTION", "LAST_PAYMENT_ERROR_TYPE", "LAST_PAYMENT_ERROR_CODE", "LAST_PAYMENT_ERROR_DECLINE_CODE", "LAST_PAYMENT_ERROR_DOC_URL", "LAST_PAYMENT_ERROR_MESSAGE", "LAST_PAYMENT_ERROR_PARAM", "LAST_PAYMENT_ERROR_SOURCE_ID", "LAST_PAYMENT_ERROR_CHARGE_ID", "LIVEMODE", "ON_BEHALF_OF", "RECEIPT_EMAIL", "STATEMENT_DESCRIPTOR", "STATUS", "TRANSFER_DATA_DESTINATION", "TRANSFER_GROUP", "PAYMENT_METHOD_ID", "CUSTOMER_ID", "METADATA", "SOURCE_ID", "_FIVETRAN_SYNCED", "ENTITY_TYPE", "DW_SESSION_NM", "DW_INS_UPD_DTS"
        from DATAFLOTEST_DATABASE.dbt_salesdataflo.Stg_Payment_Intent__dbt_tmp
    );
