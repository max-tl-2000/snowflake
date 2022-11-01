/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** GoogleSheets
dbt run --select customer_specific.maximus.imports.MarketingSpend --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='MarketingSpend') }}

SELECT PROPERTY AS "Property"
    , SOURCE AS "Source"
    , VALID_SOURCE AS "Valid Source"
    , TO_DATE(REPORTING_MONTH,'MM/DD/YY')::TIMESTAMP_NTZ AS "Reporting Month"
    , BUDGET::DECIMAL(10,2) AS "Budget"
    , ACTUAL::DECIMAL(10,2) AS "Actual"
    , BUDGET_EVENT AS "Budget Event"
    , NOTES AS "Notes"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538"."MARKETINGSPEND"
WHERE PROPERTY IS NOT NULL
