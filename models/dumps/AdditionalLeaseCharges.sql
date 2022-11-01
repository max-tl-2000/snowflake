/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.AdditionalLeaseCharges --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.AdditionalLeaseCharges --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.AdditionalLeaseCharges --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='AdditionalLeaseCharges') }}

SELECT columnIsolation.leaseId AS "leaseId"
    , COALESCE(columnIsolation.name, '[name missing]') AS "name"
    , COALESCE(columnIsolation.amount, '0') AS "amount"
    , COALESCE(columnIsolation.feeType, '[feeType missing]') AS "feeType"
    , COALESCE(columnIsolation.isFirstFee, '[isFirstFee missing]') AS "isFirstFee"
    , COALESCE(columnIsolation.displayName, '[displayName missing]') AS "displayName"
    , COALESCE(columnIsolation.quoteSectionName, '[quoteSectionName missing]') AS "quoteSectionName"
FROM (
    SELECT l.id AS leaseId
        , REPLACE(fl.value: name, '"', '') AS NAME
        , REPLACE(fl.value: amount, '"', '') AS amount
        , REPLACE(fl.value: feeType, '"', '') AS feeType
        , REPLACE(fl.value: firstFee, '"', '') AS isFirstFee
        , REPLACE(fl.value: displayName, '"', '') AS displayName
        , REPLACE(fl.value: quoteSectionName, '"', '') AS quoteSectionName
    FROM {{ var("source_tenant") }}.LEASE l
        , LATERAL flatten(input => parse_json(l.BASELINEDATA: publishedLease: additionalCharges)) fl
    ) AS columnIsolation
ORDER BY columnIsolation.leaseId
