/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.Concessions --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.Concessions --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.Concessions --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='Concessions') }}

SELECT columnIsolation.leaseId AS "leaseId"
    , COALESCE(columnIsolation.amount, '0') AS "amount"
    , COALESCE(columnIsolation.amountVariableAdjustment, '0') AS "amountVariableAdjustment"
    , COALESCE(columnIsolation.recurrenceCount, '0') AS "recurrenceCount"
FROM (
    SELECT l.id AS leaseId
        , REPLACE(fl.value: amount, '"', '') AS amount
        , REPLACE(fl.value: amountVariableAdjustment, '"', '') AS amountVariableAdjustment
        , l.baselineData: quote: recurrenceCount AS recurrenceCount
    FROM {{ var("source_tenant") }}.LEASE l
        , LATERAL flatten(input => parse_json(l.BASELINEDATA: publishedLease: concessions)) fl
    ) AS columnIsolation
