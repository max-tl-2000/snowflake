/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ActiveLeaseWorkflowData --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ActiveLeaseWorkflowData --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ActiveLeaseWorkflowData --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ActiveLeaseWorkflowData') }}

-- depends on: {{ ref('ActiveLeasePartyDump') }}

SELECT *
FROM (
    SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || alwd.partyId::TEXT AS "partyId"
        , alwd.isImported AS "isImported"
        , REPLACE(alwd.leaseData: unitName, '"', '') AS "unitName"
        , REPLACE(alwd.leaseData: inventoryId, '"', '') AS "inventoryId"
        , CONVERT_TIMEZONE('{{ var("timezone") }}', (REPLACE(alwd.leaseData: leaseStartDate, '"', ''))::TIMESTAMP)::TIMESTAMP_NTZ AS "leaseStartDate"
        , CASE
            WHEN REPLACE(alwd.leaseData: leaseEndDate, '"', '') = 'Invalid date' THEN DATEADD(MONTH, alwd.leaseData: leaseTerm, CONVERT_TIMEZONE('{{ var("timezone") }}', (REPLACE(alwd.leaseData: leaseStartDate, '"', ''))::TIMESTAMP))::TIMESTAMP_NTZ
            ELSE CONVERT_TIMEZONE('{{ var("timezone") }}', (REPLACE(alwd.leaseData: leaseEndDate, '"', ''))::TIMESTAMP)::TIMESTAMP_NTZ
          END AS "leaseEndDate"
        , alwd.STATE AS "state"
        , CONVERT_TIMEZONE('{{ var("timezone") }}', (REPLACE(alwd.metadata: vacateDate, '"', ''))::TIMESTAMP)::TIMESTAMP_NTZ AS "vacateDate"
        , CONVERT_TIMEZONE('{{ var("timezone") }}', (REPLACE(alwd.metadata: dateOfTheNotice, '"', ''))::TIMESTAMP)::TIMESTAMP_NTZ AS "dateOfTheNotice"
        , rank() OVER (
            PARTITION BY alwd.leaseData: inventoryId ORDER BY alwd.created_at DESC
            ) AS "theRank"
        , alpd."Property"
    FROM {{ var("source_tenant") }}.ActiveLeaseWorkflowData AS alwd
    LEFT JOIN {{ var("target_schema") }}."ActiveLeasePartyDump" AS alpd ON alwd.PARTYID = alpd."id"
    ) AS mostRecent
WHERE mostRecent."theRank" = 1
