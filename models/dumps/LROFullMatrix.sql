/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.LROFullMatrix --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.LROFullMatrix --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.LROFullMatrix --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='LROFullMatrix') }}

SELECT listByDate.inventoryId AS "inventoryId"
    , listByDate.term AS "leaseTerm"
    , listByDate.rent AS "rent"
    , date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}',listByDate.startDate::TIMESTAMP))::TIMESTAMP_NTZ AS "startDate"
    , date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}',listByDate.endDate::TIMESTAMP))::TIMESTAMP_NTZ AS "endDate"
    , listByDate.amenityValue AS "LROAmenityValue"
    , listByDate.STATUS AS "LROUnitStatus"
    , listByDate.isRenewal AS "isRenewal"
    , date_trunc('day', COALESCE(CONVERT_TIMEZONE(prop.timezone, listByDate.renewalDate)::TIMESTAMP, '1900-01-01'))::TIMESTAMP AS "renewalDate"
    , substring(listByDate.fileName, 18, 8) AS "fileDate"
    , CONVERT_TIMEZONE('{{ var("timezone") }}', listByDate.created_at)::TIMESTAMP AS "fileImportedDate"
FROM (
    SELECT listByTerm.inventoryId
        , listByTerm.term
        , listByTerm.dates
        , REPLACE(listByTerm.dates [key] :rent::VARCHAR, '"', '') AS rent
        , REPLACE(listByTerm.dates [key] :endDate::VARCHAR, '"', '') AS endDate
        , fl.KEY AS startDate
        , listByTerm.amenityValue
        , listByTerm.STATUS
        , listByTerm.isRenewal
        , listByTerm.renewalDate
        , listByTerm.fileName
        , listByTerm.created_at
    FROM (
        SELECT rms.inventoryId
            , rms.rentMatrix
            , fl.KEY::INT AS term
            , fl.value AS dates
            , rms.amenityValue
            , rms.STATUS
            , CASE
                WHEN rms.renewalDate IS NULL
                    THEN 0
                ELSE 1
                END AS isRenewal
            , rms.renewalDate
            , rms.fileName
            , rms.created_at
        FROM {{ var("source_tenant") }}.RmsPricing AS rms
            , LATERAL flatten(input => rms.RENTMATRIX) AS fl
        ) AS listByTerm
        , LATERAL flatten(input => listByTerm.dates) AS fl
    ) AS listByDate
INNER JOIN {{ var("source_tenant") }}.Inventory AS i ON i.id = listByDate.inventoryId
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = i.propertyId
