/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.LROStandardLeaseMatrix --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.LROStandardLeaseMatrix --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.LROStandardLeaseMatrix --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='LROStandardLeaseMatrix') }}

-- depends on: {{ ref('InventoryDump') }}

SELECT rms.inventoryId AS "inventoryId"
    , rms.standardLeaseLength AS "standardLeaseLength"
    , rms.standardRent AS "standardRent"
    , rms.amenityValue AS "amenityValue"
    , rms.STATUS AS "externalInventoryStatus"
    , date_trunc('day', stdLeaseDateKeys.minStartDateKey::TIMESTAMP)::TIMESTAMP AS "standardRentStartDate"
    , stdLeaseDateKeys.endDate::TIMESTAMP AS "standardRentEndDate"
    , rms.minRentLeaseLength AS "minRentLeaseLength"
    , rms.minRent AS "minRent"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, rms.minRentStartDate))::TIMESTAMP AS "minRentStartDate"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, rms.minRentEndDate))::TIMESTAMP AS "minRentEndDate"
    , stdLeaseDateKeys.rentmatrix_standard AS "standardMatrix"
    , CASE
        WHEN rms.renewalDate IS NULL
            THEN 0
        ELSE 1
        END AS "isRenewal"
    , date_trunc('day', COALESCE(CONVERT_TIMEZONE(prop.timezone, rms.renewalDate), '1900-01-01'))::TIMESTAMP AS "renewalDate"
    , id."externalId"
FROM {{ var("source_tenant") }}.RmsPricing AS rms
INNER JOIN (
    SELECT rmsId
        , minStartDateKey
        , rentmatrix_standard
        , REPLACE(fl.value: endDate, '"', '') AS endDate
    FROM (
        SELECT dateKeys.rmsId
            , min(dateKeys.rentStartDateKey) AS minStartDateKey
            , any_value(dateKeys.rentm) AS rentmatrix_standard
        FROM (
            SELECT T.id AS rmsId
                , T.rentm
                , fl.KEY AS rentStartDateKey
            FROM (
                SELECT id
                    , RENTMATRIX
                    , fl.value AS rentm
                FROM {{ var("source_tenant") }}.RMSPRICING
                    , LATERAL flatten(input => parse_json(rentMatrix)) AS fl
                WHERE fl.KEY = STANDARDLEASELENGTH
                ) AS T
                , LATERAL flatten(input => rentm) AS fl
            ) AS dateKeys
        GROUP BY dateKeys.rmsId
        ) AS T
        , LATERAL flatten(input => T.rentmatrix_standard) AS fl
    WHERE T.minStartDateKey = fl.KEY
    ) AS stdLeaseDateKeys ON stdLeaseDateKeys.rmsId = rms.id
INNER JOIN {{ var("source_tenant") }}.Inventory AS i ON i.id = rms.inventoryId
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = i.propertyId
LEFT JOIN {{ var("target_schema") }}."InventoryDump" AS id ON id."inventoryId" = rms.inventoryId
ORDER BY rms.inventoryId
