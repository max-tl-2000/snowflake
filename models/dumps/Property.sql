/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.Property --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.Property --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.Property --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='Property') }}

SELECT prop.name AS "name"
    , prop.propertyLegalName AS "propertyLegalName"
    , prop.displayName AS "displayName"
    , prop.timezone AS "propTimezone"
    , TRANSLATE(utc_offset, ':0', '') AS "offset"
    , prop.externalId AS "externalId"
    , prop.rmsExternalId AS "rmsExternalId"
    , COALESCE(a.city, 'N/A') AS "city"
    , COALESCE(a.STATE, 'N/A') AS "state"
    , COALESCE(a.postalCode, 'N/A') AS "postalCode"
    , CONVERT_TIMEZONE(prop.timezone, prop.created_at)::TIMESTAMP_NTZ AS "createdDate"
    , CONVERT_TIMEZONE(prop.timezone, prop.startDate)::TIMESTAMP_NTZ AS "acquisitionDate"
    , CONVERT_TIMEZONE(prop.timezone, prop.endDate)::TIMESTAMP_NTZ AS "dispositionDate"
    , prop.inactive AS "inactive"
    , CASE WHEN prop.inactive = TRUE THEN 'x' ELSE NULL END AS "inactiveFlag"
    , CASE WHEN prop.inactive = TRUE THEN 'Inactive' ELSE 'Active' END "inactiveFlagNN"
    , CASE
        WHEN prop.name = 'swparkme'
            THEN 'Property1'
        WHEN prop.name = 'cove'
            THEN 'Property2'
        WHEN prop.name = 'lark'
            THEN 'Property3'
        WHEN prop.name = 'wood'
            THEN 'Property4'
        WHEN prop.name = 'sharon'
            THEN 'Property5'
        WHEN prop.name = 'shore'
            THEN 'Property6'
        ELSE 'UNKNOWN'
        END AS "FakeNameForDemo"
FROM {{ var("source_tenant") }}.Property AS prop
INNER JOIN {{ var("source_tenant") }}.PG_TIMEZONE_NAMES AS tzn ON tzn.name = prop.timezone
LEFT JOIN {{ var("source_tenant") }}.Address AS a ON a.id = prop.addressId
