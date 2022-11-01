/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.Users --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.Users --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.Users --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='Users') }}

SELECT u.fullName AS "fullName"
    , u.preferredName AS "preferredName"
    , u.email AS "email"
    , COALESCE(u.displayEmail, '[Missing]') AS "displayEmail"
    , COALESCE(u.displayPhoneNumber, '[Missing]') AS "displayPhoneNumber"
    , COALESCE(u.metadata: businessTitle, '[Missing]') AS "businessTitle"
    , t.name AS "teamName"
    , t.displayName AS "teamDisplayName"
    , t.module AS "teamModuleAccess"
    , t.timeZone AS "teamTimeZone"
    , CASE
        WHEN tm.inactive = 'false'
            THEN 0
        ELSE 1
        END AS "isInactive"
    , REPLACE(mr.value, '"', '') AS "mainRole"
    , translate(CASE
        WHEN functionalRoles = '[]'
            THEN array_construct('None')
        ELSE functionalRoles
        END::varchar,'"[]','') AS "functionalRoles"
    , prop.name AS "propertyName"
    , prop.propertyLegalName AS "propertyLegalName"
    , prop.displayName AS "propertyDisplayName"
    , prop.timezone AS "propertyTimezone"
    , prop.externalId AS "propertyExternalId"
    , COALESCE(isAnLA.isTrue, 0) AS "isAnLA"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',u.created_at)::TIMESTAMP_NTZ AS "createdDate"
FROM {{ var("source_tenant") }}.Users AS u
INNER JOIN {{ var("source_tenant") }}.TeamMembers AS tm ON tm.userId = u.id
INNER JOIN {{ var("source_tenant") }}.Teams AS t ON t.id = tm.teamId
INNER JOIN {{ var("source_tenant") }}.TeamProperties AS tp ON tp.teamId = t.id
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = tp.propertyId
LEFT OUTER JOIN (
    SELECT DISTINCT userId
        , 1 AS isTrue
    FROM {{ var("source_tenant") }}.TeamMembers
        , LATERAL FLATTEN(input => mainRoles) AS mrs
    WHERE mrs.value = 'LA'
    ) AS isAnLA ON isAnLA.userId = u.id
    , LATERAL FLATTEN(input => tm.mainRoles) AS mr