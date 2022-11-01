/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.AgentCalendarEvents --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.AgentCalendarEvents --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.AgentCalendarEvents --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='AgentCalendarEvents') }}

SELECT CONVERT_TIMEZONE('{{ var("timezone") }}', uce.created_at)::TIMESTAMP_NTZ AS "created_at"
    , CONVERT_TIMEZONE('{{ var("timezone") }}', uce.startDate)::TIMESTAMP_NTZ AS "startDate"
    , CONVERT_TIMEZONE('{{ var("timezone") }}', uce.endDate)::TIMESTAMP_NTZ AS "endDate"
    , REPLACE(uce.metadata: type::VARCHAR, '"', '') AS "type"
    , u.fullName AS "agentName"
    , COALESCE(prop.name, CASE
            WHEN uce.metadata: type = 'personal'
                THEN 'N/A'
            ELSE '[None Selected]'
            END) AS "tourProperty"
    , t.STATE AS "tourState"
    , COALESCE(t.partyId::TEXT, 'N/A') AS "partyId"
    , u.email AS "agentEmail"
    , CASE
        WHEN REPLACE(uce.metadata: type::VARCHAR, '"', '') = 'personal'
            THEN 'Personal'
        ELSE t.STATE
        END AS "tourStateNN"
FROM {{ var("source_tenant") }}.UserCalendarEvents AS uce
INNER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = uce.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.Tasks AS t ON t.id::TEXT = REPLACE(uce.metadata: id, '"', '')::VARCHAR
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id::TEXT = REPLACE(t.metadata: selectedPropertyId, '"', '')::VARCHAR

UNION ALL

SELECT fma.created_at::TIMESTAMP_NTZ AS created_at
    , fma.day::TIMESTAMP_NTZ AS startDate
    , DATEADD(DAY, 1, fma.day::TIMESTAMP)::TIMESTAMP_NTZ AS endDate
    , 'float' AS type
    , u.fullName AS agentName
    , totalAvail.property
    , 'N/A' AS tourState
    , 'N/A' AS partyId
    , u.email AS agentEmail
    , 'N/A' AS tourStateNN
FROM {{ var("source_tenant") }}.FloatingMemberAvailability AS fma
INNER JOIN {{ var("source_tenant") }}.TeamMembers AS tm ON tm.id = fma.teamMemberId
INNER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = tm.userId
INNER JOIN (
    SELECT u.fullName AS agentName
        , prop.name AS property
        , t.name AS teamName
        , u.id AS userId
        , tm.id AS teamMemberId
    FROM {{ var("source_tenant") }}.Users AS u
    INNER JOIN {{ var("source_tenant") }}.TeamMembers AS tm ON tm.userId = u.id
    INNER JOIN {{ var("source_tenant") }}.Teams AS t ON t.id = tm.teamId
    INNER JOIN {{ var("source_tenant") }}.TeamProperties AS tp ON tp.teamId = t.id
    INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = tp.propertyId
    ) AS totalAvail ON totalAvail.userId = u.id AND totalAvail.teamMemberId <> tm.id
