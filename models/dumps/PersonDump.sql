/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.PersonDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.PersonDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.PersonDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='PersonDump') }}

SELECT date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP_NTZ AS "dumpGenDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "dumpGenTime"
    , per.id AS "id"
    , per.fullName AS "fullName"
    , per.preferredName AS "preferredName"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), per.created_at))::TIMESTAMP_NTZ AS "personCreated"
    , split_part(per.fullName, ' ', 1) AS "firstName"
    , split_part(per.fullName, ' ', 2) AS "secondName"
    , split_part(per.fullName, ' ', 3) AS "thirdName"
    , ci.email AS "email"
    , ci.phone AS "phone"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id AS "partyId"
    , p.id AS "partyIdNoURL"
    , pm.memberType AS "partyMemberType"
    , COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), mostRecentComm.commDate), '1900-01-01 00:00:00'::TIMESTAMP)::TIMESTAMP_NTZ AS "mostRecentCommDate"
    , p.workflowName AS "workflowName"
    , p.partyGroupId AS "partyGroupId"
FROM {{ var("source_tenant") }}.Person AS per
LEFT OUTER JOIN {{ var("source_tenant") }}.PartyMember AS pm ON pm.personId = per.id AND pm.endDate IS NULL
LEFT OUTER JOIN {{ var("source_tenant") }}.Party AS p ON p.id = pm.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN (
    SELECT per.id AS personId
        , ci.type
        , ci.value::TEXT AS email
        , ci2.value::TEXT AS phone
    FROM {{ var("source_tenant") }}.Person AS per
    LEFT OUTER JOIN {{ var("source_tenant") }}.ContactInfo AS ci ON ci.personId = per.id AND ci.type = 'email' AND ci.ISPRIMARY = TRUE
    LEFT OUTER JOIN {{ var("source_tenant") }}.ContactInfo AS ci2 ON ci2.personId = per.id AND ci2.type = 'phone' AND ci2.ISPRIMARY = TRUE
    ) AS ci ON ci.personId = per.id
LEFT OUTER JOIN (
    SELECT comms.personId
        , MAX(comms.created_at) AS commDate
    FROM (
        SELECT fl.value::VARCHAR AS personId
            , created_at
        FROM {{ var("source_tenant") }}.Communication
            , LATERAL flatten(input => parse_json(persons)) AS fl
        WHERE direction = 'in'
        ) AS comms
    GROUP BY comms.personId
    ) AS mostRecentComm ON mostRecentComm.personId = per.id::TEXT