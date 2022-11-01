/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.AP_NewComm --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='AP_NewComm') }}

-- depends on: {{ ref('PartyDump') }}
-- depends on: {{ ref('PersonDump') }}

SELECT per."id" AS "ContactId"
    , per."firstName" AS "FirstName"
    , per."fullName" AS "FullName"
    , per."email" AS "Email"
    , per."phone" AS "Phone"
    , p."closeReasonNonNull" AS "CloseReason"
    , p."PropertyNonNull" AS "Property"
    , p."agentName" AS "Agent"
    , p."PartyCreatedDate"::TIMESTAMP_NTZ AS "DateCreated"
    , p."PartyClosedDate"::TIMESTAMP_NTZ AS "DateClosed"
    , p."mostRecentCommDate"::TIMESTAMP_NTZ AS "PartyLastComm"
    , per."mostRecentCommDate"::TIMESTAMP_NTZ AS "ContactLastComm"
    , per."personCreated"::TIMESTAMP_NTZ AS "DatePersonCreated"
    , per."dumpGenDate"::TIMESTAMP_NTZ AS "ListCreateDate"
    , p."QQMoveInNN" AS "QQMoveIn"
FROM {{ var("target_schema") }}."PersonDump" AS per
INNER JOIN {{ var("target_schema") }}."PartyDump" AS p ON p."partyId" = per."partyId"
WHERE coalesce(per."email", '') <> ''
    AND p."closeReasonNonNull" IN ('FOUND_ANOTHER_PLACE', 'NOT_INTERESTED', 'NO_RESPONSE', 'Open')
    AND (per."mostRecentCommDate" <= CURRENT_TIMESTAMP()
        AND per."mostRecentCommDate" >= dateadd(DAY, - 2, CURRENT_TIMESTAMP()))

