/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.AP_Tours --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='AP_Tours') }}

-- depends on: {{ ref('PartyDump') }}
-- depends on: {{ ref('TourDump') }}
-- depends on: {{ ref('PersonDump') }}

SELECT t."partyId"
    , t."tourCreateDate"::TIMESTAMP_NTZ AS "DateScheduled"
    , t."tourStartDate"::TIMESTAMP_NTZ  AS "DateOfTour"
    , t."completionDate"::TIMESTAMP_NTZ  AS "DateMarkedComplete"
    , t."tourResultNN" AS "TourResult"
    , t."taskOwners" AS "TourOwners"
    , t."tourProperties" AS "TourProperty"
    , per."id" AS "ContactId"
    , per."firstName" AS "FirstName"
    , per."fullName" AS "FullName"
    , per."email" AS "Email"
    , per."phone" AS "Phone"
    , p."closeReasonNonNull" AS "CloseReason"
    , p."PropertyNonNull" AS "Property"
    , p."agentName" AS "Agent"
    , p."PartyCreatedDate"::TIMESTAMP_NTZ  AS "DateCreated"
    , p."PartyClosedDate"::TIMESTAMP_NTZ  AS "DateClosed"
    , p."mostRecentCommDate" AS "PartyLastComm"
    , CASE
        WHEN per."mostRecentCommDate" = to_date('1900-01-01')
            THEN NULL
        ELSE per."mostRecentCommDate"
        END::TIMESTAMP_NTZ  AS "ContactLastComm"
    , per."personCreated"::TIMESTAMP_NTZ  AS "DatePersonCreated"
    , per."dumpGenDate" AS "ListCreateDate"
    , p."QQMoveInNN" AS "QQMoveIn"
    , t."tourStartDate"::TIMESTAMP_NTZ  as "tourStartDate"
FROM {{ var("target_schema") }}."TourDump" AS t
INNER JOIN {{ var("target_schema") }}."PartyDump" AS p ON p."partyId" = t."partyId"
INNER JOIN {{ var("target_schema") }}."PersonDump" AS per ON per."partyId" = p."partyId"
WHERE coalesce(per."email", '') <> ''
    AND p."closeReasonNonNull" IN ('Open')
    AND (t."tourStartDate" >= dateadd(DAY, - 3, CURRENT_TIMESTAMP())
    AND t."tourStartDate" <= dateadd(DAY, 2, CURRENT_TIMESTAMP()))
ORDER BY t."tourStartDate"
