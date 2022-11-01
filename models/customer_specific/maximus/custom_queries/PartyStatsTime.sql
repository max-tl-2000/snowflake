/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.PartyStatsTime --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='PartyStatsTime') }}

-- depends on: {{ ref('ReportingAttributes') }}
-- depends on: {{ ref('PartyDump') }}
-- depends on: {{ ref('InventoryDump') }}

SELECT p."Property" AS "Property"
    , p."SourceSpecial" AS "Source"
    , p."partyId" AS "Party ID"
    /* possible to remove the Party ID and aggregate below with Sum.  No real value in having this broken out at Party ID level */
    , t."ONLYDATE" AS "Report Date"
    , p."reportingStatus" AS "Include in Reporting"
    , r."Region" AS "Region"
    , p."campaignName" AS "Campaign"
    , p."agentName" AS "Agent"
    , CASE
        WHEN p."PartyCreatedDate" = t."ONLYDATE"
            THEN 1
        ELSE 0
        END AS "Contact"
    , CASE
        WHEN p."PartyCreatedDate" = t."ONLYDATE" AND p."IsQualified" = 1
            THEN 1
        ELSE 0
        END AS "Qualified"
    , CASE
        WHEN DATE_FROM_PARTS(DATE_PART('year', p."FCTTourDate"), DATE_PART('month', p."FCTTourDate"), DATE_PART('day', p."FCTTourDate")) = t."ONLYDATE"
            THEN 1
        ELSE 0
        END AS "Tour"
    , CASE
        WHEN DATE_FROM_PARTS(DATE_PART('year', p."firstAppSubmissionDate"), DATE_PART('month', p."firstAppSubmissionDate"), DATE_PART('day', p."firstAppSubmissionDate")) = t."ONLYDATE"
            THEN 1
        ELSE 0
        END AS "Applied"
    , CASE
        WHEN p."signDate" = t."ONLYDATE" AND p."HasSigned" = 1
            THEN 1
        ELSE 0
        END AS "Sign"
    , CASE
        WHEN p."HasSigned" = 1
            THEN datediff('day', p."FCTTourDate", p."signDate")
        ELSE 0
        END AS "DaysTour2Sign"
    , CASE
        WHEN p."HasSigned" = 1
            THEN abs(datediff('day', p."signDate", p."PartyCreatedDate"))
        ELSE 0
        END AS "SalesCycleDays"
    , CASE
        WHEN i."isOnHold" = 'true'
            THEN 1
        ELSE 0
        END AS "Inventory Held"
    , CASE
        WHEN p."isRenewal" = 0
            THEN 'New'
        ELSE 'Renewal'
        END AS "Lease Type"
    , 0 AS "$PSF"
    , CASE
        WHEN p."HasSigned" = 1
            THEN p."daysToClose"
        ELSE 0
        END AS "DaysToClose"
    , CASE
        WHEN DATE_FROM_PARTS(DATE_PART('year', p."tourCreateDate"), DATE_PART('month', p."tourCreateDate"), DATE_PART('day', p."tourCreateDate")) = t."ONLYDATE"
            THEN 1
        ELSE 0
        END AS "ScheduleTour"
FROM {{ var("target_schema") }}."d_Date" AS t
LEFT JOIN {{ var("target_schema") }}."PartyDump" AS p ON
            (t."ONLYDATE" = p."PartyCreatedDate"
            OR t."ONLYDATE" = DATE_FROM_PARTS(DATE_PART('year', p."FCTTourDate"), DATE_PART('month', p."FCTTourDate"), DATE_PART('day', p."FCTTourDate"))
            OR t."ONLYDATE" = DATE_FROM_PARTS(DATE_PART('year', p."tourCreateDate"), DATE_PART('month', p."tourCreateDate"), DATE_PART('day', p."tourCreateDate"))
            OR t."ONLYDATE" = DATE_FROM_PARTS(DATE_PART('year', p."firstAppSubmissionDate"), DATE_PART('month', p."firstAppSubmissionDate"), DATE_PART('day', p."firstAppSubmissionDate"))
            OR t."ONLYDATE" = p."signDate")
LEFT JOIN {{ var("target_schema") }}."InventoryDump" AS i ON t."ONLYDATE" = i."holdStart" AND i."holdParty" = p."partyId"
LEFT JOIN {{ var("target_schema") }}."ReportingAttributes" AS r ON p."Property" = r."PropertyName"
WHERE p."partyId" IS NOT NULL
    AND t."ONLYDATE" > '2016-01-01'
