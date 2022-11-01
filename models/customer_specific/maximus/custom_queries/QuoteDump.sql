/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.QuoteDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='QuoteDump') }}

WITH InventoryDetails
AS (
    SELECT prop.name AS property
        , i.name AS inventoryName
        , ig.name AS inventoryGroupName
        , i.externalId AS inventoryExternalId
        , i.id AS inventoryId
        , lay.name AS layoutName
        , b.name AS buildingName
        , i.availabilityDate
        , lay.numBedrooms::TEXT || 'x' || lay.numBathrooms::TEXT AS BedBath
        , prop.timezone
    FROM {{ var("source_tenant") }}.INVENTORY AS i
    INNER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = i.propertyId
    INNER JOIN {{ var("source_tenant") }}.INVENTORYGROUP AS ig ON ig.id = i.inventoryGroupId
    INNER JOIN {{ var("source_tenant") }}.LAYOUT AS lay ON lay.id = i.layoutId
    INNER JOIN {{ var("source_tenant") }}.BUILDING AS b ON b.id = i.buildingId
    WHERE i.type = 'unit'
    )
SELECT
     'https://' || '{{ var("client") }}' || '.reva.tech/party/' || q.partyId::TEXT AS "partyId"
    , q.partyId AS "partyIdNoURL"
    , id.property AS "Property"
    , q.id AS "quoteId"
    , id.inventoryName AS "inventoryName"
    , id.inventoryGroupName AS "inventoryGroupName"
    , id.inventoryExternalId AS "inventoryExternalId"
    , id.inventoryId AS "inventoryId"
    , id.layoutName AS "layoutName"
    , id.buildingName AS "buildingName"
    , CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'),id.availabilityDate)::TIMESTAMP_NTZ AS "availabilityDate"
    , CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'),hold.holdDate)::TIMESTAMP_NTZ AS "holdDate"
    , CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'), q.publishDate)::TIMESTAMP_NTZ AS "publishDate"
    , CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'), q.expirationDate)::TIMESTAMP_NTZ AS "quoteExpirationDate"
    , CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'), q.leaseStartDate)::TIMESTAMP_NTZ AS "quoteLeaseStartDate"
    , COALESCE(pq.promotionStatus, 'unpromoted') AS "promotionStatus"
    , COALESCE(CONVERT_TIMEZONE(COALESCE(id.timezone, '{{ var("timezone") }}'), pq.created_at), '1900-01-01'::TIMESTAMP)::TIMESTAMP_NTZ AS "quotePromotionDate"
    , COALESCE(promotedQuoteOrigBaseRent, 0) AS "promotedQuoteOrigBaseRent"
    , COALESCE(promotedQuoteOverwrittenBaseRent, 0) AS "promotedQuoteOverwrittenBaseRent"
    , CASE
        WHEN l.STATUS IS NULL
            THEN 'no lease generated'
        ELSE l.STATUS
        END AS "leaseStatus"
    , id.BedBath AS "BedBath"
    , CASE
        WHEN q.createdFromCommId IS NULL
            THEN 0
        ELSE 1
        END AS "isSelfService"
    , CASE
        WHEN hold.holdDate IS NULL
            THEN 0
        ELSE 1
        END AS "IsHeld"
FROM {{ var("source_tenant") }}.QUOTE AS q
INNER JOIN InventoryDetails AS id ON id.inventoryId = q.inventoryId
LEFT OUTER JOIN (
    SELECT pquote.id AS quoteId
        , pquote.created_at
        , pquote.partyId
        , pqp.promotionStatus
        , (fl.value: termLength)::INT AS termLength
        , COALESCE((fl.value: originalBaseRent)::DECIMAL, 0) AS promotedQuoteOrigBaseRent
        , COALESCE((fl.value: overwrittenBaseRent)::DECIMAL, 0) AS promotedQuoteOverwrittenBaseRent
    FROM (
        SELECT rank() OVER (
                PARTITION BY quoteId ORDER BY created_at DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.PARTYQUOTEPROMOTIONS
        ) AS pqp
    INNER JOIN {{ var("source_tenant") }}.QUOTE pquote ON pquote.id = pqp.quoteId
        , LATERAL flatten(input => publishedQuoteData: leaseTerms) AS fl
    WHERE pqp.theRank = 1
    ) AS pq ON pq.quoteId = q.id
LEFT OUTER JOIN {{ var("source_tenant") }}.LEASE AS l ON l.quoteId = q.id
LEFT OUTER JOIN (
    SELECT inventoryId
        , partyId
        , min(startDate) AS holdDate
    FROM {{ var("source_tenant") }}.INVENTORYONHOLD
    WHERE endDate IS NULL
    GROUP BY inventoryId
        , partyId
    ) AS hold ON hold.inventoryId = id.inventoryId AND hold.partyId = q.partyId
