/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.QuoteDetails --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.QuoteDetails --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.QuoteDetails --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='QuoteDetails') }}

SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , p.id AS "partyIdNoURL"
    , prop.name AS "property"
    , q.id AS "quoteId"
    , i.name AS "inventoryName"
    , ig.name AS "inventoryGroupName"
    , i.externalId AS "inventoryExternalId"
    , i.id AS "inventoryId"
    , lay.name AS "layoutName"
    , lay.numBedrooms AS "numBedrooms"
    , lay.numBathrooms AS "numBathrooms"
    , lay.numBedrooms::TEXT || 'x' || lay.numBathrooms::TEXT AS "BedBath"
    , CONVERT_TIMEZONE(prop.timezone, q.publishDate)::TIMESTAMP_NTZ AS "quotePublishDate"
    , CONVERT_TIMEZONE(prop.timezone, q.expirationDate)::TIMESTAMP_NTZ AS "quoteExpirationDate"
    , CONVERT_TIMEZONE(prop.timezone, q.leaseStartDate)::TIMESTAMP_NTZ AS "quoteLeaseStartDate"
    , COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0) AS "quoteOriginalBaseRent"
    , COALESCE((q.leaseTerms: overwrittenBaseRent)::DECIMAL, 0) AS "quoteOverwrittenBaseRent"
    , COALESCE(pq.promotionStatus, 'unpromoted') AS "promotionStatus"
    , CASE
        WHEN COALESCE(pq.promotionStatus, 'canceled') = 'canceled'
            THEN 0
        ELSE 1
        END AS "isPromoted"
    , coalesce(CONVERT_TIMEZONE(prop.timezone, pq.created_at), '1900-01-01')::TIMESTAMP_NTZ AS "quotePromotionDate"
    , q.termLength AS "termLength"
    , CASE
        WHEN l.STATUS IS NULL
            THEN 'no lease generated'
        ELSE l.STATUS
        END AS "leaseStatus"
    , u.fullName AS "quoteLastModifiedBy"
    , 1 AS "oldProcessingMinutes"
    , CASE
        WHEN lower('{{ var("client") }}') = 'customernew'
            THEN 0.35
        ELSE 0.33
        END AS "newProcessingMinutes"
FROM {{ var("source_tenant") }}.Party AS p
INNER JOIN (
    SELECT fl.value AS leaseTerms
        , publishedQuoteData: leaseTerms [fl.index] :termLength::VARCHAR AS termLength
        , q0.*
    FROM {{ var("source_tenant") }}.QUOTE q0
        , LATERAL flatten(input => q0.publishedQuoteData: leaseTerms) AS fl
    ) AS q ON q.partyId = p.id
INNER JOIN {{ var("source_tenant") }}.Inventory AS i ON i.id = q.inventoryId
INNER JOIN {{ var("source_tenant") }}.Layout AS lay ON lay.id = i.layoutId
INNER JOIN {{ var("source_tenant") }}.InventoryGroup AS ig ON ig.id = i.inventoryGroupId
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = i.propertyId
LEFT OUTER JOIN (
    SELECT *
    FROM (
        SELECT rank() OVER (
                PARTITION BY quoteId ORDER BY created_at DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.PartyQuotePromotions
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) AS pq ON pq.quoteId = q.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Lease AS l ON l.quoteId = q.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = q.modified_by
