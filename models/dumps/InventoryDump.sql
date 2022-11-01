/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.InventoryDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.InventoryDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.InventoryDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='InventoryDump') }}

-- depends on: {{ ref('PartyDump') }}

SELECT CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS "ReportingDate"
    , i.id AS "inventoryId"
    , i.name AS "inventoryName"
    , prop.name AS "propertyName"
    , i.multipleItemTotal AS "multipleItemTotal"
    , i.description AS "description"
    , i.type AS "type"
    , i.floor AS "floor"
    , l.name AS "layoutName"
    , ig.name AS "inventoryGroupName"
    -- used in customernew
    , i.id as "Id"
    , i.name as "Name"
    , ig.name as "GroupName"
    , b.name AS "buildingName"
    , i.parentInventory AS "parentInventory"
    , i.state AS "state"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), i.stateStartDate))::TIMESTAMP AS "stateStartDate"
    , i.externalId AS "externalId"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.created_at))::DATETIME AS "created_at"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.updated_at))::DATETIME AS "updated_at"
    , ig.basePriceMonthly AS "basePriceMonthly"
    , amen.amenities AS "amenities"
    , COALESCE(ig.basePriceMonthly, 0) + COALESCE(amen.amenities, 0) AS "totalPrice"
    , CASE
        WHEN hold.id IS NULL
            THEN 'false'
        ELSE 'true'
        END AS "isOnHold"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || hold.partyId AS "holdParty"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, hold.startDate))::TIMESTAMP AS "holdStart"
    , CASE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
        WHEN '0'
            THEN '0 days'
        ELSE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
        END AS "numDaysOnHold"
    , hold.reason AS "holdReason"
    , holdAgent.fullName AS "holdAgent"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.availabilityDate))::TIMESTAMP AS "availabilityDate"
    , amen.amenityNames AS "amenityNames"
    , igAmen.amenityNames AS "IGAmenityNames"
    , lAmen.amenityNames AS "LayoutAmenityNames"
    , bAmen.amenityNames AS "buildingAmenityNames"
    , propAmen.amenityNames AS "propAmenityNames"
    , CASE i.state
        WHEN 'admin'
            THEN 0
        WHEN 'down'
            THEN 0
        WHEN 'excluded'
            THEN 0
        WHEN 'model'
            THEN 0
        WHEN 'occupied'
            THEN 1
        WHEN 'occupiedNotice'
            THEN 1
        WHEN 'occupiedNoticeReserved'
            THEN 1
        WHEN 'vacantMakeReady'
            THEN 0
        WHEN 'vacantMakeReadyReserved'
            THEN 0
        WHEN 'vacantReady'
            THEN 0
        WHEN 'vacantReadyReserved'
            THEN 0
        END AS "isCurrentlyOccupied"
    , CASE i.STATE
        WHEN 'admin'
            THEN 0
        WHEN 'down'
            THEN 0
        WHEN 'excluded'
            THEN 0
        WHEN 'model'
            THEN 0
        WHEN 'occupied'
            THEN 1
        WHEN 'occupiedNotice'
            THEN 0
        WHEN 'occupiedNoticeReserved'
            THEN 1
        WHEN 'vacantMakeReady'
            THEN 0
        WHEN 'vacantMakeReadyReserved'
            THEN 1
        WHEN 'vacantReady'
            THEN 0
        WHEN 'vacantReadyReserved'
            THEN 1
        END AS "isOccupiedOrReserved"
    , l.numBedrooms::TEXT || 'x' || l.numBathrooms::TEXT AS "BedBath"
    , l.surfaceArea AS SQFT
    , l.numBedrooms AS "numBedrooms"
    , l.numBathrooms AS "numBathrooms"
    , CASE
        WHEN l.numBedrooms = 0
            THEN 1
        ELSE 0
        END AS "isBedsStudio"
    , CASE
        WHEN l.numBedrooms = 1
            THEN 1
        ELSE 0
        END AS "isBedsOne"
    , CASE
        WHEN l.numBedrooms = 2
            THEN 1
        ELSE 0
        END AS "isBedsTwo"
    , CASE
        WHEN l.numBedrooms = 3
            THEN 1
        ELSE 0
        END AS "isBedsThree"
    , CASE
        WHEN l.numBedrooms = 4
            THEN 1
        ELSE 0
        END AS "isBedsFour"
    , children.childInventoryNames AS "childInventoryNames"
    , children.childInventoryDescriptions AS "childInventoryDescriptions"
    , COALESCE(i.inactive, 'false') AS "inventoryInactive"
    , CASE
        WHEN amen.amenities = 0
            THEN ig.basePriceMonthly
        WHEN amen.amenities > 0
            THEN COALESCE(ig.basePriceMonthly, 0) + COALESCE(amen.amenities, 0)
        END AS "MonthlyRent"
    , DATEDIFF(DAY, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), i.stateStartDate))::TIMESTAMP, CURRENT_TIMESTAMP()) AS "daysInState"
    , DATEDIFF(DAY, date_trunc('day', CONVERT_TIMEZONE(prop.timezone, hold.startDate))::TIMESTAMP, CURRENT_TIMESTAMP()) AS "daysOnHoldNum"
    , CASE
        WHEN (
                CASE
                    WHEN hold.id IS NULL
                        THEN 'false'
                    ELSE 'true'
                    END
                ) = 'false'
            THEN ' '
        ELSE (
                CASE
                    WHEN hold.id IS NULL
                        THEN 'false'
                    ELSE 'true'
                    END
                )
        END AS "IsOnHoldDisplay"
    , CASE
        WHEN (date_trunc('day', CONVERT_TIMEZONE(prop.timezone, hold.startDate))::TIMESTAMP) IS NULL
            THEN ' '
        ELSE Replace((date_trunc('day', CONVERT_TIMEZONE(prop.timezone, hold.startDate))::TIMESTAMP)::VARCHAR, '00:00:00.000', '')
        END AS "HoldStartDateDisplay"
    , CASE
        WHEN (
                CASE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
                    WHEN '0'
                        THEN '0 days'
                    ELSE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
                    END
                ) IS NULL
            THEN ' '
        ELSE (
                CASE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
                    WHEN '0'
                        THEN '0 days'
                    ELSE datediff(DAY, hold.startDate, CURRENT_DATE ())::TEXT
                    END
                )
        END AS "NumDaysOnHoldDisplay"
    , CASE
        WHEN holdAgent.fullName IS NULL
            THEN ' '
        ELSE holdAgent.fullName
        END AS "HoldAgentDisplay"
    , CASE
        WHEN b.NAME = 'garden'
            THEN 'Townhome'
        ELSE 'Tower'
        END AS "majorTypeMax"
    , CASE
        WHEN i.STATE = 'occupiedNotice'
            THEN 'On-Notice Unrented'
        WHEN i.STATE = 'vacantMakeReady'
            THEN 'Vacant UnRented'
        WHEN i.STATE = 'vacantReady'
            THEN 'Vacant UnRented'
        ELSE i.STATE
        END AS "inventoryStateDisplay"
    , 1 AS "recordCount"
    , CASE
        WHEN i.STATE = 'occupiedNotice' AND hold.partyId IS NULL
            THEN 1
        ELSE 0
        END AS "OnNoticeNotHeld"
    , COALESCE(iso.LEASEDUTILSTATUS, '') AS "StateOverride"
    , pd."LeaseTypeNN" AS "holdLeaseType"
    , CASE
        WHEN i.STATE = 'admin' OR i.STATE = 'down' OR i.STATE = 'excluded' OR i.STATE = 'model'
            THEN 'DOM'
        WHEN i.STATE = 'occupied'
            THEN 'Occupied'
        WHEN i.STATE = 'occupiedNotice'
            THEN 'Notice'
        WHEN i.STATE = 'occupiedNoticeReserved' OR i.STATE = 'vacantMakeReadyReserved' OR i.STATE = 'vacantReadyReserved'
            THEN 'Reserved'
        WHEN i.STATE = 'vacantMakeReady' OR i.STATE = 'vacantReady'
            THEN 'Vacant'
        ELSE 'ERROR'
        END AS "InventoryStateType"
    , l.externalId AS "layoutExternalId"
FROM {{ var("source_tenant") }}.INVENTORY i
INNER JOIN {{ var("source_tenant") }}.INVENTORYGROUP ig ON ig.id = i.inventoryGroupId
INNER JOIN {{ var("source_tenant") }}.PROPERTY prop ON prop.id = i.propertyId
INNER JOIN {{ var("source_tenant") }}.LAYOUT l ON l.id = i.layoutId
INNER JOIN {{ var("source_tenant") }}.BUILDING b ON b.id = i.buildingId
LEFT OUTER JOIN (
    SELECT ia.inventoryId
        , SUM(a.absolutePrice) AS amenities
        , listagg(DISTINCT a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.AMENITY a
    INNER JOIN {{ var("source_tenant") }}.INVENTORY_AMENITY ia ON ia.amenityId = a.id
    GROUP BY ia.inventoryId
    ) amen ON amen.inventoryId = i.id
LEFT OUTER JOIN (
    SELECT iga.inventoryGroupId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.AMENITY a
    INNER JOIN {{ var("source_tenant") }}.INVENTORYGROUP_AMENITY iga ON iga.amenityId = a.id
    GROUP BY iga.inventoryGroupId
    ) igAmen ON igAmen.inventoryGroupId = ig.id
LEFT OUTER JOIN (
    SELECT la.layoutId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.AMENITY a
    INNER JOIN {{ var("source_tenant") }}.LAYOUT_AMENITY la ON la.amenityId = a.id
    GROUP BY la.layoutId
    ) lAmen ON lAmen.layoutId = l.id
LEFT OUTER JOIN (
    SELECT ba.buildingId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.AMENITY a
    INNER JOIN {{ var("source_tenant") }}.BUILDING_AMENITY ba ON ba.amenityId = a.id
    GROUP BY ba.buildingId
    ) bAmen ON bAmen.buildingId = b.id
LEFT OUTER JOIN (
    SELECT a.propertyId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.AMENITY a
    WHERE a.category = 'property'
    GROUP BY a.propertyId
    ) propAmen ON propAmen.propertyId = prop.id
LEFT OUTER JOIN (
    SELECT ih.*
    FROM {{ var("source_tenant") }}.INVENTORYONHOLD ih
    INNER JOIN (
        SELECT id
            , inventoryId
            , row_number() OVER (
                PARTITION BY inventoryId ORDER BY created_at
                ) AS rowNum
            , created_at
        FROM {{ var("source_tenant") }}.INVENTORYONHOLD
        WHERE endDate IS NULL
        ) firstActiveHold ON firstActiveHold.id = ih.id AND firstActiveHold.rowNum = 1
    ) hold ON hold.inventoryId = i.id
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS holdAgent ON holdAgent.id = hold.heldBy
LEFT OUTER JOIN (
    SELECT listagg(i0.name, ' | ') WITHIN
    GROUP (
            ORDER BY i0.name
            ) AS childInventoryNames
        , listagg(i0.description, ' | ') WITHIN
    GROUP (
            ORDER BY i0.description
            ) AS childInventoryDescriptions
        , i0.parentInventory
    FROM {{ var("source_tenant") }}.INVENTORY i0
    GROUP BY i0.parentInventory
    ) children ON children.parentInventory = i.id
LEFT JOIN {{ var("target_schema") }}."PartyDump" AS pd ON pd."partyIdNoURL" = hold.partyId
LEFT JOIN "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".InventoryStateOverride AS iso ON lower('{{ var("client") }}') = 'maximus' AND i.externalId = iso.unit
