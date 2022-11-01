/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.InventoryDataLastDay --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.InventoryDataLastDay --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.InventoryDataLastDay --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='InventoryDataLastDay')}}

-- depends on: {{ ref('ActiveLeaseWorkflowData') }}

SELECT
      HASH((date_trunc(day,CONVERT_TIMEZONE('{{ var("timezone") }}',CURRENT_TIMESTAMP()))::TIMESTAMP_NTZ)::VARCHAR || '-' || i.id::VARCHAR)::VARCHAR as "inventorydatahashid"
    , CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS "ReportingDate"
    , date_trunc('day',CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ) AS "ReportingDateOnly"
    , i.id AS "inventoryId"
    , i.name AS "inventoryName"
    , prop.name AS "propertyName"
    , i.multipleItemTotal AS "multipleItemTotal"
    , i.description AS "description"
    , i.type AS "type"
    , i.floor AS "floor"
    , l.name AS "layoutName"
    , ig.name AS "inventoryGroupName"
    , b.name AS "buildingName"
    , i.parentInventory AS "parentInventory"
    , i.STATE AS "state"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.stateStartDate))::TIMESTAMP_NTZ AS "stateStartDate"
    , i.externalId AS "externalId"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.created_at))::TIMESTAMP_NTZ AS "created_at"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, i.updated_at))::TIMESTAMP_NTZ AS "updated_at"
    , ig.basePriceMonthly AS "basePriceMonthly"
    , amen.amenities AS "amenities"
    , COALESCE(ig.basePriceMonthly, 0) + COALESCE(amen.amenities, 0) AS "totalPrice_old"
    , CASE
        WHEN hold.id IS NULL
            THEN 'false'
        ELSE 'true'
        END AS "isOnHold"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || hold.partyId AS "holdParty"
    , date_trunc('day', CONVERT_TIMEZONE(prop.timezone, hold.startDate))::TIMESTAMP_NTZ AS "holdStart"
    , CASE datediff(day, CONVERT_TIMEZONE(prop.timezone, hold.startDate), CONVERT_TIMEZONE(prop.timezone, CURRENT_DATE ()))::TEXT
        WHEN '0'
            THEN '0'
        ELSE datediff(day, CONVERT_TIMEZONE(prop.timezone, hold.startDate)::TIMESTAMP_NTZ, CONVERT_TIMEZONE(prop.timezone, CURRENT_DATE ()))::TEXT
        END || ' days' AS "numDaysOnHold"
    , hold.reason AS "holdReason"
    , holdAgent.fullName AS "holdAgent"
    , CONVERT_TIMEZONE(prop.timezone, i.availabilityDate)::TIMESTAMP_NTZ AS "availabilityDate"
    , amen.amenityNames AS "amenityNames"
    , igAmen.amenityNames AS "IGAmenityNames"
    , lAmen.amenityNames AS "LayoutAmenityNames"
    , bAmen.amenityNames AS "buildingAmenityNames"
    , propAmen.amenityNames AS "propAmenityNames"
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
    , 1 AS "RecordCount"
    , CASE
        WHEN b.NAME = 'garden'
            THEN 'Townhome'
        ELSE 'Tower'
        END AS "majorTypeMax"
    , DATE_PART('year', CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_TIMESTAMP()))::VARCHAR || DAYOFYEAR(CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_TIMESTAMP()))::VARCHAR AS "yeardaynum"
    , CASE WHEN "basePriceMonthly" IS NULL THEN 0 ELSE "basePriceMonthly" END + CASE WHEN "amenities" IS NULL THEN 0 ELSE "amenities" END AS "totalPrice"
    , DATE_FROM_PARTS(DATE_PART(year, "ReportingDate"), DATE_PART(month, "ReportingDate"), 1)::TIMESTAMP_NTZ AS "ReportingMonth"
    , CONVERT_TIMEZONE(prop.timezone, i.availabilityDate)::TIMESTAMP_NTZ AS "availabilityDate2"
    , CONVERT_TIMEZONE(prop.timezone, i.availabilityDate)::TIMESTAMP_NTZ AS "availabilityDate3"
    , ad."leaseStartDate"::TIMESTAMP_NTZ AS "leaseStart"
    , ad."leaseEndDate"::TIMESTAMP_NTZ AS "leaseEnd"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), q."quoteLeaseStartDate")::TIMESTAMP_NTZ AS "leaseStartQuote" -- only used for customernew
    , ad."vacateDate"::TIMESTAMP_NTZ AS "vacateDate"
    , ad."dateOfTheNotice"::TIMESTAMP_NTZ AS "dateOfTheNotice"
FROM {{ var("source_tenant") }}.Inventory i
INNER JOIN {{ var("source_tenant") }}.InventoryGroup ig ON ig.id = i.inventoryGroupId
INNER JOIN {{ var("source_tenant") }}.Property prop ON prop.id = i.propertyId
INNER JOIN {{ var("source_tenant") }}.Layout l ON l.id = i.layoutId
INNER JOIN {{ var("source_tenant") }}.Building b ON b.id = i.buildingId
LEFT OUTER JOIN (
    SELECT ia.inventoryId
        , any_value(ia.ID)
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') within
    GROUP (
            ORDER BY a.NAME
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.Amenity a
    INNER JOIN {{ var("source_tenant") }}.Inventory_Amenity ia ON ia.amenityId = a.id
    GROUP BY ia.inventoryId
    ) amen ON amen.inventoryId = i.id
LEFT OUTER JOIN (
    SELECT iga.inventoryGroupId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.Amenity a
    INNER JOIN {{ var("source_tenant") }}.InventoryGroup_Amenity iga ON iga.amenityId = a.id
    GROUP BY iga.inventoryGroupId
    ) igAmen ON igAmen.inventoryGroupId = ig.id
LEFT OUTER JOIN (
    SELECT la.layoutId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.Amenity a
    INNER JOIN {{ var("source_tenant") }}.Layout_Amenity la ON la.amenityId = a.id
    GROUP BY la.layoutId
    ) lAmen ON lAmen.layoutId = l.id
LEFT OUTER JOIN (
    SELECT ba.buildingId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.name
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.Amenity a
    INNER JOIN {{ var("source_tenant") }}.Building_Amenity ba ON ba.amenityId = a.id
    GROUP BY ba.buildingId
    ) bAmen ON bAmen.buildingId = b.id
LEFT OUTER JOIN (
    SELECT a.propertyId
        , SUM(a.absolutePrice) AS amenities
        , listagg(a.name, ' | ') WITHIN
    GROUP (
            ORDER BY a.NAME
            ) AS amenityNames
    FROM {{ var("source_tenant") }}.Amenity a
    WHERE a.category = 'property'
    GROUP BY a.propertyId
    ) propAmen ON propAmen.propertyId = prop.id
LEFT OUTER JOIN (
    SELECT ih.*
    FROM {{ var("source_tenant") }}.InventoryOnHold ih
    INNER JOIN (
        SELECT id
            , inventoryId
            , row_number() OVER (
                PARTITION BY inventoryId ORDER BY created_at
                ) AS rowNum
            , created_at
        FROM {{ var("source_tenant") }}.InventoryOnHold
        WHERE endDate IS NULL
        ) firstActiveHold ON firstActiveHold.id = ih.id AND firstActiveHold.rowNum = 1
    ) hold ON hold.inventoryId = i.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Users holdAgent ON holdAgent.id = hold.heldBy
LEFT JOIN (
    SELECT listagg(i0.name, ' | ') AS childInventoryNames
        , listagg(i0.description, ' | ') AS childInventoryDescriptions
        , i0.parentInventory
    FROM {{ var("source_tenant") }}.Inventory AS i0
    GROUP BY i0.parentInventory
    ) AS children ON children.parentInventory = i.id
LEFT JOIN {{ var("target_schema") }}."ActiveLeaseWorkflowData" AS ad on ad."inventoryId" = i.ID
LEFT JOIN (
    SELECT inventoryId AS "inventoryId"
        , MIN(leaseStartDate) AS "quoteLeaseStartDate"
    FROM {{ var("source_tenant") }}.Quote
    GROUP BY inventoryId
    ) AS q ON q."inventoryId" = i.id
