/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.DailyStatisticsMaximus --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='DailyStatistics_maximus') }}

{{ config(
    pre_hook='DROP TABLE IF EXISTS "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DailyStatistics"',
    post_hook='ALTER TABLE "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DailyStatistics_maximus" RENAME TO "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DailyStatistics"'
) }}

-- depends on: {{ ref('InventoryDataMaximus') }}
-- depends on: {{ ref('ProdLeaseDump') }}

SELECT
      i."ReportingDate" AS "Report Date"
    , i."propertyName" AS "Property"
    , i."majorTypeMax" AS "Building Type"
    , i."buildingName" AS "Building"
    , i."BedBath" AS "Unit type"
    , i."inventoryId"
    , i."inventoryName" AS "Unit"
    , l."LeaseTypeNN" AS "Lease Type"
    , SUM(i."RecordCount") AS "Total Units"
    , SUM(CASE WHEN i."state" = 'occupied' OR i."state" = 'occupiedNotice' OR i."state" = 'occupiedNoticeReserved' THEN 1 ELSE 0 END) AS "Occupied"
    , SUM(CASE WHEN i."state" = 'occupied' THEN 1 ELSE 0 END) AS "Occupied excluding Notice"
    , SUM(CASE WHEN i."state" = 'occupiedNotice' THEN 1 ELSE 0 END) AS "On Notice"
    , SUM(CASE WHEN i."state" = 'occupiedNoticeReserved' THEN 1 ELSE 0 END) AS "Occupied Notice Reserved"
    , SUM(CASE WHEN i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model' THEN 1 ELSE 0 END) AS "DOM"
    , SUM(CASE WHEN i."state" = 'vacantReady' THEN 1 ELSE 0 END) AS "Vacant Ready"
    , SUM(CASE WHEN i."state" = 'vacantMakeReady' THEN 1 ELSE 0 END) AS "Vacant Make Ready"
    , SUM(CASE WHEN i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved' THEN 1 ELSE 0 END) AS "Pending Move-in"
    , SUM(CASE WHEN i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady' THEN 1 ELSE 0 END) AS "Vacant Rentable"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice' AND i."availabilityDate" < DATEADD('day', 15, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "OnNotice14"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice' AND i."availabilityDate" < DATEADD('day', 31, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "OnNotice30"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice' AND i."availabilityDate" < DATEADD('day', 61, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "OnNotice60"
    , SUM(CASE WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('day', 15, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "Reserved14"
    , SUM(CASE WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('day', 31, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "Reserved30"
    , SUM(CASE WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('day', 61, CURRENT_TIMESTAMP())) THEN 1 ELSE 0 END) AS "Reserved60"
    , SUM(CASE WHEN i."isOnHold" = 'true' THEN 1 ELSE 0 END) AS "Pending Sale"
    , SUM(CASE WHEN (i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady') AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held Vacant"
    , SUM(CASE WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantReadyReserved' OR i."state" = 'vacantMakeReadyReserved') AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held Reserved"
    , SUM(CASE WHEN (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model' OR i."state" = 'occupied') AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held DOM/Occupied"
    , SUM(CASE WHEN i."state" = 'occupiedNotice' AND i."isOnHold" = 'true' THEN 1 ELSE 0 END) AS "Held On Notice"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('day', 15, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held On Notice14"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('day', 31, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held On Notice30"
    , SUM(CASE WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('day', 61, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held On Notice60"
    , SUM(CASE WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('day', 15, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held Vacant14"
    , SUM(CASE WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('day', 31, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held Vacant30"
    , SUM(CASE WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('day', 61, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "Held Vacant60"
    /* The following section is a breakout of 'available to rent' units by those available for near term move-in, versus for future move-in*/
    , SUM(CASE WHEN i."state" = 'vacantReady' AND "isOnHold" = 'false' THEN 1 ELSE 0 END) AS "Rentable Now"
    , SUM(CASE WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false' THEN 1 ELSE 0 END) AS "Rentable Future"
    /* This section is the same as above, but add available date into the mix for near term and future available to rent*/
    , SUM(CASE WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice' OR i."state" = 'vacantReady') AND (i."isOnHold" = 'false') AND (i."availabilityDate" < DATEADD('day', 30, i."availabilityDate")) THEN 1 ELSE 0 END) AS "Rentable Now 2"
    , SUM(CASE WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice' OR i."state" = 'vacantReady') AND (i."isOnHold" = 'false') AND (i."availabilityDate" >= DATEADD('day', 30, i."availabilityDate")) THEN 1 ELSE 0 END) AS "Rentable Future 2"
    , SUM(CASE WHEN ((i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false') OR (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model') THEN 1 ELSE 0 END) AS "Gross Available"
    , SUM(CASE WHEN (i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false' THEN 1 ELSE 0 END) AS "Net Available"
    , SUM(CASE WHEN i."state" = 'vacantReady' THEN 1 ELSE 0 END) AS "Vacant Days"
    , SUM(CASE WHEN i."state" = 'vacantReady' THEN datediff('day', "stateStartDate", "ReportingDate") ELSE 0 END) AS "Cumm Vacant Days"
    , SUM(CASE WHEN i."state" = 'vacantMakeReady' OR i."state" = 'vacantMakeReadyReserved' THEN 1 ELSE 0 END) AS "Vacant MR Days"
    , SUM(CASE WHEN i."state" = 'vacantMakeReady' OR i."state" = 'vacantMakeReadyReserved' THEN datediff('day', "stateStartDate", "ReportingDate") ELSE 0 END) AS "Cumm Vacant MR Days"
    , SUM(i."basePriceMonthly") AS "Base Rent"
    , SUM(CASE WHEN i."state" = 'occupied' THEN 1 ELSE 0 END) AS "sOccupied"
    , SUM(CASE WHEN i."state" = 'occupiedNotice' THEN 1 ELSE 0 END) AS "sOccupiedNotice"
    , SUM(CASE WHEN i."state" = 'occupiedNoticeReserved' THEN 1 ELSE 0 END) AS "sOccupiedNoticeReserved"
    , SUM(CASE WHEN i."state" = 'vacantReady' THEN 1 ELSE 0 END) AS "sVacantReady"
    , SUM(CASE WHEN i."state" = 'vacantReadyReserved' THEN 1 ELSE 0 END) AS "sVacantReadyReserved"
    , SUM(CASE WHEN i."state" = 'vacantMakeReady' THEN 1 ELSE 0 END) AS "sVacantMakeReady"
    , SUM(CASE WHEN i."state" = 'vacantMakeReadyReserved' THEN 1 ELSE 0 END) AS "sVacantMakeReadyReserved"
    , SUM(CASE WHEN (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model' OR i."state" = 'occupied') AND (i."isOnHold" = 'true') THEN 1 ELSE 0 END) AS "sDown"
    , date_trunc('day',i."ReportingDate") as "reportDateOnly"
FROM {{ var("target_schema") }}."InventoryData" AS i
LEFT JOIN {{ var("target_schema") }}."ProdLeaseDump" AS l ON l."inventoryId" = i."inventoryId" AND l."status" = 'executed' AND l."leaseStartDate" > CURRENT_TIMESTAMP()
GROUP BY i."propertyName"
    , i."ReportingDate"
    , i."majorTypeMax"
    , i."buildingName"
    , i."BedBath"
    , i."inventoryId"
    , i."inventoryName"
    , l."LeaseTypeNN"
