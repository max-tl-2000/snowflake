/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.MonthlyStatistics --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='MonthlyStatistics') }}

-- depends on: {{ ref('InventoryDataMaximus') }}
-- depends on: {{ ref('ProdLeaseDump') }}

SELECT i."ReportingMonth" AS "Report Month"
    , i."propertyName" AS "Property"
    , i."majorTypeMax" AS "Building Type"
    /* Specific to Maximus Building Types*/
    , i."buildingName" AS "Building"
    , i."BedBath" AS "Unit type"
    , i."inventoryId"
    -- i.state AS "State",
    , i."inventoryName" AS "Unit"
    , l."LeaseTypeNN" AS "Lease Type"
    /* The following are core metrics associated with inventory state groups for dashboard metrics */
    , avg(i."RecordCount") AS "Total Units"
    , avg(CASE
            WHEN i."state" = 'occupied' OR i."state" = 'occupiedNotice' OR i."state" = 'occupiedNoticeReserved'
                THEN 1
            ELSE 0
            END) AS "Occupied"
    , avg(CASE
            WHEN i."state" = 'occupied'
                THEN 1
            ELSE 0
            END) AS "Occupied excluding Notice"
    , avg(CASE
            WHEN i."state" = 'occupiedNotice'
                THEN 1
            ELSE 0
            END) AS "On Notice"
    , avg(CASE
            WHEN i."state" = 'occupiedNoticeReserved'
                THEN 1
            ELSE 0
            END) AS "Occupied Notice Reserved"
    , avg(CASE
            WHEN i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model'
                THEN 1
            ELSE 0
            END) AS "DOM"
    , avg(CASE
            WHEN i."state" = 'vacantReady'
                THEN 1
            ELSE 0
            END) AS "Vacant Ready"
    , avg(CASE
            WHEN i."state" = 'vacantMakeReady'
                THEN 1
            ELSE 0
            END) AS "Vacant Make Ready"
    , avg(CASE
            WHEN i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved'
                THEN 1
            ELSE 0
            END) AS "Pending Move-in"
    , avg(CASE
            WHEN i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady'
                THEN 1
            ELSE 0
            END) AS "Vacant Rentable"
    /* The following are associated with Calculating Notice and Reserved Units at 14 and 30 days.  */
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 15, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "OnNotice14"
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 31, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "OnNotice30"
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 61, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "OnNotice60"
    , avg(CASE
            WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('days', 15, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "Reserved14"
    , avg(CASE
            WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('days', 31, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "Reserved30"
    , avg(CASE
            WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantMakeReadyReserved' OR i."state" = 'vacantReadyReserved') AND (l."leaseStartDate" < DATEADD('days', 61, CURRENT_TIMESTAMP()))
                THEN 1
            ELSE 0
            END) AS "Reserved60"
    /* The following section is a breakout of HELD units.  Held is a surrogate for Pending Sales.  But at times non-sellable units are held (e.g. holding for a transfer) and these might
		need to be broken out of the "Pending Sale" number.  */
    , avg(CASE
            WHEN i."isOnHold" = 'true'
                THEN 1
            ELSE 0
            END) AS "Pending Sale"
    , avg(CASE
            WHEN (i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady') AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held Vacant"
    , avg(CASE
            WHEN (i."state" = 'occupiedNoticeReserved' OR i."state" = 'vacantReadyReserved' OR i."state" = 'vacantMakeReadyReserved') AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held Reserved"
    , avg(CASE
            WHEN (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model' OR i."state" = 'occupied') AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held DOM/Occupied"
    , avg(CASE
            WHEN i."state" = 'occupiedNotice' AND i."isOnHold" = 'true'
                THEN 1
            ELSE 0
            END) AS "Held On Notice"
    /* Future Held used to calculate future occupancy */
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 15, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held On Notice14"
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 31, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held On Notice30"
    , avg(CASE
            WHEN (i."state" = 'occupiedNotice') AND (i."availabilityDate" < DATEADD('days', 61, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held On Notice60"
    , avg(CASE
            WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('days', 15, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held Vacant14"
    , avg(CASE
            WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('days', 31, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held Vacant30"
    , avg(CASE
            WHEN ((i."state" = 'vacantReady') OR (i."state" = 'vacantMakeReady')) AND (i."availabilityDate" < DATEADD('days', 61, CURRENT_TIMESTAMP())) AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "Held Vacant60"
    /* The following section is a breakout of 'available to rent' units by those available for near term move-in, versus for future move-in*/
    , avg(CASE
            WHEN i."state" = 'vacantReady' AND "isOnHold" = 'false'
                THEN 1
            ELSE 0
            END) AS "Rentable Now"
    , avg(CASE
            WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false'
                THEN 1
            ELSE 0
            END) AS "Rentable Future"
    /* This section is the same as above, but add available date into the mix for near term and future available to rent*/
    , avg(CASE
            WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice' OR i."state" = 'vacantReady') AND (i."isOnHold" = 'false') AND (i."availabilityDate" < DATEADD('days', 30, i."availabilityDate"))
                THEN 1
            ELSE 0
            END) AS "Rentable Now 2"
    , avg(CASE
            WHEN (i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice' OR i."state" = 'vacantReady') AND (i."isOnHold" = 'false') AND (i."availabilityDate" >= DATEADD('days', 30, i."availabilityDate"))
                THEN 1
            ELSE 0
            END) AS "Rentable Future 2"
    /* This section will pre-calculate Gross and Net Available */
    , avg(CASE
            WHEN ((i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false') OR (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model')
                THEN 1
            ELSE 0
            END) AS "Gross Available"
    , avg(CASE
            WHEN (i."state" = 'vacantReady' OR i."state" = 'vacantMakeReady' OR i."state" = 'occupiedNotice') AND i."isOnHold" = 'false'
                THEN 1
            ELSE 0
            END) AS "Net Available"
    /* Metrics Captured for Vacancy and Turnover Analytics */
    , avg(CASE
            WHEN i."state" = 'vacantReady'
                THEN 1
            ELSE 0
            END) AS "Vacant Days"
    , avg(CASE
            WHEN i."state" = 'vacantReady'
                THEN DATEDIFF('day', "stateStartDate", "ReportingDate")
            ELSE 0
            END) AS "Cumm Vacant Days"
    , avg(CASE
            WHEN i."state" = 'vacantMakeReady' OR i."state" = 'vacantMakeReadyReserved'
                THEN 1
            ELSE 0
            END) AS "Vacant MR Days"
    , avg(CASE
            WHEN i."state" = 'vacantMakeReady' OR i."state" = 'vacantMakeReadyReserved'
                THEN DATEDIFF('day', "stateStartDate", "ReportingDate")
            ELSE 0
            END) AS "Cumm Vacant MR Days"
    /* Metrics to Capture Price information */
    , avg(i."basePriceMonthly") AS "Base Rent"
    /* Flattened Unit Count by State */
    , avg(CASE
            WHEN i."state" = 'occupied'
                THEN 1
            ELSE 0
            END) AS "sOccupied"
    , avg(CASE
            WHEN i."state" = 'occupiedNotice'
                THEN 1
            ELSE 0
            END) AS "sOccupiedNotice"
    , avg(CASE
            WHEN i."state" = 'occupiedNoticeReserved'
                THEN 1
            ELSE 0
            END) AS "sOccupiedNoticeReserved"
    , avg(CASE
            WHEN i."state" = 'vacantReady'
                THEN 1
            ELSE 0
            END) AS "sVacantReady"
    , avg(CASE
            WHEN i."state" = 'vacantReadyReserved'
                THEN 1
            ELSE 0
            END) AS "sVacantReadyReserved"
    , avg(CASE
            WHEN i."state" = 'vacantMakeReady'
                THEN 1
            ELSE 0
            END) AS "sVacantMakeReady"
    , avg(CASE
            WHEN i."state" = 'vacantMakeReadyReserved'
                THEN 1
            ELSE 0
            END) AS "sVacantMakeReadyReserved"
    , avg(CASE
            WHEN (i."state" = 'admin' OR i."state" = 'down' OR i."state" = 'excluded' OR i."state" = 'model' OR i."state" = 'occupied') AND (i."isOnHold" = 'true')
                THEN 1
            ELSE 0
            END) AS "sDown"
FROM {{ var("target_schema") }}."InventoryData" AS i
LEFT JOIN {{ var("target_schema") }}."ProdLeaseDump" AS l ON l."inventoryId" = i."inventoryId" AND l."status" = 'executed' AND l."leaseStartDate" > CURRENT_TIMESTAMP()
GROUP BY i."propertyName"
    , i."ReportingMonth"
    , l."LeaseTypeNN"
    , i."majorTypeMax"
    , i."buildingName"
    , i."BedBath"
    , i."inventoryId"
    , i."inventoryName"
