/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.InventoryDataMaximus --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

-- depends on: {{ ref('InventoryDataLastDay') }}

{{ config(alias='InventoryDataMaximus')}}
{{ config(
  sql_header='
              MERGE INTO ANALYTICS.SNOW_9F27B14E_6973_48A5_B746_434828265538."InventoryData" AS dest
                    USING ANALYTICS.SNOW_9F27B14E_6973_48A5_B746_434828265538."InventoryDataLastDay" AS src
                    ON
                        src."inventorydatahashid" = dest."inventorydatahashid"
              WHEN MATCHED THEN UPDATE SET
                    "ReportingDate" = src."ReportingDate","ReportingDateOnly" = src."ReportingDateOnly","inventoryId" = src."inventoryId","inventoryName" = src."inventoryName","propertyName" = src."propertyName","multipleItemTotal" = src."multipleItemTotal"
                    ,"description" = src."description","type" = src."type","floor" = src."floor","layoutName" = src."layoutName","inventoryGroupName" = src."inventoryGroupName","buildingName" = src."buildingName"
                    ,"parentInventory" = src."parentInventory","state" = src."state","stateStartDate" = src."stateStartDate","externalId" = src."externalId","created_at" = src."created_at"
                    ,"updated_at" = src."updated_at","basePriceMonthly" = src."basePriceMonthly","amenities" = src."amenities","totalPrice_old" = src."totalPrice_old","isOnHold" = src."isOnHold"
                    ,"holdParty" = src."holdParty","holdStart" = src."holdStart","numDaysOnHold" = src."numDaysOnHold","holdReason" = src."holdReason","holdAgent" = src."holdAgent"
                    ,"availabilityDate" = src."availabilityDate","amenityNames" = src."amenityNames","IGAmenityNames" = src."IGAmenityNames","LayoutAmenityNames" = src."LayoutAmenityNames"
                    ,"buildingAmenityNames" = src."buildingAmenityNames","propAmenityNames" = src."propAmenityNames","isCurrentlyOccupied" = src."isCurrentlyOccupied"
                    ,"isOccupiedOrReserved" = src."isOccupiedOrReserved","BedBath" = src."BedBath","SQFT" = src."SQFT","numBedrooms" = src."numBedrooms","numBathrooms" = src."numBathrooms"
                    ,"isBedsStudio" = src."isBedsStudio","isBedsOne" = src."isBedsOne","isBedsTwo" = src."isBedsTwo","isBedsThree" = src."isBedsThree","isBedsFour" = src."isBedsFour"
                    ,"childInventoryNames" = src."childInventoryNames","childInventoryDescriptions" = src."childInventoryDescriptions","inventoryInactive" = src."inventoryInactive"
                    ,"RecordCount" = src."RecordCount","majorTypeMax" = src."majorTypeMax","yeardaynum" = src."yeardaynum","totalPrice" = src."totalPrice","ReportingMonth" = src."ReportingMonth"
                    ,"availabilityDate2" = src."availabilityDate2","availabilityDate3" = src."availabilityDate3","leaseStart" = src."leaseStart","leaseEnd" = src."leaseEnd"
                    ,"leaseStartQuote" = src."leaseStartQuote","vacateDate" = src."vacateDate","dateOfTheNotice" = src."dateOfTheNotice"
              WHEN NOT MATCHED THEN INSERT
                    ("inventorydatahashid", "ReportingDate", "ReportingDateOnly", "inventoryId", "inventoryName", "propertyName", "multipleItemTotal", "description", "type", "floor", "layoutName", "inventoryGroupName", "buildingName"
                    , "parentInventory", "state", "stateStartDate", "externalId", "created_at", "updated_at", "basePriceMonthly", "amenities", "totalPrice_old", "isOnHold", "holdParty", "holdStart", "numDaysOnHold"
                    , "holdReason", "holdAgent", "availabilityDate", "amenityNames", "IGAmenityNames", "LayoutAmenityNames", "buildingAmenityNames", "propAmenityNames", "isCurrentlyOccupied", "isOccupiedOrReserved"
                    , "BedBath", "SQFT", "numBedrooms", "numBathrooms", "isBedsStudio", "isBedsOne", "isBedsTwo", "isBedsThree", "isBedsFour", "childInventoryNames", "childInventoryDescriptions", "inventoryInactive"
                    , "RecordCount", "majorTypeMax", "yeardaynum", "totalPrice", "ReportingMonth", "availabilityDate2", "availabilityDate3", "leaseStart", "leaseEnd", "leaseStartQuote", "vacateDate"
                    , "dateOfTheNotice")
              VALUES
                    ("inventorydatahashid", "ReportingDate", "ReportingDateOnly","inventoryId", "inventoryName", "propertyName", "multipleItemTotal", "description", "type", "floor", "layoutName", "inventoryGroupName", "buildingName"
                    , "parentInventory", "state", "stateStartDate", "externalId", "created_at", "updated_at", "basePriceMonthly", "amenities", "totalPrice_old", "isOnHold", "holdParty", "holdStart", "numDaysOnHold"
                    , "holdReason", "holdAgent", "availabilityDate", "amenityNames", "IGAmenityNames", "LayoutAmenityNames", "buildingAmenityNames", "propAmenityNames", "isCurrentlyOccupied", "isOccupiedOrReserved"
                    , "BedBath", "SQFT", "numBedrooms", "numBathrooms", "isBedsStudio", "isBedsOne", "isBedsTwo", "isBedsThree", "isBedsFour", "childInventoryNames", "childInventoryDescriptions", "inventoryInactive"
                    , "RecordCount", "majorTypeMax", "yeardaynum", "totalPrice", "ReportingMonth", "availabilityDate2", "availabilityDate3", "leaseStart", "leaseEnd", "leaseStartQuote", "vacateDate"
                    , "dateOfTheNotice")
            ;
             '
) }}

{{ config(
    post_hook='DROP TABLE IF EXISTS "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."InventoryDataMaximus"'
) }}

select '1' as dummy_col


