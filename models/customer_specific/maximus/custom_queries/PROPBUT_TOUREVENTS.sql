/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.PROPBUT_TOUREVENTS --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='PROPBUT_TOUREVENTS') }}

SELECT 'OneToManys' AS "Table_Name"
    , '' AS "Entity_Record_Code"
    , '' AS "Field_Name1"
    , '' AS "Field_Value1"
    , '' AS "Field_Name2"
    , '' AS "Field_Value2"
    , '' AS "Field_Name3"
    , '' AS "Field_Value3"
    , '' AS "Field_Name4"
    , '' AS "Field_Value4"
    , '' AS "Field_Name5"
    , '' AS "Field_Value5"
    , '' AS "Field_Name6"
    , '' AS "Field_Value6"
    , '' AS "Field_Name7"
    , '' AS "Field_Value7"
    , '' AS "Field_Name8"
    , '' AS "Field_Value8"

UNION ALL

SELECT 'Table_Name' AS Table_Name
    , 'Entity_Record_Code' AS Entity_Record_Code
    , 'Field_Name1' AS Field_Name1
    , 'Field_Value1' AS Field_Value1
    , /*pCode*/
    'Field_Name2' AS Field_Name2
    , 'Field_Value2' AS Field_Value2
    , /*UnitCode*/
    'Field_Name3' AS Field_Name3
    , 'Field_Value3' AS Field_Value3
    , /*UnitType*/
    'Field_Name4' AS Field_Name4
    , 'Field_Value4' AS Field_Value4
    , /*dtDate*/
    'Field_Name5' AS Field_Name5
    , 'Field_Value5' AS Field_Value5
    , /*sFirstName*/
    'Field_Name6' AS Field_Name6
    , 'Field_Value6' AS Field_Value6
    , /*sLastName*/
    'Field_Name7' AS Field_Name7
    , 'Field_Value7' AS Field_Value7
    , /*Agent*/
    'Field_Name8' AS Field_Name8
    , 'Field_Value8' AS Field_Value8 /*sType*/

UNION ALL

SELECT 'PROPBUT_TOUREVENTS' AS Table_Name
    , prop.NAME AS Entity_Record_Code
    , 'pCode' AS Field_Name1
    , '"' || pm.externalProspectId || '"' AS Field_Value1
    , 'UnitCode' AS Field_Name2
    , '"' || i.externalId || '"' AS Field_Value2
    , 'UnitType' AS Field_Name3
    , ig.externalId AS Field_Value3
    , 'dtDate' AS Field_Name4
    , '"' || date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}', tour.eventDate)::TIMESTAMP)::VARCHAR || '"' AS Field_Value4
    , 'sFirstName' AS Field_Name5
    , '"' || regexp_replace(per.fullName, ' [^ ]*$', '') || '"' AS Field_Value5
    , 'sLastName' AS Field_Name6
    , '"' || regexp_replace(per.fullName, '^.* ', '') || '"' AS Field_Value6
    , 'Agent' AS Field_Name7
    , '"' || u.fullName || '"' AS Field_Value7
    , 'sType' AS Field_Name8
    , 'Show' AS Field_Value8
FROM {{ var("source_tenant") }}.Party AS p
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
INNER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = p.userId
INNER JOIN (
    SELECT partyId
        , CAST(metadata: endDate AS TIMESTAMPTZ) AS eventDate
        , completionDate
        , (metadata: inventories [0])::VARCHAR AS inventoryId
        , row_number() OVER (
            PARTITION BY partyId ORDER BY metadata: startDate
                , created_at
            ) AS rowNum
    FROM {{ var("source_tenant") }}.Tasks
    WHERE name = 'APPOINTMENT' AND STATE = 'Completed' AND metadata: appointmentResult = 'COMPLETE'
    ) AS tour ON tour.partyId = p.id AND tour.rowNum = 1
LEFT OUTER JOIN (
    SELECT *
    FROM (
        SELECT ei.externalId
            , ei.externalProspectId
            , ei.partyId
            , pm.personId
            , row_number() OVER (
                PARTITION BY ei.partyId ORDER BY COALESCE(ei.endDate, '2200-01-01') DESC
                    , ei.startDate DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.ExternalPartyMemberInfo AS ei
        INNER JOIN {{ var("source_tenant") }}.PartyMember AS pm ON pm.id = ei.partyMemberId
        WHERE ei.isPrimary = 'true'
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) AS pm ON pm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Person AS per ON per.id = pm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.Inventory AS i ON i.id = tour.inventoryId
LEFT OUTER JOIN {{ var("source_tenant") }}.InventoryGroup AS ig ON ig.id = i.inventoryGroupId
WHERE date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}', tour.completionDate))::TIMESTAMP = CONVERT_TIMEZONE('{{ var("timezone") }}', CURRENT_DATE ())::TIMESTAMP
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'MARKED_AS_SPAM')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'MERGED_WITH_ANOTHER_PARTY')
        AND (p.metadata:archiveReasonId IS NULL OR p.metadata:archiveReasonId != 'MERGED_WITH_ANOTHER_PARTY')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'ALREADY_A_RESIDENT')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'NO_MEMBERS')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'NOT_FOR_LEASING')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'INITIAL_HANGUP')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'REVA_TESTING')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'NOT_LEASING_BUSINESS')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'CLOSED_DURING_IMPORT')
        AND (REPLACE(p.metadata:closeReasonId,'"','') IS NULL OR REPLACE(p.metadata:closeReasonId,'"','') != 'BLOCKED_CONTACT')
