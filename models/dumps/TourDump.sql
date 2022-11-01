/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.TourDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.TourDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.TourDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='TourDump') }}

SELECT date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP AS "dumpGenDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIME)::VARCHAR)::TIMESTAMP AS "dumpGenTime"
    , t.id AS "taskId"
    , prop.name AS "Property"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::VARCHAR AS "partyId"
    , prop.name AS "partyProperty"
    , COALESCE(s.displayName, p.metadata: source) AS "source"
    , COALESCE(prog.displayName, p.metadata: campaignId) AS "campaign"
    , prog.reportingDisplayName AS "programReportingDisplayName"
    , prog.path AS "programPath"
    , CASE p.metadata: "firstContactChannel"
        WHEN 'ContactEvent'
            THEN 'Walk-In'
        WHEN 'Sms'
            THEN 'Phone'
        WHEN 'Email'
            THEN 'Digital'
        WHEN 'Call'
            THEN 'Phone'
        WHEN 'Web'
            THEN 'Digital'
        WHEN 'Walk-in'
            THEN 'Walk-In'
        ELSE 'Digital'
        END AS "initialChannel"
    , REPLACE(t.metadata: appointmentResult, '"', '') AS "tourResult"
    , CASE
        WHEN firstCon.type = 'ContactEvent'
            THEN 'gold'
        WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at))::TIMESTAMP <= date_trunc('day', dateadd(day, 2, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), agentCon.contactDate)))::TIMESTAMP
            THEN CASE
                    WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.metadata: startDate::TIMESTAMP))::TIMESTAMP < date_trunc('day', dateadd(day, 14, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), agentCon.contactDate)))::TIMESTAMP
                        THEN 'gold'
                    ELSE 'silver'
                    END
        WHEN p.endDate::TIMESTAMP IS NOT NULL
            THEN CASE
                    WHEN p.metadata: closeReasonId = 'CANT_AFFORD'
                        THEN 'Unqualified'
                    WHEN agentCon.contactDate::TIMESTAMP IS NOT NULL
                        THEN 'bronze'
                    ELSE 'prospect'
                    END
        WHEN agentCon.contactDate::TIMESTAMP IS NOT NULL
            THEN 'bronze'
        ELSE 'prospect'
        END AS "finalLeadScore"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at))::TIMESTAMP_NTZ AS "tourCreateDate"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.metadata: startDate::TIMESTAMP)::TIMESTAMP_NTZ AS "tourStartDate"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.metadata: endDate::TIMESTAMP)::TIMESTAMP_NTZ AS "tourEndDate"
    , date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.metadata: endDate::TIMESTAMP))::TIMESTAMP_NTZ AS "tourEndMonth"
    , CASE
        WHEN p.endDate IS NULL
            THEN NULL
        ELSE CASE
                WHEN REPLACE(p.metadata: archiveReasonId, '', '') = 'MERGED_WITH_ANOTHER_PARTY'
                    THEN 'MERGED_WITH_ANOTHER_PARTY'
                ELSE REPLACE(p.metadata: closeReasonId, '', '')
                END
        END AS "closeReason"
    , i.externalId AS "unitCode"
    , ig.name AS "inventoryGroupCode"
    , tCreator.fullName AS "taskCreatedBy"
    , origOwner.fullName AS "originalPartyOwner"
    , origAssignee."user(s)" AS "originalAssignees"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate)::TIMESTAMP_NTZ AS "completionDate"
    , pm.externalProspectId AS "pCode"
    , peeps.NAMES AS "names"
    , po.fullName AS "partyOwner"
    , tu.users AS "taskOwners"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'),l.signDate)::TIMESTAMP_NTZ AS "signDate"
    , REPLACE(p.metadata: firstContactChannel, '"', '') AS "rawInitialChannel"
    , REPLACE(t.metadata: note, '"', '') AS "tourNotes"
    , COALESCE(tourProp.properties, prop.name) AS "tourProperties"
    , CASE
        WHEN COALESCE(t.metadata: rescheduled, 'false') = 'true'
            THEN 1
        ELSE 0
        END AS "isRescheduled"
    , COALESCE(t.metadata: appointmentCreatedFrom, 'REVA') AS "tourCreatedFrom"
    , COALESCE(t.metadata: closingNote, '') AS "closingNotes"
    , CASE
        WHEN t.metadata::VARCHAR LIKE '%SELF_SERVICE%'
            THEN 1
        ELSE 0
        END AS "isSelfService"
    , COALESCE(t.metadata: tourType, '[No Tour Type Selected]') AS "tourType"
    , CASE
        WHEN (REPLACE(t.metadata: appointmentResult, '"', '')) IS NULL
            THEN 'Scheduled'
        ELSE (REPLACE(t.metadata: appointmentResult, '"', ''))
        END AS "tourResultNN"
    , CASE
        WHEN (
                CASE
                    WHEN t.metadata::VARCHAR LIKE '%SELF_SERVICE%'
                        THEN 1
                    ELSE 0
                    END
                ) = 0
            THEN 'Agent Added'
        WHEN (
                CASE
                    WHEN t.metadata::VARCHAR LIKE '%SELF_SERVICE%'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 'Self-Service'
        END AS "isSelfServiceNN"
    , CASE
        WHEN charindex('virtual', Lower(REPLACE(t.metadata: note, '"', ''))) > 0
            THEN 1
        WHEN charindex('virtual', Lower(COALESCE(t.metadata: closingNote, ''))) > 0
            THEN 1
        WHEN COALESCE(t.metadata: tourType, '[No Tour Type Selected]') = 'virtualTour'
            THEN 1
        ELSE 0
        END AS "isVirtual"
    , CASE
        WHEN (
                (
                    CASE
                        WHEN charindex('virtual', Lower(REPLACE(t.metadata: note, '"', ''))) > 0
                            THEN 1
                        WHEN charindex('virtual', Lower(COALESCE(t.metadata: closingNote, ''))) > 0
                            THEN 1
                        WHEN COALESCE(t.metadata: tourType, '[No Tour Type Selected]') = 'virtualTour'
                            THEN 1
                        ELSE 0
                        END
                    ) = 1 AND (
                    (
                        CASE
                            WHEN (REPLACE(t.metadata: appointmentResult, '"', '')) IS NULL
                                THEN 'Scheduled'
                            ELSE (REPLACE(t.metadata: appointmentResult, '"', ''))
                            END
                        ) = 'COMPLETE'
                    )
                )
            THEN 1
        ELSE 0
        END AS "IsVirtualCompleted"
    , CASE
        WHEN (
                CASE
                    WHEN charindex('virtual', Lower(REPLACE(t.metadata: note, '"', ''))) > 0
                        THEN 1
                    WHEN charindex('virtual', Lower(COALESCE(t.metadata: closingNote, ''))) > 0
                        THEN 1
                    WHEN COALESCE(t.metadata: tourType, '[No Tour Type Selected]') = 'virtualTour'
                        THEN 1
                    ELSE 0
                    END
                ) = 0 AND (
                (
                    CASE
                        WHEN (REPLACE(t.metadata: appointmentResult, '"', '')) IS NULL
                            THEN 'Scheduled'
                        ELSE (REPLACE(t.metadata: appointmentResult, '"', ''))
                        END
                    ) = 'COMPLETE'
                )
            THEN 1
        WHEN (
                CASE
                    WHEN charindex('virtual', Lower(REPLACE(t.metadata: note, '"', ''))) > 0
                        THEN 1
                    WHEN charindex('virtual', Lower(COALESCE(t.metadata: closingNote, ''))) > 0
                        THEN 1
                    WHEN COALESCE(t.metadata: tourType, '[No Tour Type Selected]') = 'virtualTour'
                        THEN 1
                    ELSE 0
                    END
                ) = 0 AND (
                (
                    CASE
                        WHEN (REPLACE(t.metadata: appointmentResult, '"', '')) IS NULL
                            THEN 'Scheduled'
                        ELSE (REPLACE(t.metadata: appointmentResult, '"', ''))
                        END
                    ) = 'COMPLETE'
                )
            THEN 1
        ELSE 0
        END AS "isNotVirtualCompleted"
    , 1 AS "recordCount"
    , CASE
        WHEN lt.properties IS NULL
            THEN prop.NAME
        ELSE lt.properties
        END AS "Lookup_TourProperties"
    , CASE
        WHEN l.signDate IS NULL
            THEN 0
        ELSE 1
        END AS "hasSigned"
FROM {{ var("source_tenant") }}.TASKS AS t
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = t.partyId
INNER JOIN (
    SELECT p.id AS partyId
        , con.created_at contactDate
    FROM {{ var("source_tenant") }}.PARTY AS p
    LEFT OUTER JOIN (
        SELECT c.partyId
            , MIN(c.created_at) AS created_at
        FROM (
            SELECT fl.value::VARCHAR AS partyId
                , *
            FROM {{ var("source_tenant") }}.COMMUNICATION c
                , LATERAL flatten(input => parse_json(c.parties)) AS fl
            ) AS c
        WHERE c.type = 'ContactEvent' OR (c.type = 'Call' AND c.direction = 'out') OR (c.type = 'Call' AND c.direction = 'in' AND c.message: isMissed <> 'true' AND c.message: isVoiceMail <> 'true') OR (c.type = 'Email' AND c.direction = 'out') OR (c.type = 'Sms' AND c.direction = 'out') OR (c.type = 'Web' AND c.direction = 'out')
        GROUP BY c.partyId
        ) AS con ON con.partyId = p.id::TEXT
    ) AS agentCon ON agentCon.partyId = p.id
INNER JOIN {{ var("source_tenant") }}.USERS AS po ON po.id = p.userId
INNER JOIN (
    SELECT t1.id AS taskId
        , LISTAGG(u1.fullName, ', ') WITHIN
    GROUP (
            ORDER BY u1.fullName
            ) AS users
    FROM {{ var("source_tenant") }}.USERS AS u1
    INNER JOIN (
        SELECT fl.value::VARCHAR AS userId
            , *
        FROM {{ var("source_tenant") }}.TASKS t
            , LATERAL flatten(input => parse_json(T.userIds)) AS fl
        ) AS t1 ON t1.userId = u1.id::TEXT
    WHERE t1.name = 'APPOINTMENT'
    GROUP BY t1.id
    ) AS tu ON tu.taskId = t.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN (
    SELECT c.type AS firstConType
        , *
    FROM (
        SELECT fl.value::VARCHAR AS partyId
            , *
        FROM {{ var("source_tenant") }}.COMMUNICATION c
            , LATERAL flatten(input => parse_json(c.parties)) AS fl
        ) AS c
    INNER JOIN (
        SELECT fl.value::VARCHAR AS Party
            , min(created_at) AS dtCreated
        FROM {{ var("source_tenant") }}.COMMUNICATION c
            , LATERAL flatten(input => parse_json(c.parties)) AS fl
        GROUP BY fl.value::VARCHAR
        ) AS minCom ON minCom.Party = c.partyId AND minCom.dtCreated = c.created_at
    ) AS firstCon ON firstCon.partyId = p.id::TEXT
LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id::TEXT = REPLACE(t.metadata: inventories [0], '"', '')
LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORYGROUP AS ig ON ig.id = i.inventoryGroupId
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
        FROM {{ var("source_tenant") }}.EXTERNALPARTYMEMBERINFO AS ei
        INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER AS pm ON pm.id = ei.partyMemberId
        WHERE ei.isPrimary = 'true'
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) AS pm ON pm.partyId = p.id
LEFT OUTER JOIN (
    SELECT pm1.partyId
        , LISTAGG(per.fullName, ', ') WITHIN
    GROUP (
            ORDER BY per.fullName
            ) AS NAMES
    FROM {{ var("source_tenant") }}.PARTYMEMBER AS pm1
    INNER JOIN {{ var("source_tenant") }}.PERSON AS per ON per.id = pm1.personId
    GROUP BY pm1.partyId
    ) AS peeps ON peeps.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS tCreator ON tCreator.id::VARCHAR = t.metadata: createdBy
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS origOwner ON origOwner.id::VARCHAR = t.metadata: originalPartyOwner
LEFT OUTER JOIN (
    SELECT LISTAGG(u1.fullName, ' | ') WITHIN
    GROUP (
            ORDER BY u1.fullName
            ) AS "user(s)"
        , t1.id AS taskId
    FROM (
        SELECT translate(fl.value, '[] "', '') AS userId
            , id
        FROM {{ var("source_tenant") }}.TASKS
            , LATERAL flatten(input => parse_json(ARRAY_CONSTRUCT(metadata: originalAssignees)::VARIANT)) AS fl
        ) AS t1
    INNER JOIN {{ var("source_tenant") }}.USERS AS u1 ON u1.id::TEXT = t1.userId
    GROUP BY t1.id
    ) AS origAssignee ON origAssignee.taskId = t.id
LEFT OUTER JOIN {{ var("source_tenant") }}.LEASE AS l ON l.partyId = p.id AND l.STATUS = 'executed'
LEFT OUTER JOIN {{ var("source_tenant") }}.TEAMPROPERTYPROGRAM AS tpp ON tpp.id = p.teamPropertyProgramId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROGRAMS AS prog ON prog.id = tpp.programId
LEFT OUTER JOIN {{ var("source_tenant") }}.SOURCES AS s ON s.id = prog.sourceId
LEFT OUTER JOIN (
    SELECT t.id AS taskId
        , CASE
            WHEN COALESCE(tourProp0.name, LISTAGG(DISTINCT unitProp.name, ' | ') WITHIN GROUP (
                        ORDER BY unitProp.name
                        )) = ''
                THEN NULL
            ELSE COALESCE(tourProp0.name, LISTAGG(DISTINCT unitProp.name, ' | ') WITHIN GROUP (
                        ORDER BY unitProp.name
                        ))
            END AS properties
    FROM {{ var("source_tenant") }}.TASKS AS t
    LEFT OUTER JOIN (
        SELECT t.id
            , fl.value::VARCHAR AS inventoryId
        FROM {{ var("source_tenant") }}.TASKS AS t
            , LATERAL flatten(input => parse_json(t.metadata: inventories)) AS fl
        ) AS taskInv ON taskInv.id = t.id
    LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id::TEXT = taskInv.inventoryId
    LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS unitProp ON unitProp.id = i.propertyId
    LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS tourProp0 ON tourProp0.id::TEXT = (t.metadata: SELECTedPropertyId)
    WHERE t.name = 'APPOINTMENT'
    GROUP BY t.id
        , tourProp0.NAME
    ) AS tourProp ON tourProp.taskId = t.id
LEFT OUTER JOIN (
    SELECT t.id AS taskId
        , COALESCE(tourProp0.name, listagg(DISTINCT unitProp.name, ' | ') WITHIN GROUP (
                ORDER BY unitProp.name
                )) AS properties
    FROM {{ var("source_tenant") }}.TASKS AS t
    LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id::TEXT = replace(replace(replace(t.metadata: inventories, '[', '{'), ']', '}'), '"', '')::TEXT
    LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS unitProp ON unitProp.id = i.PROPERTYID
    LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS tourProp0 ON tourProp0.id::TEXT = replace(t.metadata: selectedPropertyId, '"', '')
    WHERE t.name = 'APPOINTMENT'
    GROUP BY t.id
        , tourProp0.name
    ) AS lt ON lt.taskId = t.id
WHERE t.name = 'APPOINTMENT' AND p.workflowName IN ('newLease', 'renewal')
