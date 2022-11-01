/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ActiveLeasePartyDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ActiveLeasePartyDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ActiveLeasePartyDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ActiveLeasePartyDump') }}

SELECT p.id AS "id"
    , prop.name AS "Property"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , p.id AS "partyIdNoURL"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP_NTZ AS "PartyCreatedDate"
    , ('1970-01-01 ' || ((CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "PartyCreateTime"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), p.endDate))::TIMESTAMP_NTZ AS "PartyClosedDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), p.endDate)::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "PartyClosedTime"
    , CASE
        WHEN p.endDate IS NULL
            THEN NULL
        ELSE CASE
                WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                    THEN 'MERGED_WITH_ANOTHER_PARTY'
                ELSE p.metadata: closeReasonId
                END
        END AS "closeReason"
    , p.STATE AS "currentState"
    , COALESCE(u.fullName, 'No Agent') AS "agentName"
    , s.displayName AS "source"
    , prog.displayName AS "campaign"
    , prog.reportingDisplayName AS "programReportingDisplayName"
    , prog.path AS "programPath"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), agentCon.contactDate)::TIMESTAMP_NTZ AS "firstCommDate"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.created_at))::TIMESTAMP_NTZ AS "tourCreateDate"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP_NTZ AS "tourStartDate"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourEndDate))::TIMESTAMP_NTZ AS "tourEndDate"
    , initialTour.tourResult AS "tourResult"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP_NTZ AS "signDate"
    , (('1970-01-01 ' || CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate)::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "signTime"
    , CASE
        WHEN p.endDate IS NOT NULL
            THEN CASE
                    WHEN p.metadata: closeReasonId = 'NO_MEMBERS'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                        THEN 'Ignore'
                    WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'MARKED_AS_SPAM'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'ALREADY_A_RESIDENT'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'BLOCKED_CONTACT'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'CLOSED_DURING_IMPORT'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'INITIAL_HANGUP'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'NOT_LEASING_BUSINESS'
                        THEN 'Ignore'
                    WHEN p.metadata: closeReasonId = 'REVA_TESTING'
                        THEN 'Ignore'
                    ELSE 'Include'
                    END
        ELSE 'Include'
        END AS "reportingStatus"
    , CASE p.metadata: firstContactChannel
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
    , CASE
        WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)) + interval '28 days')::TIMESTAMP
            THEN CASE
                    WHEN p.metadata: closeReasonId = 'CANT_AFFORD'
                        THEN 'Unqualified'
                    ELSE 'bronze'
                    END
        WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP <= date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)) + interval '28 days')::TIMESTAMP
            THEN CASE
                    WHEN (p.qualificationQuestions: moveInTime = 'NEXT_4_WEEKS' AND qualificationQuestions: cashAvailable = 'YES')
                        THEN 'gold'
                    ELSE 'silver'
                    END
        WHEN initialTour.TourStartDate IS NULL
            THEN CASE
                    WHEN (p.qualificationQuestions: moveInTime = 'NEXT_4_WEEKS' AND p.qualificationQuestions: cashAvailable = 'YES')
                        THEN 'silver'
                    WHEN p.metadata: closeReasonId = 'CANT_AFFORD'
                        THEN 'Unqualified'
                    WHEN (p.qualificationQuestions: moveInTime <> 'NEXT_4_WEEKS' AND COALESCE(NULLIF(p.qualificationQuestions: moveInTime, ''), 'I_DONT_KNOW') <> 'I_DONT_KNOW' AND p.qualificationQuestions: cashAvailable = 'YES')
                        THEN 'bronze'
                    ELSE 'prospect'
                    END
        WHEN p.metadata: closeReasonId = 'CANT_AFFORD'
            THEN 'Unqualified'
        ELSE 'prospect'
        END AS "finalLeadScore"
    , date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP_NTZ AS "createdMonth"
    , date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP_NTZ AS "signMonth"
    , completedTours.numTours AS "numTours"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP_NTZ AS "dumpGenDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "dumpGenTime"
    , DATEDIFF(DAY, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at))::TIMESTAMP, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate)::TIMESTAMP) + 1 AS "daysToClose"
    , DATEDIFF(DAY, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)), date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) AS "partyAge"
    , CASE
        WHEN datediff(DAY, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) > 20
            THEN 0.25
        WHEN datediff(DAY, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) > 15
            THEN 0.5
        WHEN datediff(DAY, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) > 10
            THEN 0.75
        WHEN datediff(DAY, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) <= 10
            THEN 1
        ELSE - 1
        END AS "forecastValue"
    , pm.externalProspectId AS "pCode"
    , pm.externalId AS "externalId"
    , '"' || peeps.NAMES || '"' AS "names"
    , CONVERT_TIMEZONE(coalesce(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP_NTZ AS "FCTTourDate"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP_NTZ AS "FCTCompletionDate"
    , COALESCE(FCT.unitCode, 'None Selected') AS "FCTunitCode"
    , COALESCE(FCT.inventoryGroupCode, 'None Selected') AS "FCTinventoryGroupCode"
    , REPLACE(p.qualificationQuestions: moveInTime, '', '') AS "QQMoveIn"
    , REPLACE(p.qualificationQuestions: cashAvailable, '', '') AS "QQBudget"
    , tCreator.fullName AS "taskCreatedBy"
    , origOwner.fullName AS "originalPartyOwner"
    , origAssignee."user(s)" AS "originalAssignees"
    , REPLACE(p.metadata: firstContactChannel, '"', '') AS "rawInitialChannel"
    , REPLACE(p.metadata: creationType, '"', '') AS "creationType"
    , p.qualificationQuestions: numBedrooms AS "QQNumBedrooms"
    , CASE
        WHEN (p.qualificationQuestions: groupProfile <> '')
            THEN p.qualificationQuestions: groupProfile
        ELSE 'NOT_YET_DETERMINED'
        END AS "QQGroupProfile"
    , prog.name AS "campaignName"
    , progProp.name AS "campaignProperty"
    , CASE STATE
        WHEN 'Contact'
            THEN 0
        WHEN 'Prospect'
            THEN 0
        WHEN 'Lead'
            THEN 0
        WHEN 'Applicant'
            THEN 1
        WHEN 'Lease'
            THEN 1
        WHEN 'FutureResident'
            THEN 1
        WHEN 'Resident'
            THEN 1
        ELSE 0
        END AS "hasApplied"
    , COALESCE(childCount.numChildren, 0) AS "numChildren"
    , CASE
        WHEN startedApp.partyId IS NULL
            THEN 0
        ELSE 1
        END AS "hasStartedApp"
    , COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), firstSubmittedApp.firstRequestDate), '1900-01-01 00:00:00'::TIMESTAMP)::TIMESTAMP_NTZ AS "firstAppSubmissionDate"
    , FCT.currentOwner AS "FCTAgent"
    , COALESCE(lease.Quote2LeaseDiff, 0) AS "Quote2LeaseDiff"
    , CASE
        WHEN p.qualificationQuestions: numBedrooms LIKE '%STUDIO%'
            THEN 1
        ELSE 0
        END AS "IsBedsStudio"
    , CASE
        WHEN p.qualificationQuestions: numBedrooms LIKE '%ONE%'
            THEN 1
        ELSE 0
        END AS "IsBedsOne"
    , CASE
        WHEN p.qualificationQuestions: numBedrooms LIKE '%TWO%'
            THEN 1
        ELSE 0
        END AS "IsBedsTwo"
    , CASE
        WHEN p.qualificationQuestions: numBedrooms LIKE '%THREE%'
            THEN 1
        ELSE 0
        END AS "IsBedsThree"
    , CASE
        WHEN p.qualificationQuestions: numBedrooms LIKE '%FOUR%'
            THEN 1
        ELSE 0
        END AS "IsBedsFour"
    , CASE
        WHEN COALESCE(p.qualificationQuestions: numBedrooms, '') = ''
            THEN 1
        WHEN REPLACE(REPLACE(p.qualificationQuestions: numBedrooms, '', ''), '[]', '') = ''
            THEN 1
        ELSE 0
        END AS "IsBedsNotAnswered"
    , origTeam.displayName AS originalTeam
    , COALESCE(firstCollab.fullName, '[None]') AS originalParticipant
    , CASE
        WHEN p.workflowName = 'renewal'
            THEN 1
        ELSE 0
        END AS "isRenewal"
    , 'prospect' AS "partyStage"
    , partyChannels.distinctChannelList AS "distinctChannelList"
    , COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), mostRecentComm.commDate), '1900-01-01 00:00:00'::TIMESTAMP)::TIMESTAMP_NTZ AS "mostRecentCommDate"
    , COALESCE(recordedCall.hasRecordedCall, 0) AS "hasRecordedCall"
    , CASE
        WHEN initialQuote.isSelfService = 1
            THEN CASE
                    WHEN date_trunc('minute', initialQuote.created_at) BETWEEN date_trunc('minute', COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at))
                            AND date_trunc('minute', COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)) + interval '1 minute'
                        THEN 1
                    ELSE 0
                    END
        ELSE 0
        END AS "CreatedViaSSQuote"
    , CASE
        WHEN p.isTransferLease = 'true'
            THEN 1
        ELSE 0
        END AS "isTransfer"
    , p.workflowName AS "workflowName"
    , p.workflowState AS "workflowState"
    , p.partyGroupId AS "partyGroupId"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'),archiveDate)::TIMESTAMP_NTZ AS "archiveDate"
    , seedPartyId AS "seedPartyId"
    , REPLACE(p.metadata: activatePaymentPlanDate, '"', '') AS "activatePaymentPlanDate"
    , COALESCE(p.mergedWith::TEXT, 'None') AS "mergedWith"
    , lease.leaseStartDate::TIMESTAMP_NTZ AS "leaseStartDate"
    , lease.leaseEndDate::TIMESTAMP_NTZ AS "leaseEndDate"
    , CASE
        WHEN REPLACE(p.metadata: activatePaymentPlanDate, '"', '') IS NULL
            THEN 0
        ELSE 1
        END AS "isPaymentPlanParticipant"
FROM {{ var("source_tenant") }}.Party AS p
INNER JOIN (
    SELECT p.id AS partyId
        , con.created_at contactDate
    FROM {{ var("source_tenant") }}.Party AS p
    LEFT OUTER JOIN (
        SELECT c.partyId
            , MIN(c.created_at) AS created_at
        FROM (
            SELECT fl.value AS partyId
                , c.*
            FROM {{ var("source_tenant") }}.Communication AS c
                , LATERAL flatten(input => parse_json(c.parties)) AS fl
            ) AS c
        WHERE c.type = 'ContactEvent' OR (c.type = 'Call' AND c.direction = 'out') OR (c.type = 'Call' AND c.direction = 'in' AND c.message: isMissed <> 'true' AND c.message: isVoiceMail <> 'true') OR (c.type = 'Email' AND c.direction = 'out') OR (c.type = 'Sms' AND c.direction = 'out') OR (c.type = 'Web' AND c.direction = 'out')
        GROUP BY c.partyId
        ) AS con ON con.partyId = p.id::TEXT
    ) AS agentCon ON agentCon.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = p.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN (
    SELECT *
    FROM (
        SELECT t1.partyId
            , t1.created_at
            , CAST(t1.metadata: startDate AS TIMESTAMPTZ) AS TourStartDate
            , CAST(t1.metadata: endDate AS TIMESTAMPTZ) AS TourEndDate
            , REPLACE(t1.metadata: appointmentResult, '"', '') AS tourResult
            , row_number() OVER (
                PARTITION BY t1.partyId ORDER BY t1.metadata: startDate
                    , t1.created_at
                ) AS rowNum
        FROM {{ var("source_tenant") }}.Tasks AS t1
        INNER JOIN (
            SELECT partyId
                , MIN(CAST(metadata: startDate AS TIMESTAMPTZ)) AS firstTour
            FROM {{ var("source_tenant") }}.Tasks
            WHERE name = 'APPOINTMENT'
            GROUP BY partyId
            ) AS firstTour ON firstTour.partyId = t1.partyId AND firstTour.firstTour = CAST(t1.metadata: startDate AS TIMESTAMPTZ)
        ) AS "rows"
    WHERE "rows".rowNum = 1
    ) AS initialTour ON initialTour.partyId = p.id
LEFT OUTER JOIN (
    SELECT partyId
        , count(*) AS numTours
    FROM {{ var("source_tenant") }}.Tasks
    WHERE name = 'APPOINTMENT' AND STATE = 'Completed' AND metadata: appointmentResult = 'COMPLETE'
    GROUP BY partyId
    ) AS completedTours ON completedTours.partyId = p.id
LEFT OUTER JOIN (
    SELECT *
    FROM (
        SELECT t1.partyId
            , t1.created_at
            , CAST(t1.metadata: startDate AS TIMESTAMPTZ) AS TourStartDate
            , CAST(t1.metadata: endDate AS TIMESTAMPTZ) AS TourEndDate
            , REPLACE(t1.metadata: appointmentResult, '"', '') AS tourResult
            , t1.completionDate
            , i.externalId AS unitCode
            , ig.name AS inventoryGroupCode
            , row_number() OVER (
                PARTITION BY t1.partyId ORDER BY t1.metadata: startDate
                    , t1.created_at
                ) AS rowNum
            , t1.metadata
            , t1.id AS taskId
            , tourOwner.fullName AS currentOwner
        FROM {{ var("source_tenant") }}.Tasks AS t1
        INNER JOIN (
            SELECT partyId
                , MIN(CAST(metadata: endDate AS TIMESTAMPTZ)) AS firstTourDate
            FROM {{ var("source_tenant") }}.Tasks
            WHERE name = 'APPOINTMENT' AND STATE = 'Completed' AND metadata: appointmentResult = 'COMPLETE'
            GROUP BY partyId
            ) AS firstCompletedTour ON firstCompletedTour.partyId = t1.partyId AND firstCompletedTour.firstTourDate = CAST(t1.metadata: endDate AS TIMESTAMPTZ)
        LEFT OUTER JOIN {{ var("source_tenant") }}.Inventory AS i ON i.id::TEXT = (t1.metadata: inventories [0])
        LEFT OUTER JOIN {{ var("source_tenant") }}.InventoryGroup AS ig ON ig.id = i.inventoryGroupId
        LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS tourOwner ON (tourOwner.id)::VARCHAR = t1.userIds
        WHERE t1.STATE = 'Completed' AND t1.metadata: appointmentResult = 'COMPLETE'
        ) AS "rows"
    WHERE "rows".rowNum = 1
    ) AS FCT ON FCT.partyId = p.id
LEFT OUTER JOIN (
    SELECT l.partyId
        , l.signDate
        , COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0) - COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0) AS Quote2LeaseDiff
        , REPLACE(l.baselineData: publishedLease: leaseStartDate, '"', '')::TIMESTAMP AS leaseStartDate
        , REPLACE(l.baselineData: publishedLease: leaseEndDate, '"', '')::TIMESTAMP AS leaseEndDate
    FROM {{ var("source_tenant") }}.Lease AS l
    INNER JOIN (
        SELECT q.id
            , fl.value AS leaseTerms
            , publishedQuoteData: leaseTerms [fl.index] :termLength AS termLength
        FROM {{ var("source_tenant") }}.QUOTE AS q
            , LATERAL flatten(input => publishedQuoteData: leaseTerms) AS fl
        ) q ON q.id = l.quoteId
    INNER JOIN (
        SELECT *
        FROM (
            SELECT rank() OVER (
                    PARTITION BY quoteId ORDER BY created_at DESC
                    ) AS theRank
                , *
            FROM {{ var("source_tenant") }}.PartyQuotePromotions
            ) mostRecent
        WHERE mostRecent.theRank = 1
        ) AS pq ON pq.quoteId = q.id
    INNER JOIN {{ var("source_tenant") }}.LeaseTerm AS lt ON lt.id = pq.leaseTermId AND lt.termLength = q.termLength
    WHERE l.STATUS = 'executed'
    ) AS lease ON lease.partyId = p.id
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
LEFT OUTER JOIN (
    SELECT pm1.partyId
        , LISTAGG(per.fullName, ' | ') WITHIN
    GROUP (
            ORDER BY per.fullName
            ) AS NAMES
    FROM {{ var("source_tenant") }}.PartyMember AS pm1
    INNER JOIN {{ var("source_tenant") }}.Person AS per ON per.id = pm1.personId
    GROUP BY pm1.partyId
    ) AS peeps ON peeps.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS tCreator ON tCreator.id::TEXT = (FCT.metadata: createdBy)
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS origOwner ON origOwner.id::TEXT = (FCT.metadata: originalPartyOwner)
LEFT OUTER JOIN (
    SELECT LISTAGG(u1.FULLNAME, ' | ') AS "user(s)"
        , t1.taskId
    FROM (
        SELECT id AS taskId
            , TRANSLATE(fl.value, '[] "', '') AS userid
        FROM {{ var("source_tenant") }}.TASKS
            , LATERAL flatten(input => parse_json(ARRAY_CONSTRUCT(metadata: originalAssignees)::VARIANT)) AS fl
        ) AS t1
    INNER JOIN {{ var("source_tenant") }}.USERS u1 ON u1.id::VARCHAR = t1.userId
    GROUP BY t1.taskId
    ) AS origAssignee ON origAssignee.taskId = FCT.taskId
LEFT OUTER JOIN {{ var("source_tenant") }}.TeamPropertyProgram AS tpp ON tpp.id = p.teamPropertyProgramId
LEFT OUTER JOIN {{ var("source_tenant") }}.Programs AS prog ON prog.id = tpp.programId
LEFT OUTER JOIN (
    SELECT id
        , displayName
        , CASE
            WHEN displayName = 'parkmerced.com'
                THEN 'Website'
            WHEN displayName = 'serenityatlarkspur.com'
                THEN 'Website'
            WHEN displayName = 'sharongreenapts.com'
                THEN 'Website'
            WHEN displayName = 'thecoveattiburon.com'
                THEN 'Website'
            WHEN displayName = 'woodchaseapartments.com'
                THEN 'Website'
            WHEN displayName = 'Parkmerced website'
                THEN 'Website'
            WHEN displayName = 'The Cove website'
                THEN 'Website'
            WHEN displayName = 'Serenity at Larkspur website'
                THEN 'Website'
            WHEN displayName = 'Sharon Green website'
                THEN 'Website'
            WHEN displayName = 'Woodchase Apartments website'
                THEN 'Website'
            WHEN displayName = 'apartmentlist.com'
                THEN 'ApartmentList.com'
            ELSE displayName
            END AS displayNameWebsite
    FROM {{ var("source_tenant") }}.Sources
    ) AS s ON s.id = prog.sourceId
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS progProp ON progProp.id = tpp.propertyId
LEFT OUTER JOIN (
    SELECT partyId
        , count(*) AS numChildren
    FROM {{ var("source_tenant") }}.Party_AdditionalInfo
    WHERE type = 'child'
    GROUP BY partyId
    ) AS childCount ON childCount.partyId = p.id
LEFT OUTER JOIN (
    SELECT DISTINCT partyId
    FROM {{ var("source_tenant") }}.rentapp_PersonApplication
    WHERE applicationStatus <> 'not_sent'
    ) AS startedApp ON startedApp.partyId = p.id
LEFT OUTER JOIN (
    SELECT partyId
        , min(req.created_at) AS firstRequestDate
    FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest req
    INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication pa ON pa.id = req.partyApplicationId
    GROUP BY pa.partyId
    ) AS firstSubmittedApp ON firstSubmittedApp.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Teams AS origTeam ON origTeam.id::TEXT = p.metadata: originalTeam
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS firstCollab ON firstCollab.id::TEXT = p.metadata: firstCollaborator
LEFT OUTER JOIN (
    SELECT c00.partyId
        , c00.distinctChannelList0
        , CASE
            WHEN c00.distinctChannelList0 = 'Call | Manual'
                THEN 'Call'
            WHEN c00.distinctChannelList0 = 'Call | Web'
                THEN 'Call'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Web'
                THEN 'Call'
            WHEN c00.distinctChannelList0 = 'Manual | Email'
                THEN 'Email'
            WHEN c00.distinctChannelList0 = 'Email | Web'
                THEN 'Email'
            WHEN c00.distinctChannelList0 = 'Manual | Email | Web'
                THEN 'Email'
            WHEN c00.distinctChannelList0 = 'Manual | Sms'
                THEN 'Sms'
            WHEN c00.distinctChannelList0 = 'Sms | Web'
                THEN 'Sms'
            WHEN c00.distinctChannelList0 = 'Manual | Sms | Web'
                THEN 'Sms'
            WHEN c00.distinctChannelList0 = 'Manual'
                THEN 'Other'
            WHEN c00.distinctChannelList0 = 'Manual | Web'
                THEN 'Other'
            WHEN c00.distinctChannelList0 = 'Web'
                THEN 'Other'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Email'
                THEN 'Call | Email'
            WHEN c00.distinctChannelList0 = 'Call | Email | Web'
                THEN 'Call | Email'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Email | Web'
                THEN 'Call | Email'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Sms'
                THEN 'Call | Sms'
            WHEN c00.distinctChannelList0 = 'Call | Sms | Web'
                THEN 'Call | Sms'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Sms | Web'
                THEN 'Call | Sms'
            WHEN c00.distinctChannelList0 = 'Manual | Email | Sms'
                THEN 'Email | Sms'
            WHEN c00.distinctChannelList0 = 'Manual | Email | Sms | Web'
                THEN 'Email | Sms'
            WHEN c00.distinctChannelList0 = 'Email | Sms | Web'
                THEN 'Email | Sms'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Email | Sms'
                THEN 'Call | Email | Sms'
            WHEN c00.distinctChannelList0 = 'Call | Email | Sms | Web'
                THEN 'Call | Email | Sms'
            WHEN c00.distinctChannelList0 = 'Call | Manual | Email | Sms | Web'
                THEN 'Call | Email | Sms'
            ELSE c00.distinctChannelList0
            END AS distinctChannelList
    FROM (
        SELECT c0.partyId::VARCHAR AS partyId
            , LISTAGG(replace(c0.type, 'ContactEvent', 'Manual'), ' | ') WITHIN
        GROUP (
                ORDER BY c0.TYPE
                ) AS distinctChannelList0
        FROM (
            SELECT DISTINCT fl.value::VARCHAR AS partyId
                , type
            FROM {{ var("source_tenant") }}.COMMUNICATION
                , LATERAL flatten(input => parse_json(parties)) AS fl
            ) c0
        GROUP BY c0.partyId
        ) c00
    ) AS partyChannels ON partyChannels.partyId = p.id::TEXT
LEFT OUTER JOIN (
    SELECT comms.partyId::VARCHAR AS partyId
        , MAX(comms.created_at) AS commDate
    FROM (
        SELECT fl.value::VARCHAR AS partyId
            , created_at
        FROM {{ var("source_tenant") }}.COMMUNICATION
            , LATERAL flatten(input => parse_json(parties)) AS fl
        WHERE direction = 'in'
        ) AS comms
    GROUP BY comms.partyId
    ) AS mostRecentComm ON mostRecentComm.partyId = p.id::TEXT
LEFT OUTER JOIN (
    SELECT DISTINCT fl.value::VARCHAR AS partyId
        , 1 AS hasRecordedCall
    FROM {{ var("source_tenant") }}.COMMUNICATION
        , LATERAL flatten(input => parse_json(parties)) AS fl
    WHERE direction = 'in' AND type = 'Call' AND message: isRecorded = 'true'
    ) AS recordedCall ON recordedCall.partyId = p.id::TEXT
LEFT OUTER JOIN (
    SELECT rank() OVER (
            PARTITION BY partyId ORDER BY created_at
            ) AS theRank
        , created_at
        , CASE
            WHEN createdFromCommId IS NULL
                THEN 0
            ELSE 1
            END AS isSelfService
        , partyId
    FROM {{ var("source_tenant") }}.Quote
    ) AS initialQuote ON initialQuote.partyId = p.id AND initialQuote.theRank = 1
WHERE p.workflowName IN ('activeLease')
