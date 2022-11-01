/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.PartyDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.PartyDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.PartyDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='PartyDump') }}

-- depends on: {{ ref('MostRecentReqResp') }}

WITH FirstCompletedTour
AS (
    SELECT t1.partyId
        , t1.TourEndDate
        , t1.completionDate
        , i.externalId AS unitCode
        , ig.name AS inventoryGroupCode
        , tourOwner.fullName AS currentOwner
        , t1.id AS taskId
        , t1.createdBy
        , t1.originalPartyOwner
        , t1.originalAssignees
        , t1.tourType
        , t1.isSelfService
    FROM (
        SELECT partyId
            , CAST(metadata: endDate AS TIMESTAMPTZ) AS TourEndDate
            , CAST(metadata: startDate AS TIMESTAMPTZ) AS TourStartDate
            , (metadata: inventories [0])::VARCHAR AS inventoryId
            , (metadata: createdBy)::VARCHAR AS createdBy
            , (metadata: originalPartyOwner)::VARCHAR AS originalPartyOwner
            , metadata: originalAssignees AS originalAssignees
            , completionDate
            , userIds
            , id
            , (metadata: tourType)::VARCHAR AS tourType
            , CASE WHEN metadata::VARCHAR like '%SELF_SERVICE%' THEN 1 ELSE 0 END as isSelfService
        FROM {{ var("source_tenant") }}.TASKS
        WHERE name = 'APPOINTMENT' AND STATE = 'Completed' AND REPLACE(metadata: appointmentResult, '"', '') = 'COMPLETE' QUALIFY ROW_NUMBER() OVER (
                PARTITION BY partyId ORDER BY partyId ASC
                    , 2 ASC
                    , 3 ASC
                    , created_at ASC
                ) = 1
        ) t1
    LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id = t1.inventoryId
    LEFT OUTER JOIN {{ var("source_tenant") }}.INVENTORYGROUP AS ig ON ig.id = i.inventoryGroupId
    LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS tourOwner ON ARRAY_CONTAINS(tourOwner.id::VARIANT, t1.userIds)
    )
SELECT prop.name AS "Property"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::VARCHAR AS "partyId"
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
    , REPLACE(initialTour.tourResult, '"', '') AS "tourResult"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP_NTZ AS "signDate"
    , (('1970-01-01 ' || CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate)::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "signTime"
    , CASE
        WHEN (p.endDate) IS NOT NULL
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
    , CASE
        WHEN LOWER('{{ var("client") }}') = 'customernew'
            THEN 'NA'
        ELSE pm.externalProspectId
        END AS "pCode"
    , pm.externalId AS "externalId"
    , CASE
        WHEN peeps.NAMES IS NOT NULL
            THEN '"' || peeps.NAMES || '"'
        ELSE NULL
        END AS "NAMES"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP_NTZ AS "FCTTourDate"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP_NTZ AS "FCTCompletionDate"
    , COALESCE(FCT.unitCode, 'None Selected') AS "FCTunitCode"
    , COALESCE(FCT.inventoryGroupCode, 'None Selected') AS "FCTinventoryGroupCode"
    , REPLACE(p.qualificationQuestions: moveInTime, '"', '') AS "QQMoveIn"
    , REPLACE(p.qualificationQuestions: cashAvailable, '"', '') AS "QQBudget"
    , tCreator.fullName AS "taskCreatedBy"
    , origOwner.fullName AS "originalPartyOwner"
    , origAssignee.users AS "originalAssignees"
    , REPLACE(p.metadata: firstContactChannel, '"', '') AS "rawInitialChannel"
    , REPLACE(p.metadata: creationType, '"', '') AS "creationType"
    , REPLACE(p.qualificationQuestions: numBedrooms, '\n', '') AS "QQNumBedrooms"
    , CASE
        WHEN (p.qualificationQuestions: groupProfile <> '')
            THEN p.qualificationQuestions: groupProfile
        ELSE 'NOT_YET_DETERMINED'
        END "QQGroupProfile"
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
        WHEN qualificationQuestions: numBedrooms LIKE '%STUDIO%'
            THEN 1
        ELSE 0
        END AS "IsBedsStudio"
    , CASE
        WHEN qualificationQuestions: numBedrooms LIKE '%ONE%'
            THEN 1
        ELSE 0
        END AS "IsBedsOne"
    , CASE
        WHEN qualificationQuestions: numBedrooms LIKE '%TWO%'
            THEN 1
        ELSE 0
        END AS "IsBedsTwo"
    , CASE
        WHEN qualificationQuestions: numBedrooms LIKE '%THREE%'
            THEN 1
        ELSE 0
        END AS "IsBedsThree"
    , CASE
        WHEN qualificationQuestions: numBedrooms LIKE '%FOUR%'
            THEN 1
        ELSE 0
        END AS "IsBedsFour"
    , CASE
        WHEN COALESCE(qualificationQuestions: numBedrooms, '') = ''
            THEN 1
        WHEN REPLACE(REPLACE(qualificationQuestions: numBedrooms, '"', ''), '[]', '') = ''
            THEN 1
        ELSE 0
        END AS "IsBedsNotAnswered"
    , origTeam.displayName AS "originalTeam"
    , COALESCE(firstCollab.fullName, '[None]') AS "originalParticipant"
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
    , CONVERT_TIMEZONE('{{ var("timezone") }}',archiveDate)::TIMESTAMP_NTZ AS "archiveDate"
    , seedPartyId AS "seedPartyId"
    , p.metadata: activatePaymentPlanDate AS "activatePaymentPlanDate"
    , COALESCE(p.mergedWith::VARCHAR, 'None') AS "mergedWith"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',lease.leaseStartDate)::TIMESTAMP_NTZ AS "leaseStartDate"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',lease.leaseEndDate)::TIMESTAMP_NTZ AS "leaseEndDate"
    , p.id AS "partyIDLinktoCall"
    , CASE
        WHEN (
                CASE
                    WHEN p.endDate IS NULL
                        THEN NULL
                    ELSE CASE
                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                            ELSE p.metadata: closeReasonId
                            END
                    END
                ) IS NULL
            THEN 'Open'
        ELSE (
                CASE
                    WHEN p.endDate IS NULL
                        THEN NULL
                    ELSE CASE
                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                            ELSE p.metadata: closeReasonId
                            END
                    END
                )
        END AS "closeReasonNonNull"
    , CASE
        WHEN (
                CASE
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
                    END
                ) = 'gold'
            THEN '1-Gold'
        WHEN (
                CASE
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
                    END
                ) = 'silver'
            THEN '2-Silver'
        WHEN (
                CASE
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
                    END
                ) = 'bronze'
            THEN '3-Bronze'
        WHEN (
                CASE
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
                    END
                ) = 'prospect'
            THEN '4-Prospect'
        WHEN (
                CASE
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
                    END
                ) = 'Unqualified'
            THEN '5-Unqualified'
        END AS "FinalLeadScoreSort"
    , '<a href=' || ('https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::VARCHAR) || '>Party Link</a>' AS "PartyLink"
    , CASE
        WHEN (REPLACE(initialTour.tourResult, '"', '')) IS NULL
            THEN ' '
        ELSE (REPLACE(initialTour.tourResult, '"', ''))
        END AS "tourResultNonNull"
    , CASE
        WHEN prop.name IS NULL
            THEN 'No Property'
        ELSE prop.name
        END AS "PropertyNonNull"
    , CASE
        WHEN p.STATE = 'Contact'
            THEN '1-Prospect'
        WHEN p.STATE = 'Lead'
            THEN '2-Contacts'
        WHEN p.STATE = 'Prospect'
            THEN '3-Tour/Quoted'
        WHEN p.STATE = 'Applicant'
            THEN '4-Applicant'
        WHEN p.STATE = 'Lease'
            THEN '5-Leasing'
        WHEN p.STATE = 'FutureResident'
            THEN '6-Future Resident'
        WHEN p.STATE = 'Resident'
            THEN '7-Resident'
        WHEN p.STATE = 'MovingOut'
            THEN '8-Moving Out'
        END AS "StageSort"
    , CASE
        WHEN prop.name = progProp.name
            THEN 1
        ELSE 0
        END AS "CampaignPropertyMatch"
    , CASE
        WHEN (month(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)) = month(CURRENT_TIMESTAMP())) AND (year(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)) = year(CURRENT_TIMESTAMP()))
            THEN 1
        WHEN (month(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = month(CURRENT_TIMESTAMP())) AND (year(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = year(CURRENT_TIMESTAMP()))
            THEN 1
        ELSE 0
        END AS "CurrentMoSIP"
    , CASE
        WHEN REPLACE(p.metadata: firstContactChannel, '"', '') = 'Self-book'
            THEN 30
        ELSE 60
        END AS "TourSIP$"
    , CASE
        WHEN (
                CASE
                    WHEN (
                            CASE
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
                                END
                            ) = 'gold'
                        THEN '1-Gold'
                    WHEN (
                            CASE
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
                                END
                            ) = 'silver'
                        THEN '2-Silver'
                    WHEN (
                            CASE
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
                                END
                            ) = 'bronze'
                        THEN '3-Bronze'
                    WHEN (
                            CASE
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
                                END
                            ) = 'prospect'
                        THEN '4-Prospect'
                    WHEN (
                            CASE
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
                                END
                            ) = 'Unqualified'
                        THEN '5-Unqualified'
                    END
                ) IN ('1-Gold', '2-Silver', '3-Bronze')
            THEN 1
        ELSE 0
        END AS "IsQualified"
    , CASE
        WHEN (
                CASE
                    WHEN (p.qualificationQuestions: groupProfile <> '')
                        THEN p.qualificationQuestions: groupProfile
                    ELSE 'NOT_YET_DETERMINED'
                    END
                ) IS NULL
            THEN 'Contact Not Yet Qualified'
        WHEN (
                CASE
                    WHEN (p.qualificationQuestions: groupProfile <> '')
                        THEN p.qualificationQuestions: groupProfile
                    ELSE 'NOT_YET_DETERMINED'
                    END
                ) = 'NOT_YET_DETERMINED'
            THEN 'Agent Marked Not Determined'
        WHEN (
                CASE
                    WHEN (p.qualificationQuestions: groupProfile <> '')
                        THEN p.qualificationQuestions: groupProfile
                    ELSE 'NOT_YET_DETERMINED'
                    END
                ) = 'message:'
            THEN 'No Lease Type Captured'
        ELSE (
                CASE
                    WHEN (p.qualificationQuestions: groupProfile <> '')
                        THEN p.qualificationQuestions: groupProfile
                    ELSE 'NOT_YET_DETERMINED'
                    END
                )
        END AS "LeaseTypeNN"
    , CASE
        WHEN (REPLACE(p.qualificationQuestions: cashAvailable, '"', '') IS NULL OR (REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = ' ') AND (
                coalesce(CASE
                        WHEN (p.qualificationQuestions: groupProfile <> '')
                            THEN p.qualificationQuestions: groupProfile
                        ELSE 'NOT_YET_DETERMINED'
                        END, ' ') = ' '
                ) AND (coalesce(REPLACE(p.qualificationQuestions: moveInTime, '"', ''), ' ') = ' ') AND (coalesce(REPLACE(p.qualificationQuestions: numBedrooms, '\n', ''), ' ') = ' ')
            THEN 'No'
        ELSE 'Yes'
        END AS "QQStarted"
    , CASE
        WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)) IS NULL
            THEN 0
        ELSE 1
        END AS "HasCompletedTour"
    , CASE
        WHEN (date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) IS NULL
            THEN 0
        ELSE 1
        END AS "HasSigned"
    , 1 AS "RecordCount"
    , CASE
        WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Self-book'
            THEN 'Property website'
        WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Chat'
            THEN 'Property website'
        WHEN s.displayName IS NULL
            THEN 'Agent Entered'
        ELSE s.displayName
        END AS "SourceSpecial"
    , CASE
        WHEN (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP) IS NULL
            THEN 0
        ELSE 1
        END AS "HasScheduledTour"
    , datediff(day, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate), COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), firstSubmittedApp.firstRequestDate), '1900-01-01 00:00:00'::TIMESTAMP)::TIMESTAMP) AS "DaysTourApplication"
    , datediff(day, COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), firstSubmittedApp.firstRequestDate), '1900-01-01 00:00:00'::TIMESTAMP)::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) AS "DaysApplicationSign"
    , datediff(day, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate), date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) AS "DaysTourSign"
    , CASE
        WHEN COALESCE(lease.Quote2LeaseDiff, 0) < 50
            THEN CASE
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP) IS NOT NULL AND (datediff(day, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate), date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP)) > 3
                        THEN 'Not Compliant'
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP) IS NOT NULL AND (datediff(day, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate), date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP)) < 4
                        THEN 'Compliant'
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP) IS NULL AND (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP) IS NOT NULL AND (datediff(day, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP)) > 3
                        THEN 'Not Compliant'
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.TourEndDate)::TIMESTAMP) IS NULL AND (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP) IS NOT NULL AND (datediff(day, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP)) < 4
                        THEN 'Compliant'
                    ELSE 'Not Compliant'
                    END
        ELSE 'Compliant'
        END AS "3DayCompliance"
    , datediff(day, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), initialTour.TourStartDate))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) AS "DaysFirstApptSign"
    , CASE
        WHEN (
                CASE STATE
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
                    END
                ) = 1
            THEN 'Yes'
        ELSE 'No'
        END AS "hasAppliedText"
    , CASE
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') IS NULL
            THEN 'Not Entered'
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'BEYOND_4_MONTHS'
            THEN 'BEYOND_4_MONTHS'
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'I_DONT_KNOW'
            THEN 'I_DONT_KNOW'
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_2_MONTHS'
            THEN 'NEXT_2_MONTHS'
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_MONTHS'
            THEN 'NEXT_4_MONTHS'
        WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_WEEKS'
            THEN 'NEXT_4_WEEKS'
        ELSE 'Not Entered'
        END AS "QQMoveInNN"
    , CASE
        WHEN (
                CASE
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1 AND (
                CASE
                    WHEN (date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1
            THEN '1 - Lease Executed'
        WHEN (
                CASE
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1 AND (
                CASE
                    WHEN (date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 0 AND (
                CASE
                    WHEN (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            ) IS NULL
                        THEN 'Open'
                    ELSE (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            )
                    END
                ) = 'Open' AND (
                CASE STATE
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
                    END
                ) = 1
            THEN '2 - Has Applied'
        WHEN (
                CASE
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1 AND (
                CASE
                    WHEN (date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 0 AND (
                CASE
                    WHEN (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            ) IS NULL
                        THEN 'Open'
                    ELSE (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            )
                    END
                ) = 'Open' AND (
                CASE STATE
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
                    END
                ) = 0
            THEN '3 - Remains Open'
        WHEN (
                CASE
                    WHEN (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1 AND (
                CASE
                    WHEN (date_trunc('month', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 0 AND (
                CASE
                    WHEN (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            ) IS NULL
                        THEN 'Open'
                    ELSE (
                            CASE
                                WHEN p.endDate IS NULL
                                    THEN NULL
                                ELSE CASE
                                        WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                            THEN 'MERGED_WITH_ANOTHER_PARTY'
                                        ELSE p.metadata: closeReasonId
                                        END
                                END
                            )
                    END
                ) <> 'Open'
            THEN '4 -Closed Lost'
        ELSE '5 - Error Condition'
        END AS "postTourStatus"
    , CASE
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP) = 1
            THEN 'Q1'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP) = 2
            THEN 'Q2'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP) = 3
            THEN 'Q3'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP) = 4
            THEN 'Q4'
        ELSE ''
        END AS "createdQuarter"
    , CASE
        WHEN QUARTER(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP) = 1
            THEN 'Q1'
        WHEN QUARTER(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP) = 2
            THEN 'Q2'
        WHEN QUARTER(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP) = 3
            THEN 'Q3'
        WHEN QUARTER(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate)::TIMESTAMP) = 4
            THEN 'Q4'
        ELSE ''
        END AS "touredQuarter"
    , CASE
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = 1
            THEN 'Q1'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = 2
            THEN 'Q2'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = 3
            THEN 'Q3'
        WHEN QUARTER(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate))::TIMESTAMP) = 4
            THEN 'Q4'
        ELSE ''
        END AS "signedQuarter"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP_NTZ AS "PartyCreatedDate2"
    , CASE
        WHEN (
                CASE
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') IS NULL
                        THEN 'Not Entered'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'BEYOND_4_MONTHS'
                        THEN 'BEYOND_4_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'I_DONT_KNOW'
                        THEN 'I_DONT_KNOW'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_2_MONTHS'
                        THEN 'NEXT_2_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_MONTHS'
                        THEN 'NEXT_4_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_WEEKS'
                        THEN 'NEXT_4_WEEKS'
                    ELSE 'Not Entered'
                    END
                ) = 'I_DONT_KNOW'
            THEN 'Indicated Not Known'
        WHEN (
                CASE
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') IS NULL
                        THEN 'Not Entered'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'BEYOND_4_MONTHS'
                        THEN 'BEYOND_4_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'I_DONT_KNOW'
                        THEN 'I_DONT_KNOW'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_2_MONTHS'
                        THEN 'NEXT_2_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_MONTHS'
                        THEN 'NEXT_4_MONTHS'
                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_WEEKS'
                        THEN 'NEXT_4_WEEKS'
                    ELSE 'Not Entered'
                    END
                ) = 'Not Entered'
            THEN 'Agent Provided No Answer'
        ELSE 'Move-in Timeframe Provided'
        END AS "QQMoveInByCategory"
    , CASE
        WHEN (REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = 'YES' OR (REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = 'NO'
            THEN 'Budget Provided'
        WHEN (REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = 'UNKNOWN'
            THEN 'Agent Entered Unknown'
        ELSE 'Agent Provided No Answer'
        END AS "QQBudgetByCategory"
    , CASE
        WHEN (REPLACE(p.qualificationQuestions: numBedrooms, '\n', '')) = '[]' OR (REPLACE(p.qualificationQuestions: numBedrooms, '\n', '')) is NULL
            THEN 'Agent Provided No Answer'
        ELSE 'Number of Bedrooms was Provided'
        END AS "QQNumBedroomsByCategory"
    , CASE
        WHEN prog.displayName IS NULL
            THEN ' '
        ELSE prog.displayName
        END AS "CampaignNN"
    , CASE
        WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Self-book'
            THEN 'Property website'
        WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Chat'
            THEN 'Property website'
        WHEN s.displayName IS NULL
            THEN 'Agent Entered'
        ELSE s.displayName
        END AS "SourceNonNull"
    , CASE
        WHEN (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                ) = 'Contact page on Parkmerced website'
            THEN 'Website'
        WHEN (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                ) = 'Contact Us'
            THEN 'Website'
        WHEN (
                CASE
                    WHEN prog.name IS NULL
                        THEN ' '
                    ELSE prog.NAME
                    END
                ) = 'Parkmerced website'
            THEN 'Website'
        WHEN (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                ) = 'Property Website'
            THEN 'Website'
        WHEN (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                ) = 'Serenity at Larkspur website'
            THEN 'Website'
        WHEN (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                ) = 'Sharon Green website'
            THEN 'Website'
        WHEN (
                (
                    CASE
                        WHEN prog.displayName IS NULL
                            THEN ' '
                        ELSE prog.displayName
                        END
                    )
                ) = 'The HUB team'
            THEN 'Website'
        WHEN REPLACE(p.metadata: firstContactChannel, '"', '') = 'Chat'
            THEN 'Chat'
        WHEN REPLACE(p.metadata: firstContactChannel, '"', '') = 'Self-book'
            THEN 'Self-Book'
        WHEN (
                CASE
                    WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Self-book'
                        THEN 'Property website'
                    WHEN s.displayName IS NULL AND REPLACE(p.metadata: firstContactChannel, '"', '') = 'Chat'
                        THEN 'Property website'
                    WHEN s.displayName IS NULL
                        THEN 'Agent Entered'
                    ELSE s.displayName
                    END
                ) = 'Agent Entered'
            THEN 'No Campaign'
        ELSE (
                CASE
                    WHEN prog.displayName IS NULL
                        THEN ' '
                    ELSE prog.displayName
                    END
                )
        END AS "CampaignSpecial"
    , CASE
        WHEN ((REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = 'YES' OR (REPLACE(p.qualificationQuestions: cashAvailable, '"', '')) = 'NO') AND (
                (
                    CASE
                        WHEN (
                                CASE
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') IS NULL
                                        THEN 'Not Entered'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'BEYOND_4_MONTHS'
                                        THEN 'BEYOND_4_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'I_DONT_KNOW'
                                        THEN 'I_DONT_KNOW'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_2_MONTHS'
                                        THEN 'NEXT_2_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_MONTHS'
                                        THEN 'NEXT_4_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_WEEKS'
                                        THEN 'NEXT_4_WEEKS'
                                    ELSE 'Not Entered'
                                    END
                                ) = 'I_DONT_KNOW'
                            THEN 'Indicated Not Known'
                        WHEN (
                                CASE
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') IS NULL
                                        THEN 'Not Entered'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'BEYOND_4_MONTHS'
                                        THEN 'BEYOND_4_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'I_DONT_KNOW'
                                        THEN 'I_DONT_KNOW'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_2_MONTHS'
                                        THEN 'NEXT_2_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_MONTHS'
                                        THEN 'NEXT_4_MONTHS'
                                    WHEN REPLACE(p.qualificationQuestions: moveInTime, '"', '') = 'NEXT_4_WEEKS'
                                        THEN 'NEXT_4_WEEKS'
                                    ELSE 'Not Entered'
                                    END
                                ) = 'Not Entered'
                            THEN 'Agent Provided No Answer'
                        ELSE 'Move-in Timeframe Provided'
                        END
                    ) = 'Move-in Timeframe Provided'
                ) AND CASE
                WHEN p.qualificationQuestions: groupProfile <> ''
                    THEN p.qualificationQuestions: groupProfile
                ELSE 'NOT_YET_DETERMINED'
                END <> 'NOT_YET_DETERMINED' AND (
                (
                    CASE
                        WHEN (REPLACE(p.qualificationQuestions: numBedrooms, '\n', '')) = '[]' OR (REPLACE(p.qualificationQuestions: numBedrooms, '\n', '')) IS NULL
                            THEN 'Agent Provided No Answer'
                        ELSE 'Number of Bedrooms was Provided'
                        END
                    ) = 'Number of Bedrooms was Provided'
                )
            THEN 1
        ELSE 0
        END AS "fullyQualified"
    , CASE
        WHEN origOwner.fullName is NULL
            THEN COALESCE(u.fullName, 'No Agent')
        ELSE origOwner.fullName
        END AS "OriginalPartyOwner2"
    , CASE
        WHEN (
                CASE
                    WHEN (p.endDate) IS NOT NULL
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
                    END
                ) = 'Include'
            THEN 1
        ELSE 0
        END AS "ValidLead"
    , CASE
        WHEN lk.hasHold IS NULL
            THEN 0
        ELSE 1
        END AS "hasEverHeldUnit"
    , CASE
        WHEN lk.hasManualEverHeldUnit IS NULL
            THEN 0
        ELSE lk.hasManualEverHeldUnit
        END AS "hasManualEverHeldUnit"
    , CASE
        WHEN lk.hasAutomaticEverHeldUnit IS NULL
            THEN 0
        ELSE lk.hasAutomaticEverHeldUnit
        END AS "hasAutomaticEverHeldUnit"
    --customernew specific fields
    , CASE
        WHEN COALESCE(FCT.inventoryGroupCode, 'None Selected') = 'None Selected'
            THEN 0
        ELSE 1
        END AS "hasFCTInventory"
    , CASE
        WHEN CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), FCT.completionDate) IS NULL
            THEN 0
        ELSE 1
        END AS "hasFCT"
    , CASE
        WHEN CASE
                WHEN p.workflowName = 'renewal'
                    THEN 1
                ELSE 0
                END = 0
            THEN 'New Lease'
        WHEN CASE
                WHEN p.workflowName = 'renewal'
                    THEN 1
                ELSE 0
                END = 1
            THEN 'Renewal'
        ELSE 'Renewal Type ERROR'
        END AS "isRenewalClean"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP_NTZ AS "PartyCreatedDate3"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at)))::TIMESTAMP_NTZ AS "PartyCreatedDate4"
    , CASE
        WHEN ((COALESCE(p.mergedWith::VARCHAR, 'None')) = NULL) AND (
                (
                    CASE
                        WHEN (
                                CASE
                                    WHEN p.endDate IS NULL
                                        THEN NULL
                                    ELSE CASE
                                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                                            ELSE p.metadata: closeReasonId
                                            END
                                    END
                                ) IS NULL
                            THEN 'Open'
                        ELSE (
                                CASE
                                    WHEN p.endDate IS NULL
                                        THEN NULL
                                    ELSE CASE
                                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                                            ELSE p.metadata: closeReasonId
                                            END
                                    END
                                )
                        END
                    ) = 'MERGED_WITH_ANOTHER_PARTY'
                )
            THEN 1
        ELSE 0
        END AS "UserMergedClose"
    , mr."applicationDecision" AS "mostRecentScreeningDecision"
    , mr."ApplicationDecisionClean" AS "mostRecentScreeningDecisionClean (1)"
    , CASE
        WHEN "mostRecentScreeningDecision" = 'approved' THEN 'Approved'
        WHEN "mostRecentScreeningDecision" = 'approved_with_cond' THEN 'Conditional Approval'
        WHEN "mostRecentScreeningDecision" = 'declined' THEN 'Declined'
        WHEN "mostRecentScreeningDecision" = 'further_review' THEN 'Further Review'
        WHEN "mostRecentScreeningDecision" = 'Guarantor Required' THEN 'Conditional Approval'
        ELSE "mostRecentScreeningDecision"
      END AS "mostRecentScreeningDecisionClean"
    , CASE
        WHEN "QQMoveInNN" = 'I_DONT_KNOW' OR "QQMoveInNN" = 'Not Entered'
            THEN 0
        ELSE 1
        END AS "hasMoveInDate"
    , CASE
        WHEN "QQNumBedroomsByCategory" = 'Agent Provided No Answer'
            THEN 0
        ELSE 1
        END AS "hasNumOfBedrooms"
    , COALESCE(FCT.tourType, '[No Tour Type Selected]') AS "FCTTourType"
    , CASE
	    WHEN FCT.isSelfService = 0 THEN 'Agent Added'
	    WHEN FCT.isSelfService = 1 THEN 'Self-Service'
      END AS "FCTisSelfServiceNN"
FROM {{ var("source_tenant") }}.PARTY p
LEFT OUTER JOIN (
    SELECT c.partyId
        , MIN(c.created_at)::TIMESTAMP AS contactDate
    FROM (
        SELECT c.CREATED_AT
            , fl.value::VARCHAR AS partyId
            , c.TYPE
            , c.DIRECTION
            , c.message: isRecorded AS isRecorded
        FROM {{ var("source_tenant") }}.COMMUNICATION AS c
            , LATERAL flatten(input => parse_json(c.parties)) fl
        WHERE type = 'ContactEvent' OR (direction = 'out' AND type IN ('Call', 'Email', 'Sms', 'Web')) OR (type = 'Call' AND message: isMissed <> 'true' AND message: isVoiceMail <> 'true')
        ) c
    GROUP BY c.partyId
    ) AS agentCon ON agentCon.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS u ON u.id = p.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN (
    SELECT partyId
        , CAST(metadata: startDate AS TIMESTAMPTZ) AS TourStartDate
        , CAST(metadata: endDate AS TIMESTAMPTZ) AS TourEndDate
        , created_at
        , metadata: appointmentResult AS tourResult
    FROM {{ var("source_tenant") }}.TASKS
    WHERE name = 'APPOINTMENT' QUALIFY ROW_NUMBER() OVER (
            PARTITION BY partyId ORDER BY partyId ASC
                , 2 ASC
                , created_at ASC
            ) = 1
    ) AS initialTour ON initialTour.partyId = p.id
LEFT OUTER JOIN (
    SELECT partyId
        , count(*) AS numTours
    FROM {{ var("source_tenant") }}.TASKS
    WHERE name = 'APPOINTMENT' AND STATE = 'Completed' AND metadata: appointmentResult = 'COMPLETE'
    GROUP BY partyId
    ) AS completedTours ON completedTours.partyId = p.id
LEFT OUTER JOIN FirstCompletedTour FCT ON FCT.partyId = p.id
LEFT OUTER JOIN (
    SELECT LISTAGG(u1.FULLNAME, ' | ') AS users
        , t1.taskId
    FROM (
        SELECT taskId
            , TRANSLATE(fl.value, '[]"', '') AS userid
        FROM FirstCompletedTour
            , LATERAL flatten(input => parse_json(ARRAY_CONSTRUCT(originalAssignees)::VARIANT)) fl
        ) t1
    INNER JOIN {{ var("source_tenant") }}.USERS u1 ON u1.id::VARCHAR = t1.userId
    GROUP BY t1.taskId
    ) AS origAssignee ON origAssignee.taskId = FCT.taskId
LEFT OUTER JOIN (
    SELECT l.partyId
        , l.signDate
        , COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0) - COALESCE((q.publishedQuoteData: leaseTerms [0].originalBaseRent)::DECIMAL, 0) AS Quote2LeaseDiff
        , REPLACE(l.baselineData: publishedLease: leaseStartDate, '"', '') AS leaseStartDate
        , REPLACE(l.baselineData: publishedLease: leaseEndDate, '"', '') AS leaseEndDate
    FROM {{ var("source_tenant") }}.LEASE AS l
    INNER JOIN {{ var("source_tenant") }}.QUOTE AS q ON q.id = l.quoteId
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
        FROM {{ var("source_tenant") }}.EXTERNALPARTYMEMBERINFO ei
        INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER pm ON pm.id = ei.partyMemberId
        WHERE ei.isPrimary = 'true'
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) AS pm ON pm.partyId = p.id
LEFT OUTER JOIN (
    SELECT pm1.partyId
        , LISTAGG(per.fullName, ' | ') AS NAMES
    FROM {{ var("source_tenant") }}.PARTYMEMBER AS pm1
    INNER JOIN {{ var("source_tenant") }}.PERSON AS per ON per.id = pm1.personId
    GROUP BY pm1.partyId
    ) AS peeps ON peeps.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS tCreator ON tCreator.id = FCT.createdBy
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS origOwner ON origOwner.id = FCT.originalPartyOwner
LEFT OUTER JOIN {{ var("source_tenant") }}.TEAMPROPERTYPROGRAM AS tpp ON tpp.id = p.teamPropertyProgramId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROGRAMS AS prog ON prog.id = tpp.programId
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
    FROM {{ var("source_tenant") }}.SOURCES
    ) AS s ON s.id = prog.sourceId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS progProp ON progProp.id = tpp.propertyId
LEFT OUTER JOIN (
    SELECT partyId
        , count(*) AS numChildren
    FROM {{ var("source_tenant") }}.PARTY_ADDITIONALINFO
    WHERE type = 'child'
    GROUP BY partyId
    ) AS childCount ON childCount.partyId = p.id
LEFT OUTER JOIN (
    SELECT DISTINCT partyId
    FROM {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION
    WHERE applicationStatus <> 'not_sent'
    ) AS startedApp ON startedApp.partyId = p.id
LEFT OUTER JOIN (
    SELECT partyId
        , min(req.created_at) AS firstRequestDate
    FROM {{ var("source_tenant") }}.RENTAPP_SUBMISSIONREQUEST req
    INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = req.partyApplicationId
    GROUP BY pa.partyId
    ) AS firstSubmittedApp ON firstSubmittedApp.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.TEAMS AS origTeam ON origTeam.id::TEXT = p.metadata: originalTeam
LEFT OUTER JOIN {{ var("source_tenant") }}.USERS AS firstCollab ON firstCollab.id::TEXT = p.metadata: firstCollaborator
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
        ) AS c00
    ) AS partyChannels ON partyChannels.partyId = p.id
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
    ) AS mostRecentComm ON mostRecentComm.partyId = p.id
LEFT OUTER JOIN (
    SELECT DISTINCT fl.value::VARCHAR AS partyId
        , 1 AS hasRecordedCall
    FROM {{ var("source_tenant") }}.COMMUNICATION
        , LATERAL flatten(input => parse_json(parties)) AS fl
    WHERE direction = 'in' AND type = 'Call' AND message: isRecorded = 'true'
    ) AS recordedCall ON recordedCall.partyId = p.id
LEFT OUTER JOIN (
    SELECT partyId
        , created_at
        , CASE
            WHEN createdFromCommId IS NULL
                THEN 0
            ELSE 1
            END AS isSelfService
    FROM {{ var("source_tenant") }}.QUOTE QUALIFY ROW_NUMBER() OVER (
            PARTITION BY partyId ORDER BY partyId
                , created_at ASC
            ) = 1
    ) AS initialQuote ON initialQuote.partyId = p.id
LEFT OUTER JOIN (
    SELECT DISTINCT partyId
        , 1 AS hasHold
        , MAX(CASE
                WHEN reason = 'manual'
                    THEN 1
                ELSE 0
                END) AS hasManualEverHeldUnit
        , MAX(CASE
                WHEN reason = 'automatic'
                    THEN 1
                ELSE 0
                END) AS hasAutomaticEverHeldUnit
    FROM {{ var("source_tenant") }}.InventoryOnHold
    GROUP BY partyId
    ) AS lk ON lk.partyId = p.id
LEFT OUTER JOIN {{ var("target_schema") }}."MostRecentReqResp" AS mr ON mr."partyId" = 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::VARCHAR AND mr."theRank" = 1
WHERE p.workflowName IN ('newLease', 'renewal')
