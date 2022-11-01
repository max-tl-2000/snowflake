/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.CommsDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.CommsDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.CommsDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='CommsDump') }}

SELECT CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIMESTAMP_NTZ AS "dumpGenDate"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at))::TIMESTAMP_NTZ AS "dateCommEvent"
    , ('1970-01-01 ' || ((CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at))::TIME)::VARCHAR)::TIMESTAMP_NTZ AS "timeCommEvent"
    , c.type AS "eventType"
    , c.id AS "communicationId"
    , 'PLACEHOLDER' AS "genBy"
    , COALESCE(s.displayName, p.metadata: source) AS "partySource"
    , COALESCE(prog.displayName, p.metadata: campaignId) AS "partyCampaign"
    , prog.reportingDisplayName AS "programReportingDisplayName"
    , prog.path AS "programPath"
    , prop.name AS "partyProperty"
    , po.fullName AS "partyOwner"
    , pot.name AS "partyOwnerTeam"
    , p.STATE AS "saleStage"
    , CASE
        WHEN p.endDate IS NULL
            THEN NULL
        ELSE CASE
                WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                    THEN 'MERGED_WITH_ANOTHER_PARTY'
                ELSE p.metadata: closeReasonId
                END
        END AS "partyCloseReason"
    , u.fullName AS "agent"
    , (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE((p.metadata: firstContactedDate)::TIMESTAMP, p.created_at))))::TIMESTAMP_NTZ AS "datePartyCreated"
    , (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), lease.signDate)))::TIMESTAMP_NTZ AS "partySignDate"
    , (date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), p.endDate)))::TIMESTAMP_NTZ AS "partyCloseDate"
    , c.category AS "category"
    , c.direction AS "direction"
    , REPLACE(REPLACE(REPLACE(CASE c.type
                    WHEN 'Call'
                        THEN COALESCE(c.message: to, c.message: toNumber)
                    ELSE NULL
                    END, '"', ''), '[', ''), ']', '') AS "toNumber"
    , REPLACE(REPLACE(REPLACE(CASE c.type
                    WHEN 'Call'
                        THEN c.message: from
                    ELSE NULL
                    END, '"', ''), '[', ''), ']', '') AS "fromNumber"
    , COALESCE(cs.displayName, c.message: campaignData: source) AS "commSource"
    , COALESCE(cprog.displayName, c.message: campaignData: campaignId) AS "commCampaign"
    , cprog.reportingDisplayName AS "commProgramReportingDisplayName"
    , cprog.path AS "commProgramPath"
    , commProp.name AS "commCampaignProperty"
    , REPLACE(c.message: duration, '"', '') AS "duration"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), cqs.entryTime)::TIMESTAMP_NTZ AS "queueEntryTime"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), cqs.exitTime)::TIMESTAMP_NTZ AS "queueExitTime"
    , TIMEDIFF(SECOND, cqs.entryTime, cqs.exitTime)::INT AS "queueDuration"
    , cqs.hangUp AS "queueHangUp"
    , cqsu.fullName AS "queueAgent"
    , cqs.callerRequestedAction AS "callerRequestedAction"
    , CASE
        WHEN cqs.callBackTime IS NOT NULL
            THEN 'Yes'
        WHEN cqs.callBackTime IS NULL AND cqs.id IS NOT NULL
            THEN 'No'
        ELSE NULL
        END AS "calledBack"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), cqs.callBackTime)::TIMESTAMP_NTZ AS "callBackTime"
    , TIMEDIFF(MINUTE, cqs.exitTime, cqs.callBackTime) AS "timeToCallBack"
    , cqs.transferredToVoiceMail AS "transferredToVoiceMail"
    , 'false' AS "connectedDirectly"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), cqs.created_at)::TIMESTAMP_NTZ AS "queueCreated_at"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), cqs.updated_at)::TIMESTAMP_NTZ AS "queueUpdated_at"
    , team."Team(s)"
    , '' AS "TeamPhoneNumber(s)"
    , EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT AS "callSeconds"
    , CASE
        WHEN c.type = 'Call'
            THEN COALESCE(c.message: isMissed, 'false')
        ELSE NULL
        END AS "isMissed"
    , (dateadd(SECOND, EXTRACT(MINUTE FROM to_time('00:' || (COALESCE(c.message: duration, 0)::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (COALESCE(c.message: duration, 0)::VARCHAR)))::INT, CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at)))::TIMESTAMP_NTZ AS "callEndedAt"
    , COALESCE(c.message: isMissed, 'false')::boolean AND NOT COALESCE(c.message: isVoiceMail, 'false')::boolean AS "hasHungUp"
    , COALESCE(cqsu.fullName, u.fullName) AS "answeredBy"
    , CASE c.unread
        WHEN 'false'
            THEN 0
        ELSE 1
        END AS "isUnread"
    , COALESCE(c.message:text, ' ') AS "text"
    , peeps."commPeople"
    , c.message: type AS "contactEventType"
    , c.message:rawMessageData: marketingSessionId AS "marketingSessionId"
    , CASE
        WHEN COALESCE(message: isDeclined, 'false') = 'false'
            THEN 0
        ELSE 1
        END AS "isDeclined"
    , CASE
        WHEN c.type <> 'Call'
            THEN '[NotACall]'
        WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
            THEN 'voicemail'
        WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
            THEN 'hangedUp'
        WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
            THEN 'hangedUp'
        WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
            THEN 'hangedUp'
        WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
            THEN 'completed'
        WHEN cqs.callerRequestedAction = 'call back'
            THEN 'callback'
        END AS "endOfCallType"
    , p.partyGroupId AS "partyGroupId"
    , COALESCE(c.message: notes, '') AS "callNotes"
    , CASE
        WHEN c.transferredFromCommId IS NULL
            THEN 0
        ELSE 1
        END AS "isTransfer"
    , floor((EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT) / 60) AS "callDurationMinutes"
    , 1 AS "recordCount"
    , CASE
        WHEN (
                CASE
                    WHEN c.type <> 'Call'
                        THEN '[NotACall]'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                        THEN 'voicemail'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                        THEN 'completed'
                    WHEN cqs.callerRequestedAction = 'call back'
                        THEN 'callback'
                    END
                ) = 'hangedUp'
            THEN 1
        ELSE 0
        END AS "HangUpCount"
    , CASE
        WHEN (
                CASE
                    WHEN c.type <> 'Call'
                        THEN '[NotACall]'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                        THEN 'voicemail'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                        THEN 'completed'
                    WHEN cqs.callerRequestedAction = 'call back'
                        THEN 'callback'
                    END
                ) = 'voicemail'
            THEN 1
        ELSE 0
        END AS "VoicemailCount"
    , CASE
        WHEN (
                CASE
                    WHEN c.type <> 'Call'
                        THEN '[NotACall]'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                        THEN 'voicemail'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                        THEN 'completed'
                    WHEN cqs.callerRequestedAction = 'call back'
                        THEN 'callback'
                    END
                ) = 'callback'
            THEN 1
        ELSE 0
        END AS "CallbackCount"
    , CASE
        WHEN (
                CASE
                    WHEN c.type <> 'Call'
                        THEN '[NotACall]'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                        THEN 'voicemail'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                        THEN 'completed'
                    WHEN cqs.callerRequestedAction = 'call back'
                        THEN 'callback'
                    END
                ) = 'completed'
            THEN 1
        ELSE 0
        END AS "CompletedCount"
    , CASE
        WHEN (
                CASE
                    WHEN c.type <> 'Call'
                        THEN '[NotACall]'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                        THEN 'voicemail'
                    WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                        THEN 'hangedUp'
                    WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                        THEN 'completed'
                    WHEN cqs.callerRequestedAction = 'call back'
                        THEN 'callback'
                    END
                ) IS NULL
            THEN 1
        ELSE 0
        END AS "NACallCount"
    , CASE
        WHEN c.direction = 'in'
            THEN 1
        ELSE 0
        END AS "InboundCount"
    , CASE
        WHEN c.direction = 'out'
            THEN 1
        ELSE 0
        END AS "OutboundCount"
    , CASE
        WHEN c.direction = 'in' AND (
                CASE
                    WHEN (
                            CASE
                                WHEN c.type <> 'Call'
                                    THEN '[NotACall]'
                                WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                                    THEN 'voicemail'
                                WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                                    THEN 'completed'
                                WHEN cqs.callerRequestedAction = 'call back'
                                    THEN 'callback'
                                END
                            ) = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "CompletedInCount"
    , CASE
        WHEN c.direction = 'out' AND (
                CASE
                    WHEN (
                            CASE
                                WHEN c.type <> 'Call'
                                    THEN '[NotACall]'
                                WHEN COALESCE(c.message: isVoiceMail, 'false') = 'true'
                                    THEN 'voicemail'
                                WHEN COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction = 'voicemail'
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND c.message: duration IS NULL AND (COALESCE(c.message: dialStatus, '') IN ('completed', ''))
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'true' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND cqs.callerRequestedAction IS NULL
                                    THEN 'hangedUp'
                                WHEN COALESCE(c.message: isMissed, 'false') = 'false' AND COALESCE(c.message: isVoiceMail, 'false') = 'false' AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT > 0
                                    THEN 'completed'
                                WHEN cqs.callerRequestedAction = 'call back'
                                    THEN 'callback'
                                END
                            ) = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "CompletedOutCount"
    , DATE_PART(hour, ('1970-01-01 ' || ((CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at))::TIME)::VARCHAR)::TIMESTAMP) AS "StartTimeHr"
    , DAYOFWEEKISO(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at))::TIMESTAMP) AS "dayOfWeekInteger"
    , CASE (DAYOFWEEKISO(date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), c.created_at))::TIMESTAMP))
        WHEN 1
            THEN 'Monday'
        WHEN 2
            THEN 'Tuesday'
        WHEN 3
            THEN 'Wednesday'
        WHEN 4
            THEN 'Thursday'
        WHEN 5
            THEN 'Friday'
        WHEN 6
            THEN 'Saturday'
        WHEN 7
            THEN 'Sunday'
        ELSE 'Unknown'
        END AS "dayOfWeekName"
    , COALESCE(team2.name, pot.name) as "Team"
FROM (
    SELECT c.*
        , fl.value::VARCHAR AS partyId
    FROM {{ var("source_tenant") }}.Communication c
        , LATERAL flatten(input => parse_json(c.parties)) fl
    ) AS c
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS u ON u.id = c.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.CallQueueStatistics AS cqs ON c.id = cqs.communicationId
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS cqsu ON cqsu.id = cqs.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.TeamPropertyProgram AS ctpp ON ctpp.id = c.teamPropertyProgramId
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS commProp ON commProp.id = ctpp.propertyId
LEFT OUTER JOIN (
    SELECT c1.id
        , listagg(t1.displayName, ' | ') within
    GROUP (
            ORDER BY t1.displayName
            ) AS "Team(s)"
    FROM (
        SELECT c.id
            , fl.value::VARCHAR AS team
        FROM {{ var("source_tenant") }}.Communication AS c
            , LATERAL flatten(input => parse_json(c.teams)) AS fl
        ) AS c1
    INNER JOIN {{ var("source_tenant") }}.Teams AS t1 ON t1.id = c1.team
    GROUP BY c1.id
    ) AS team ON c.id = team.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Teams AS team2 on team2.id = c.teams[1]
LEFT OUTER JOIN (
    SELECT c1.id
        , listagg(p1.fullName, ' | ') within
    GROUP (
            ORDER BY p1.fullName
            ) AS "commPeople"
    FROM (
        SELECT c.id
            , fl.value::VARCHAR AS person
        FROM {{ var("source_tenant") }}.Communication AS c
            , LATERAL flatten(input => parse_json(c.persons)) AS fl
        ) c1
    INNER JOIN {{ var("source_tenant") }}.Person AS p1 ON p1.id = c1.person
    GROUP BY c1.id
    ) AS peeps ON c.id = peeps.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Party AS p ON c.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.Lease AS lease ON p.id = lease.partyId AND lease.STATUS = 'executed'
LEFT OUTER JOIN {{ var("source_tenant") }}.TeamPropertyProgram AS tpp ON tpp.id = p.teamPropertyProgramId
LEFT OUTER JOIN {{ var("source_tenant") }}.Programs AS prog ON prog.id = tpp.programId
LEFT OUTER JOIN {{ var("source_tenant") }}.Programs AS cprog ON cprog.id = ctpp.programId
LEFT OUTER JOIN {{ var("source_tenant") }}.Sources AS cs ON cs.id = cprog.sourceId
LEFT OUTER JOIN {{ var("source_tenant") }}.Sources AS s ON s.id = prog.sourceId
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS po ON c.partyOwner = po.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Teams AS pot ON c.partyOwnerTeam = pot.id
