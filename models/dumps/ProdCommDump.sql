/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ProdCommDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ProdCommDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ProdCommDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ProdCommDump') }}

WITH Owners
AS (
    SELECT p.created_at AS partyCreation
        , u.fullName AS lastOwner
        , al.partyId
        , al.previousPrimaryAgentName
        , al.newPrimaryAgentName
        , al.created_at
        , lag(al.created_at, 1) OVER (
            PARTITION BY al.partyId ORDER BY al.created_at
            ) AS prev_createdat
    FROM (
        SELECT act.details: partyId::TEXT AS partyId
            , act.details: previousPrimaryAgentName::TEXT AS previousPrimaryAgentName
            , act.details: newPrimaryAgentName::TEXT AS newPrimaryAgentName
            , act.created_at
        FROM {{ var("source_tenant") }}.ActivityLog AS act
        WHERE act.component::TEXT = 'leasing team'::TEXT
        ) AS al
    JOIN {{ var("source_tenant") }}.Party AS p ON p.id = al.partyId::VARCHAR
    JOIN {{ var("source_tenant") }}.Users AS u ON u.id = p.userId
    ORDER BY p.id
        , al.created_at
    )
    , PartyOwners
AS (
    SELECT o.partyId
        , COALESCE(o.prev_createdat, o.partyCreation) AS "from"
        , o.created_at AS "to"
        , CASE
            WHEN o.previousPrimaryAgentName <> ''::TEXT
                THEN o.previousPrimaryAgentName
            ELSE o.newPrimaryAgentName
            END AS agent
        , o.lastOwner
    FROM Owners AS o
    ORDER BY o.partyId
        , (COALESCE(o.prev_createdat, o.partyCreation))
    )
    , CommParties
AS (
    SELECT fl.value AS partyId
        , c.id AS communicationId
        , c.teamPropertyProgramId
        , c.userId
        , c.created_at AS startDate
        , (det.details: endTime)::TIMESTAMP AS endDate
        , EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT AS durationSec
        , '00:' || REPLACE(c.message: duration, '"', '') AS duration
        , c.direction
        , COALESCE(c.message: from::TEXT
            , c.message: fromNumber::TEXT) AS fromNumber
	    , TRANSLATE(COALESCE(c.message: rawMessage: To::TEXT, c.message: to::TEXT, c.message: toNumber::TEXT),'[]"','') AS toNumber
        , c.persons
        , c.transferredFromCommId
        , ttc.id AS transferredTo
        , CASE
            WHEN COALESCE(c.message: isVoiceMail::TEXT, 'false'::TEXT)::boolean = true
                THEN EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT
            ELSE NULL::DOUBLE
            END AS voiceMailDurationSec
        , CASE
            WHEN COALESCE(c.message: isVoiceMail::TEXT, 'false'::TEXT)::boolean = true
                THEN '00:'::TEXT || (c.message: duration::TEXT)
            ELSE NULL::TEXT
            END AS voiceMailDuration
        , CASE
            WHEN COALESCE(c.message: isVoiceMail::TEXT, 'false'::TEXT)::boolean = false AND COALESCE(c.message: isMissed::TEXT, 'false'::TEXT)::boolean = false
                THEN EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::VARCHAR
            ELSE NULL::DOUBLE
            END AS talkDurationSec
        , CASE
            WHEN COALESCE(c.message: isVoiceMail::TEXT, 'false'::TEXT)::boolean = false AND COALESCE(c.message: isMissed::TEXT, 'false'::TEXT)::boolean = false
                THEN '00:'::TEXT || (c.message: duration::TEXT)
            ELSE NULL::TEXT
            END AS talkDuration
        , COALESCE(c.message: isVoiceMail::TEXT, 'false'::TEXT)::boolean AS isVoiceMail
        , COALESCE(c.message: isMissed::TEXT, 'false'::TEXT)::boolean AS isMissed
        , c.message: duration::TEXT AS msgduration
        , COALESCE(c.message: dialStatus::TEXT, ''::TEXT) AS dialStatus
        , c.created_at
        , CASE c.unread
            WHEN 'false'
                THEN 0
            ELSE 1
            END AS isUnread
        , c.type AS eventType
        , CASE
            WHEN COALESCE(c.message: isDeclined, 'false') = 'false'
                THEN 0
            ELSE 1
            END AS isDeclined
        , t.name AS "partyOwnerTeam"
    FROM {{ var("source_tenant") }}.Communication AS c
    LEFT JOIN {{ var("source_tenant") }}.Teams AS t ON c.partyOwnerTeam = t.id
    LEFT JOIN {{ var("source_tenant") }}.CallDetails AS det ON c.id = det.commId
    LEFT JOIN {{ var("source_tenant") }}.Communication AS ttc ON c.id = ttc.transferredFromCommId
        , LATERAL flatten(input => parse_json(c.parties)) AS fl
    WHERE c.type::TEXT = 'Call'::TEXT
    )
    , TeamProp
AS (
    SELECT t.displayName
        , cmp.directPhoneIdentifier
        , tpp.id
        , tpp.commDirection
    FROM {{ var("source_tenant") }}.Teams AS t
    JOIN {{ var("source_tenant") }}.TeamPropertyProgram AS tpp ON tpp.teamId = t.id
    JOIN {{ var("source_tenant") }}.Programs AS cmp ON cmp.id = tpp.programId
    )
    , CallQS
AS (
    SELECT cqs.communicationId
        , cqs.entryTime
        , cqs.exitTime
        , cqs.callerRequestedAction
        , CASE
            WHEN cqs.callBackTime IS NOT NULL
                THEN true
            ELSE false
            END AS calledBack
        , CASE
            WHEN cqs.callBackTime IS NOT NULL
                THEN TIMEDIFF(MINUTE, cqs.exitTime, cqs.callBackTime)
            ELSE NULL::DOUBLE
            END AS timeToCallBack
        , cqs.callBackCommunicationId
    FROM {{ var("source_tenant") }}.CallQueueStatistics AS cqs
    LEFT JOIN {{ var("source_tenant") }}.Users AS u ON u.id = cqs.userId
    )
    , AnswTeams
AS (
    SELECT t.displayName
        , u.fullName
        , p.name
        , t.module
    FROM {{ var("source_tenant") }}.TeamMembers AS pm
    JOIN {{ var("source_tenant") }}.Users AS u ON pm.userId = u.id
    JOIN {{ var("source_tenant") }}.Teams AS t ON t.id = pm.teamId
    JOIN {{ var("source_tenant") }}.TeamProperties AS tp ON tp.teamId = t.id
    JOIN {{ var("source_tenant") }}.Property AS p ON tp.propertyId = p.id
    WHERE pm.inactive = false
    )
    , CallComms
AS (
    SELECT p.id AS partyId
        , cp.communicationId
        , assignprop.name AS propertyName
        , cp.transferredTo
        , p.userId AS partyUserId
        , CONVERT_TIMEZONE(COALESCE(assignprop.timezone, team.timeZone), cp.startDate)::DATE AS startDate
        , ('1970-01-01 ' || ((CONVERT_TIMEZONE(COALESCE(assignprop.timezone, team.timeZone), cp.startDate))::TIME)::VARCHAR)::TIMESTAMP_NTZ AS startTime
        , CONVERT_TIMEZONE(COALESCE(assignprop.timezone, team.timeZone), cp.endDate)::DATE AS endDate
        , ('1970-01-01 ' || ((CONVERT_TIMEZONE(COALESCE(assignprop.timezone, team.timeZone), cp.endDate))::TIME)::VARCHAR)::TIMESTAMP_NTZ AS endTime
        , cp.durationSec
        , cp.duration
        , cp.direction
        , cp.fromNumber
        , cp.toNumber
        , cp.persons
        , cp.transferredFromCommId
        , cp.voiceMailDurationSec
        , cp.voiceMailDuration
        , cp.talkDurationSec
        , cp.talkDuration
        , timediff(microsecond, cqs.entryTime, cqs.exitTime) / 1000000 AS inQueueDurationSec
        , to_time(floor(TIMEDIFF(microsecond, cqs.entryTime, cqs.exitTime) / 1000000)::VARCHAR)::VARCHAR AS inQueueDuration
        , cqs.callerRequestedAction AS queueCallerRequestedAction
        , CASE
            WHEN cqs.callerRequestedAction IS NOT NULL
                THEN cqs.calledBack
            ELSE NULL::boolean
            END AS queueWasCalledBack
        , cqs.callBackCommunicationId AS queueCallbackCommunicationId
        , lpad((floor(cqs.timeToCallBack / 60))::VARCHAR, 2, '0') || ':' || lpad(mod(cqs.timeToCallBack, 60)::VARCHAR, 2, '0') || ':00' AS queueTimeToCallBack
        , cqs.timeToCallBack AS queueTimeToCallBackMin
        , CASE
            WHEN cp.isVoiceMail = true
                THEN 'voicemail'::TEXT
            WHEN cp.isMissed = false AND cp.isVoiceMail = false AND cp.msgduration IS NULL AND (cp.dialStatus = 'completed'::TEXT OR cp.dialStatus = ''::TEXT)
                THEN 'hangedUp'::TEXT
            WHEN cp.isMissed = true AND cp.isVoiceMail = false AND cqs.callerRequestedAction IS NULL
                THEN 'hangedUp'::TEXT
            WHEN cp.isMissed = false AND cp.isVoiceMail = false AND cp.durationSec > 0::DOUBLE
                THEN 'completed'::TEXT
            WHEN cqs.callerRequestedAction = 'call_back'::TEXT
                THEN 'callback'::TEXT
            ELSE NULL::TEXT
            END AS endOfCallType
        , u.fullName AS agent
        , pers.fullName AS contactName
        , src.name AS commSource
        , cmp.name AS commCampaign
        , commprop.name AS commCampaignProperty
        , tp.displayName AS calledTeam
        , tp1.displayName AS fromTeam
        , COALESCE(po.agent, up.fullName::TEXT) AS partyOwner
        , cp.isUnread
        , cp.eventType
        , cp.isDeclined
        , p.workflowName
        , cp."partyOwnerTeam"
    FROM CommParties AS cp
    LEFT JOIN {{ var("source_tenant") }}.Party AS p ON cp.partyId::VARCHAR = p.id
    LEFT JOIN {{ var("source_tenant") }}.Property AS assignprop ON assignprop.id = p.assignedPropertyId
    LEFT JOIN {{ var("source_tenant") }}.Teams AS team ON team.id = p.ownerTeam::VARCHAR
    LEFT JOIN CallQS AS cqs ON cp.communicationId = cqs.communicationId
    LEFT JOIN {{ var("source_tenant") }}.Users AS u ON u.id = cp.userId
    LEFT JOIN {{ var("source_tenant") }}.Person AS pers ON pers.id = REPLACE(parse_json(cp.persons) [0]::VARCHAR, '"', '')
    LEFT JOIN {{ var("source_tenant") }}.TeamPropertyProgram AS tpp ON tpp.id = cp.teamPropertyProgramId
    LEFT JOIN {{ var("source_tenant") }}.Programs AS cmp ON cmp.id = tpp.programId
    LEFT JOIN {{ var("source_tenant") }}.Sources AS src ON src.id = cmp.sourceId
    LEFT JOIN {{ var("source_tenant") }}.Property AS commprop ON commprop.id = tpp.propertyId
    LEFT JOIN TeamProp AS tp ON tp.directPhoneIdentifier::TEXT = cp.toNumber AND tp.commDirection::TEXT = 'in'::TEXT
    LEFT JOIN TeamProp AS tp1 ON tp1.id = cp.teamPropertyProgramId
    LEFT JOIN PartyOwners AS po ON po.partyId::VARCHAR = p.id AND cp.created_at >= po."from" AND cp.created_at < po."to"
    LEFT JOIN {{ var("source_tenant") }}.Users AS up ON p.userId = up.id
    )
SELECT CASE
        WHEN cd.calledTeam::VARCHAR <> 'The HUB'::VARCHAR
            THEN cd.calledTeam
        WHEN cd.direction::VARCHAR = 'out'::VARCHAR
            THEN NULL::VARCHAR
        ELSE answ.displayName
        END AS "answeringTeam"
    , cd.communicationId AS "communicationId"
    , cd.partyId AS "partyId"
    , cd.partyOwner AS "partyOwner"
    , cd."partyOwnerTeam"
    , cd.propertyName AS "propertyName"
    , cd.startDate::TIMESTAMP_NTZ AS "startDate"
    , cd.startTime AS "startTime"
    , cd.endDate::TIMESTAMP_NTZ AS "endDate"
    , cd.endTime AS "endTime"
    , cd.endOfCallType AS "endOfCallType"
    , COALESCE(cd.durationSec, 0)::NUMBER(10,0) AS "durationSec"
    , COALESCE(cd.duration, '00:00:00'::TEXT) AS "duration"
    , cd.direction AS "direction"
    , cd.fromNumber AS "fromNumber"
    , TRANSLATE(cd.toNumber,'"[]','') AS "toNumber"
    , cd.calledTeam AS "calledTeam"
    , CASE
        WHEN cd.direction::TEXT = 'in'::TEXT
            THEN NULL::VARCHAR
        ELSE cd.fromTeam
        END AS "fromTeam"
    , CASE
        WHEN cd.voiceMailDurationSec > 0::DOUBLE
            THEN NULL::VARCHAR
        ELSE cd.agent
        END AS "agent"
    , TRANSLATE(cd.persons, '"[]', '') AS "persons"
    , cd.contactName AS "contactName"
    , cd.commSource AS "commSource"
    , cd.commCampaign AS "commCampaign"
    , cd.commCampaignProperty AS "commCampaignProperty"
    , cd.transferredFromCommId AS "transferredFrom"
    , cd.transferredTo AS "transferredTo"
    , COALESCE(cd.inQueueDurationSec, 0)::NUMBER(10,0) AS "inQueueDurationSec"
    , COALESCE(cd.inQueueDuration, '00:00:00'::TEXT) AS "inQueueDuration"
    , COALESCE(cd.voiceMailDurationSec, 0)::NUMBER(10,0) AS "voiceMailDurationSec"
    , COALESCE(cd.voiceMailDuration, '00:00:00'::TEXT) AS "voiceMailDuration"
    , COALESCE(cd.talkDurationSec, 0)::NUMBER(10,0) AS "talkDurationSec"
    , COALESCE(cd.talkDuration, '00:00:00'::TEXT) AS "talkDuration"
    , CASE
        WHEN cd.calledTeam::TEXT = 'The HUB'::TEXT
            THEN true
        ELSE false
        END AS "hasQueue"
    , cd.queueCallerRequestedAction AS "queueCallerRequestedAction"
    , cd.queueWasCalledBack AS "queueWasCalledBack"
    , cd.queueCallbackCommunicationId AS "queueCallbackCommunicationId"
    , cd.queueTimeToCallBack AS "queueTimeToCallBack"
    , cd.queueTimeToCallBackMin::NUMBER(10,0) AS "queueTimeToCallBackMin"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || cd.partyId AS "partyIdWithURL"
    , cd.isUnread AS "isUnread"
    , cd.eventType AS "eventType"
    , cd.isDeclined AS "isDeclined"
    , CASE
        WHEN direction = 'in'
            THEN 1
        ELSE 0
        END AS "InboundCount"
    , CASE
        WHEN direction = 'out'
            THEN 1
        ELSE 0
        END AS "OutboundCount"
    , CASE
        WHEN cd.endOfCallType = 'hangedUp'
            THEN 1
        ELSE 0
        END AS "HangUpCount"
    , CASE
        WHEN cd.endOfCallType = 'voicemail'
            THEN 1
        ELSE 0
        END AS "VoicemailCount"
    , CASE
        WHEN cd.endofcalltype = 'callback'
            THEN 1
        ELSE 0
        END AS "CallbackCount"
    , CASE
        WHEN cd.endofcalltype = 'completed'
            THEN 1
        ELSE 0
        END AS "CompletedCount"
    , CASE
        WHEN cd.endofcalltype IS NULL
            THEN 1
        ELSE 0
        END AS "NACallCount"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'in'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN cd.endofcalltype = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "CompletedInCount"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'out'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN cd.endofcalltype = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "CompletedOutCount"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'in'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN (
                    CASE
                        WHEN COALESCE(cd.durationSec, 0) IS NULL
                            THEN 0
                        ELSE COALESCE(cd.durationSec, 0)
                        END
                    )
        ELSE 0
        END::NUMBER(10,0) AS "InboundDuration"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'out'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN (
                    CASE
                        WHEN COALESCE(cd.durationSec, 0) IS NULL
                            THEN 0
                        ELSE COALESCE(cd.durationSec, 0)
                        END
                    )
        ELSE 0
        END::NUMBER(10,0) AS "OutboundDuration"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'in'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN cd.endofcalltype = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN (COALESCE(cd.inQueueDurationSec, 0)) IS NULL
                        THEN 0
                    ELSE (COALESCE(cd.inQueueDurationSec, 0))
                    END
                ) < 90
            THEN 1
        ELSE 0
        END AS "InboundInSLA"
    , CASE
        WHEN (
                CASE
                    WHEN direction = 'in'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN cd.endofcalltype = 'completed'
                        THEN 1
                    ELSE 0
                    END
                ) = 1 AND (
                CASE
                    WHEN (COALESCE(cd.inQueueDurationSec, 0)) IS NULL
                        THEN 0
                    ELSE (COALESCE(cd.inQueueDurationSec, 0))
                    END
                ) > 89
            THEN 1
        ELSE 0
        END AS "InboundOutSLA"
    , CASE
        WHEN (
                CASE
                    WHEN cd.calledTeam::TEXT = 'The HUB'::TEXT
                        THEN TRUE
                    ELSE FALSE
                    END
                ) = 'true'
            THEN 1
        ELSE 0
        END AS "HitQueueCount"
    , CASE
        WHEN (
                CASE
                    WHEN cd.calledTeam::TEXT = 'The HUB'::TEXT
                        THEN TRUE
                    ELSE FALSE
                    END
                ) = 'true'
            THEN 'true'
        ELSE 'false'
        END AS "hasQueueNN"
    , CASE
        WHEN (COALESCE(cd.inQueueDurationSec, 0)) IS NULL
            THEN 0
        ELSE (COALESCE(cd.inQueueDurationSec, 0))
        END::NUMBER(10,0) AS "inQueueDurationSecNN"
    , CASE
        WHEN COALESCE(cd.talkDurationSec, 0) IS NULL
            THEN 0
        ELSE COALESCE(cd.talkDurationSec, 0)
        END::NUMBER(10,0) AS "talkDurationSecNN"
    , CASE
        WHEN COALESCE(cd.durationSec, 0) IS NULL
            THEN 0
        ELSE COALESCE(cd.durationSec, 0)
        END::NUMBER(10,0) AS "durationSecNN"
    , hour(cd.startTime) AS "StartTimeHr"
    , cd.startTime::TEXT AS "StartTimeText"
    , CASE
        WHEN 'False' = 'True'
            THEN 1
        ELSE 0
        END AS "QConnectedDirectlyCount"
    , CASE
        WHEN (
                CASE
                    WHEN cd.voiceMailDurationSec > 0::DOUBLE
                        THEN NULL::VARCHAR
                    ELSE cd.agent
                    END
                ) IS NULL
            THEN 'No Agent'
        ELSE agent
        END AS "AgentNN"
    , cd.endTime::TEXT AS "endTimeText"
    , CASE DAYNAME(cd.startDate)
        WHEN 'Mon' THEN 'Monday'
        WHEN 'Tue' THEN 'Tuesday'
        WHEN 'Wed' THEN 'Wednesday'
        WHEN 'Thu' THEN 'Thursday'
        WHEN 'Fri' THEN 'Friday'
        WHEN 'Sat' THEN 'Saturday'
        WHEN 'Sun' THEN 'Sunday'
      END AS "dayOfWeekName"
    , DAYOFWEEKISO(cd.startDate) AS "dayOfWeekInteger"
FROM CallComms cd
LEFT JOIN (
    SELECT t.displayName
        , t.fullName
        , t.NAME
        , t.module
    FROM AnswTeams AS t QUALIFY ROW_NUMBER() OVER (
            PARTITION BY fullName
            , name ORDER BY module
            ) = 1
    ) AS answ ON answ.fullName = cd.partyOwner
        AND (answ.name::VARCHAR = cd.propertyName::VARCHAR)
WHERE cd.workflowName IN ('newLease', 'renewal')
