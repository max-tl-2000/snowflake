/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ReqRespDetails --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ReqRespDetails --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ReqRespDetails --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ReqRespDetails') }}

SELECT combineUnions.*
    , rank() OVER (
        PARTITION BY combineUnions."requestId" ORDER BY combineUnions."responseDate" DESC
        ) AS "mostRecentRank"
FROM (
    SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || pa.partyId AS "partyId"
        , pa.partyId AS "partyIdNoURL"
        , pa.id AS "partyAppId"
        , req.id AS "requestId"
        , CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ AS "requestDate"
        , req.requestType AS "requestType"
        , req.rentData AS "requestRentData"
        , req.applicantData AS "requestApplicantData"
        , req.transactionNumber AS "requestTransactionNum"
        , req.requestResult AS "requestResult"
        , resp.id AS "responseId"
        , CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ AS "responseDate"
        , resp.STATUS AS "responseStatus"
        , resp.applicationDecision AS "responseApplicationDecision"
        , OBJECT_CONSTRUCT('applicant(s)', resp.applicantDecision) AS "responseApplicantDecision"
        , resp.criteriaResult AS "responseCriteriaResult"
        , OBJECT_CONSTRUCT('recommendation(s)', resp.recommendations) AS "responseRecommendations"
        , resp.externalId AS "responseExternalId"
        , numMembers.numGuarantors AS "numGuarantors"
        , numMembers.numApplicants AS "numApplicants"
        , CONVERT_TIMEZONE(prop.timezone, firstReq.requestDate)::TIMESTAMP_NTZ AS "firstRequestDate"
        , req.quoteId AS "requestQuoteId"
        , reqChanged.quoteChanged AS "quoteChanged"
        , reqChanged.numApplicantsChanged AS "numApplicantsChanged"
        , COALESCE(l.STATUS, 'none') AS "leaseStatus"
        , parse_json(resp.rawResponse) :ApplicantScreening :Response [0] :ServiceStatus AS "responseServiceStatus"
        , parse_json(resp.rawResponse) :ApplicantScreening :Response [0] :BlockedStatus AS "blockedStatus"
        , REPLACE((parse_json(resp.rawResponse)) :ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :RentToIncomes [0] :Applicant [0] :CreditScore [0], '"', '') AS "creditScore"
        , CASE
            WHEN req.requestType = 'New'
                THEN '1-New'
            WHEN req.requestType = 'Modify'
                THEN '2-Modify'
            ELSE 'ERROR'
            END AS "RequestTypeSort"
        , timediff(second,CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ,CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ)/60::DECIMAL(10,3)+1 AS "ReqResMinutes"
        , CASE
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) IS NULL
                THEN '7 No Resonse'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 4
                THEN '1 < 3 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 11
                THEN '2 <10 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 31
                THEN '3 <30 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 61
                THEN '4 <1 hour'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 121
                THEN '5 <2 hours'
            ELSE '6 >2 hours'
            END AS "BucketGroups"
        , 1 AS "ReqCount"
        , CASE
            WHEN respcriteria.ID IS NOT NULL THEN 1
            ELSE 0
            END AS "CR100Flag"
        , CASE
            WHEN "blockedStatus" like '%frozen%' THEN 1
            ELSE 0
            END AS "CreditFreezeFlag"
        , CASE
            WHEN "blockedStatus" like '%Criminal generated%' THEN 1
            ELSE 0
            END AS "CriminalErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%GlobalSanction%' THEN 1
            ELSE 0
            END AS "GlobalSanctionErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%SexOffender%' THEN 1
            ELSE 0
            END AS "SexOffenderErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Credit Bureau%' THEN 1
            ELSE 0
            END AS "CreditBureauErrorFlag"
        , CASE
            WHEN "blockedStatus" IS NULL THEN 1
            ELSE 0
            END AS "BlockedStatusNullFlag"
        , CASE
            WHEN "blockedStatus" = '[""]' THEN 1
            ELSE 0
            END AS "BlockedStatusQuotesFlag"
        , CASE
            WHEN "blockedStatus" like '%This key is already associated%' THEN 1
            ELSE 0
            END AS "ThisKeyAlreadyAssociatedFlag"
        , CASE
            WHEN "blockedStatus" like '%SSN encountered a problem%' THEN 1
            ELSE 0
            END AS "SSNErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Multiple-step operation generated%' THEN 1
            ELSE 0
            END AS "MultiStepErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Cannot calculate the Guarantor Rent%' THEN 1
            ELSE 0
            END AS "CannotCalcGuarFlag"
        , CASE
            WHEN "blockedStatus" like '%Update of timefinish for status record%' THEN 1
            WHEN "blockedStatus" like '%Application was unsuccessful in 180 tries%' THEN 1
            ELSE 0
            END AS "TimeErrorFlags"
        , CASE
            WHEN "blockedStatus" like '%Out of string space%' THEN 1
            WHEN "blockedStatus" like '%StagerWorkerException%' THEN 1
            ELSE 0
            END AS "OtherErrorFlag"
        , CASE
            WHEN position('"Global Sanctions"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Criminal"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Sex Offender"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "CriminalInProcessFlag"
        , CASE
            WHEN position('"Global Sanctions Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Criminal Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Sex Offender Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "CriminalOfflineProcFlag"
        , CASE
            WHEN position('"Credit"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Eviction"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Collections"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"SkipWatch"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "FinancialInProcessFlag"
        , CASE
            WHEN resp.id IS NULL THEN 0
			ELSE timediff(minute,CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ,CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ)
            END AS "RequestResponseTime"
        , CASE
            WHEN "responseCriteriaResult":"116":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"701":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"702":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"806":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"849":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"851":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"852":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"853":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"901":"passFail"::VARCHAR = 'F' THEN 1
            ELSE 0
        END AS "DeniedFinancialFlag"
        , CASE
            WHEN "responseCriteriaResult":"306":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"321":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"327":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"329":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"330":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"331":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"337":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"501":"passFail"::VARCHAR = 'F' THEN 1
            ELSE 0
        END AS "DeniedCriminalFlag"
    FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest AS req
    INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication AS pa ON pa.id = req.partyApplicationId
    INNER JOIN {{ var("source_tenant") }}.Party AS p ON p.id = pa.partyId
    LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
    LEFT OUTER JOIN {{ var("source_tenant") }}.rentapp_SubmissionResponse AS resp ON resp.submissionRequestId = req.id
    LEFT OUTER JOIN (
        SELECT ID
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
            , LATERAL flatten(input => CRITERIARESULT) fl
        WHERE fl.KEY = 100 AND fl.value::VARCHAR LIKE '%"passFail":"F"%'
        ) respcriteria ON respcriteria.ID = resp.ID
    LEFT OUTER JOIN (
        SELECT requestId
            , sum(CASE
                    WHEN applicants.type = 'Guarantor'
                        THEN 1
                    ELSE 0
                    END) AS numGuarantors
            , sum(CASE
                    WHEN applicants.type = 'Applicant'
                        THEN 1
                    ELSE 0
                    END) AS numApplicants
        FROM (
            SELECT id AS requestId
                , REPLACE(fl.value: type, '"', '') AS type
            FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
                , LATERAL flatten(input => parse_json(applicantData) :applicants) AS fl
            ) AS applicants
        GROUP BY requestId
        ) AS numMembers ON numMembers.requestId = req.id
    LEFT OUTER JOIN (
        SELECT partyApplicationId
            , min(created_at) AS requestDate
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
        GROUP BY partyApplicationId
        ) AS firstReq ON firstReq.partyApplicationId = pa.id
    LEFT OUTER JOIN (
        SELECT req.id AS requestId
            , lag(quoteId) OVER (
                PARTITION BY partyApplicationId ORDER BY created_at
                ) AS previousQuoteId
            , ARRAY_SIZE(lag(applicantData: applicants) OVER (
                    PARTITION BY partyApplicationId ORDER BY created_at
                    )) AS previousApplicants
            , CASE
                WHEN lag(req.quoteId) OVER (
                        PARTITION BY req.partyApplicationId ORDER BY req.created_at
                        ) <> req.quoteId
                    THEN 1
                WHEN (
                        lag(req.quoteId) OVER (
                            PARTITION BY req.partyApplicationId ORDER BY req.created_at
                            ) IS NULL AND req.quoteId IS NOT NULL
                        )
                    THEN CASE
                            WHEN lag(req.id) OVER (
                                    PARTITION BY partyApplicationId ORDER BY req.created_at
                                    ) IS NULL
                                THEN 0
                            ELSE 1
                            END
                ELSE 0
                END AS quoteChanged
            , CASE
                WHEN ARRAY_SIZE(req.applicantData: applicants) <> ARRAY_SIZE(lag(req.applicantData: applicants) OVER (
                            PARTITION BY req.partyApplicationId ORDER BY req.created_at
                            ))
                    THEN 1
                ELSE 0
                END AS numApplicantsChanged
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest AS req
        ) AS reqChanged ON reqChanged.requestId = req.id
    LEFT OUTER JOIN (
        SELECT partyId
            , STATUS
            , rank() OVER (
                PARTITION BY partyId ORDER BY created_at DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.Lease
        ) AS l ON l.partyId = p.id AND l.theRank = 1
    WHERE req.created_at > dateadd(MONTH, - 14, CURRENT_TIMESTAMP()) AND (resp.STATUS <> 'Complete' OR resp.STATUS IS NULL)

    UNION ALL

    SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || pa.partyId AS "partyId"
        , pa.partyId AS "partyIdNoURL"
        , pa.id AS "partyAppId"
        , req.id AS "requestId"
        , CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ AS "requestDate"
        , req.requestType AS "requestType"
        , req.rentData AS "requestRentData"
        , req.applicantData AS "requestApplicantData"
        , req.transactionNumber AS "requestTransactionNum"
        , req.requestResult AS "requestResult"
        , resp.id AS "responseId"
        , CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ AS "responseDate"
        , resp.STATUS AS "responseStatus"
        , resp.applicationDecision AS "responseApplicationDecision"
        , OBJECT_CONSTRUCT('applicant(s)', resp.applicantDecision) AS "responseApplicantDecision"
        , resp.criteriaResult AS "responseCriteriaResult"
        , OBJECT_CONSTRUCT('recommendation(s)', resp.recommendations) AS "responseRecommendations"
        , resp.externalId AS "responseExternalId"
        , numMembers.numGuarantors AS "numGuarantors"
        , numMembers.numApplicants AS "numApplicants"
        , CONVERT_TIMEZONE(prop.timezone, firstReq.requestDate)::TIMESTAMP_NTZ AS "firstRequestDate"
        , req.quoteId AS "requestQuoteId"
        , reqChanged.quoteChanged AS "quoteChanged"
        , reqChanged.numApplicantsChanged AS "numApplicantsChanged"
        , COALESCE(l.STATUS, 'none') AS leaseStatus
        , parse_json(resp.rawResponse) :ApplicantScreening :Response [0] :ServiceStatus AS "responseServiceStatus"
        , parse_json(resp.rawResponse) :ApplicantScreening :Response [0] :BlockedStatus AS "blockedStatus"
        , REPLACE((parse_json(resp.rawResponse)) :ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :RentToIncomes [0] :Applicant [0] :CreditScore [0], '"', '') AS "creditScore"
        , CASE
            WHEN req.requestType = 'New'
                THEN '1-New'
            WHEN req.requestType = 'Modify'
                THEN '2-Modify'
            ELSE 'ERROR'
            END AS "RequestTypeSort"
		, Round(timediff(minute,CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ,CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ),2)+1 AS "ReqResMinutes"
        , CASE
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) IS NULL
                THEN '7 No Resonse'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 4
                THEN '1 < 3 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 11
                THEN '2 <10 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 31
                THEN '3 <30 minutes'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 61
                THEN '4 <1 hour'
            WHEN (Round(timediff(minute, req.created_at, resp.created_at), 2) + 1) < 121
                THEN '5 <2 hours'
            ELSE '6 >2 hours'
            END AS BucketGroups
        , 1 AS "ReqCount"
        , CASE
            WHEN respcriteria.ID IS NOT NULL THEN 1
            ELSE 0
            END AS "CR100Flag"
        , CASE
            WHEN "blockedStatus" like '%frozen%' THEN 1
            ELSE 0
            END AS "CreditFreezeFlag"
        , CASE
            WHEN "blockedStatus" like '%Criminal generated%' THEN 1
            ELSE 0
            END AS "CriminalErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%GlobalSanction%' THEN 1
            ELSE 0
            END AS "GlobalSanctionErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%SexOffender%' THEN 1
            ELSE 0
            END AS "SexOffenderErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Credit Bureau%' THEN 1
            ELSE 0
            END AS "CreditBureauErrorFlag"
        , CASE
            WHEN "blockedStatus" IS NULL THEN 1
            ELSE 0
            END AS "BlockedStatusNullFlag"
        , CASE
            WHEN "blockedStatus" = '[""]' THEN 1
            ELSE 0
            END AS "BlockedStatusQuotesFlag"
        , CASE
            WHEN "blockedStatus" like '%This key is already associated%' THEN 1
            ELSE 0
            END AS "ThisKeyAlreadyAssociatedFlag"
        , CASE
            WHEN "blockedStatus" like '%SSN encountered a problem%' THEN 1
            ELSE 0
            END AS "SSNErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Multiple-step operation generated%' THEN 1
            ELSE 0
            END AS "MultiStepErrorFlag"
        , CASE
            WHEN "blockedStatus" like '%Cannot calculate the Guarantor Rent%' THEN 1
            ELSE 0
            END AS "CannotCalcGuarFlag"
        , CASE
            WHEN "blockedStatus" like '%Update of timefinish for status record%' THEN 1
            WHEN "blockedStatus" like '%Application was unsuccessful in 180 tries%' THEN 1
            ELSE 0
            END AS "TimeErrorFlags"
        , CASE
            WHEN "blockedStatus" like '%Out of string space%' THEN 1
            WHEN "blockedStatus" like '%StagerWorkerException%' THEN 1
            ELSE 0
            END AS "OtherErrorFlag"
                , CASE
            WHEN position('"Global Sanctions"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Criminal"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Sex Offender"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "CriminalInProcessFlag"
        , CASE
            WHEN position('"Global Sanctions Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Criminal Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Sex Offender Offline"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "CriminalOfflineProcFlag"
        , CASE
            WHEN position('"Credit"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Eviction"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"Collections"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            WHEN position('"SkipWatch"},"_":"In Process"', "responseServiceStatus"::VARCHAR, 1) > 0 THEN 1
            ELSE 0
            END AS "FinancialInProcessFlag"
        , CASE
            WHEN resp.id IS NULL
                THEN 0
			ELSE timediff(minute,CONVERT_TIMEZONE(prop.timezone, req.created_at)::TIMESTAMP_NTZ,CONVERT_TIMEZONE(prop.timezone, resp.created_at)::TIMESTAMP_NTZ)
            END AS "RequestResponseTime"
        , CASE
            WHEN "responseCriteriaResult":"116":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"701":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"702":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"806":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"849":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"851":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"852":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"853":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"901":"passFail"::VARCHAR = 'F' THEN 1
            ELSE 0
        END AS "DeniedFinancialFlag"
        , CASE
            WHEN "responseCriteriaResult":"306":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"321":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"327":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"329":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"330":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"331":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"337":"passFail"::VARCHAR = 'F' THEN 1
            WHEN "responseCriteriaResult":"501":"passFail"::VARCHAR = 'F' THEN 1
            ELSE 0
        END AS "DeniedCriminalFlag"
    FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest AS req
    INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication AS pa ON pa.id = req.partyApplicationId
    INNER JOIN {{ var("source_tenant") }}.rentapp_SubmissionResponse AS resp ON resp.submissionRequestId = req.id AND resp.STATUS = 'Complete'
    LEFT OUTER JOIN (
        SELECT ID
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
            , LATERAL flatten(input => CRITERIARESULT) fl
        WHERE fl.KEY = 100 AND fl.value::VARCHAR LIKE '%"passFail":"F"%'
        ) respcriteria ON respcriteria.ID = resp.ID
    INNER JOIN (
        SELECT *
        FROM (
            SELECT rank() OVER (
                    PARTITION BY resp0.submissionRequestId ORDER BY resp0.created_at
                    ) AS respNum
                , *
            FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse resp0
            WHERE resp0.STATUS = 'Complete' AND resp0.created_at > '2018-04-05'
            ) firstCompleteResp0
        WHERE firstCompleteResp0.respNum = 1
        ) AS firstCompleteResp ON firstCompleteResp.id = resp.id
    INNER JOIN {{ var("source_tenant") }}.Party AS p ON p.id = pa.partyId
    LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
    LEFT OUTER JOIN (
        SELECT requestId
            , sum(CASE
                    WHEN applicants.type = 'Guarantor'
                        THEN 1
                    ELSE 0
                    END) AS numGuarantors
            , sum(CASE
                    WHEN applicants.type = 'Applicant'
                        THEN 1
                    ELSE 0
                    END) AS numApplicants
        FROM (
            SELECT id AS requestId
                , REPLACE(fl.value: type, '"', '') AS type
            FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
                , LATERAL flatten(input => parse_json(applicantData) :applicants) fl
            ) AS applicants
        GROUP BY requestId
        ) AS numMembers ON numMembers.requestId = req.id
    LEFT OUTER JOIN (
        SELECT partyApplicationId
            , min(created_at) AS requestDate
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
        GROUP BY partyApplicationId
        ) AS firstReq ON firstReq.partyApplicationId = pa.id
    LEFT OUTER JOIN (
        SELECT req.id AS requestId
            , lag(quoteId) OVER (
                PARTITION BY partyApplicationId ORDER BY created_at
                ) AS previousQuoteId
            , ARRAY_SIZE(lag(applicantData: applicants) OVER (
                    PARTITION BY partyApplicationId ORDER BY created_at
                    )) AS previousApplicants
            , CASE
                WHEN lag(req.quoteId) OVER (
                        PARTITION BY req.partyApplicationId ORDER BY req.created_at
                        ) <> req.quoteId
                    THEN 1
                WHEN (
                        lag(req.quoteId) OVER (
                            PARTITION BY req.partyApplicationId ORDER BY req.created_at
                            ) IS NULL AND req.quoteId IS NOT NULL
                        )
                    THEN CASE
                            WHEN lag(req.id) OVER (
                                    PARTITION BY partyApplicationId ORDER BY req.created_at
                                    ) IS NULL
                                THEN 0
                            ELSE 1
                            END
                ELSE 0
                END AS quoteChanged
            , CASE
                WHEN ARRAY_SIZE(req.applicantData: applicants) <> ARRAY_SIZE(lag(req.applicantData: applicants) OVER (
                            PARTITION BY req.partyApplicationId ORDER BY req.created_at
                            ))
                    THEN 1
                ELSE 0
                END AS numApplicantsChanged
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest req
        ) AS reqChanged ON reqChanged.requestId = req.id
    LEFT OUTER JOIN (
        SELECT partyId
            , STATUS
            , rank() OVER (
                PARTITION BY partyId ORDER BY created_at DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.Lease
        ) AS l ON l.partyId = p.id AND l.theRank = 1
    WHERE req.created_at > dateadd(MONTH, - 14, CURRENT_TIMESTAMP())
    ) AS combineUnions
