/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.MostRecentReqResp --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.MostRecentReqResp --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.MostRecentReqResp --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='MostRecentReqResp') }}

-- depends on: {{ ref('ReqRespDetails') }}


SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || parApp.partyId AS "partyId"
    , prop.name AS "property"
    , req.partyApplicationId AS "partyApplicationId"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',req.created_at)::TIMESTAMP_NTZ AS "submissionTime"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',resp.responseTime)::TIMESTAMP_NTZ AS "responseTime"
    , CASE
        WHEN div0((TIMEDIFF(second, req.created_at, resp.responseTime) - MOD(TIMEDIFF(second, req.created_at, resp.responseTime), 86400)), (60 * 60 * 24)) > 0
            THEN (div0((TIMEDIFF(second, req.created_at, resp.responseTime) - MOD(TIMEDIFF(second, req.created_at, resp.responseTime), 86400)), (60 * 60 * 24))::INT)::VARCHAR || ' days'
        ELSE ''
        END::VARCHAR || ' ' || CAST(to_time(CAST(MOD(TIMEDIFF(second, req.created_at, resp.responseTime), 86400) AS VARCHAR)) AS VARCHAR) AS "duration"
    , CASE
        WHEN TIMEDIFF(SECOND, req.created_at, resp.responseTime) < 60
            THEN 1
        ELSE round(TIMEDIFF(second, req.created_at, resp.responseTime) / 60)
        END AS "durationMinutes"
    , resp.STATUS AS "applicationStatus"
    , resp.applicationDecision AS "applicationDecision"
    , rank() OVER (
        PARTITION BY req.partyApplicationId ORDER BY req.created_at DESC
        ) AS "theRank"
    , req.transactionNumber AS "transactionNumber"
    , req.applicantCount AS "applicantCount"
    , req.guarantorCount AS "guarantorCount"
    , req.partySize AS "partySize"
    , CASE
        WHEN TIMEDIFF(second, req.created_at, resp.responseTime) <= 1 * 60
            THEN '< 1 min'
        WHEN TIMEDIFF(second, req.created_at, resp.responseTime) <= 2 * 60
            THEN '< 2 min'
        WHEN TIMEDIFF(second, req.created_at, resp.responseTime) <= 3 * 60
            THEN '< 3 min'
        WHEN TIMEDIFF(second, req.created_at, resp.responseTime) <= 10 * 60
            THEN '< 10 min'
        WHEN TIMEDIFF(MINUTE, req.created_at, resp.responseTime) < 60
            THEN '< 1 hour'
        WHEN TIMEDIFF(MINUTE, req.created_at, resp.responseTime) < 2 * 60
            THEN '< 2 hour'
        WHEN TIMEDIFF(MINUTE, req.created_at, resp.responseTime) < 14 * 60
            THEN '< 14 hour'
        WHEN TIMEDIFF(hour, req.created_at, resp.responseTime) < 24
            THEN '< 1 day'
        ELSE '> 1 day'
        END AS "bucket2"
    , COALESCE(pqp.promotionStatus, 'unpromoted') AS "promotionStatus"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',pqp.created_at)::TIMESTAMP_NTZ AS "quotePromotionTime"
    , CASE
        WHEN pqp.created_at > resp.responseTime
            THEN 1
        ELSE 0
        END AS "promotionAfterResult"
    , req.id AS "requestId"
    , resp.id AS "responseId"
    , resp.creditScore AS "creditScore"
    , CASE
        WHEN crim."CriminalInProcessFlag" > 0
        THEN 1
        ELSE 0
        END AS "CriminalInProcessFlag"
    , crim."CriminalInProcessFlag" AS "CrimTest"
    , CASE
        WHEN resp.applicationDecision = 'approved'
            THEN 'Approved'
        WHEN resp.applicationDecision = 'approved_with_cond'
            THEN 'Conditional Approval'
        WHEN resp.applicationDecision = 'declined'
            THEN 'Declined'
        WHEN resp.applicationDecision = 'further_review'
            THEN 'Further Review'
        WHEN resp.applicationDecision = 'Guarantor Required'
            THEN 'Guarantor Required'
        ELSE resp.applicationDecision
        END AS "ApplicationDecisionClean"
    , crim."DeniedFinancialFlag"
    , crim."DeniedCriminalFlag"
FROM (
    SELECT *
        , regexp_count(applicantData::TEXT, '\\bApplicant\\b', 1) AS applicantCount
        , regexp_count(applicantData::TEXT, '\\bGuarantor\\b', 1) AS guarantorCount
        , regexp_count(applicantData::TEXT, '\\bApplicant\\b', 1) + regexp_count(applicantData::TEXT, '\\bGuarantor\\b', 1) AS partySize
        , COALESCE(lag(regexp_count(applicantData::TEXT, '\\bApplicant\\b', 1) + regexp_count(applicantData::TEXT, '\\bGuarantor\\b', 1)) OVER (
                PARTITION BY partyApplicationId ORDER BY created_at DESC
                ), - 1) AS prevPartySize
    FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
    ) req
INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication parApp ON parApp.id = req.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.Property prop ON prop.id = req.propertyId
LEFT OUTER JOIN (
    SELECT resp0.id
        , resp0.created_at AS responseTime
        , resp0.submissionRequestId
        , resp0.STATUS
        , resp0.applicationDecision
        , REPLACE((parse_json(resp0.rawResponse)) :ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :RentToIncomes [0] :Applicant [0] :CreditScore [0], '"', '') AS creditScore
    FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse resp0
    INNER JOIN (
        SELECT id
            , rank() OVER (
                PARTITION BY submissionRequestId ORDER BY created_at DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
        ) mostRecent ON mostRecent.id = resp0.id AND mostRecent.theRank = 1
    ) resp ON resp.submissionRequestId = req.id
LEFT OUTER JOIN (
    SELECT *
    FROM (
        SELECT rank() OVER (
                PARTITION BY quoteId ORDER BY created_at DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.PartyQuotePromotions
        ) mostRecent
    WHERE mostRecent.theRank = 1
    ) pqp ON pqp.partyId = parApp.partyId AND pqp.quoteId = req.quoteId
LEFT JOIN (
    SELECT "partyAppId"
        , SUM("DeniedFinancialFlag") AS "DeniedFinancialFlag"
        , SUM("DeniedCriminalFlag") AS "DeniedCriminalFlag"
        , SUM("CriminalInProcessFlag") AS "CriminalInProcessFlag"
    FROM {{ var("target_schema") }}."ReqRespDetails"
    GROUP BY "partyAppId"
    ) AS crim ON crim."partyAppId" = req.partyApplicationId
WHERE req.created_at > dateadd(MONTH, - 14, CURRENT_DATE ()) AND req.prevPartySize <> req.partySize
