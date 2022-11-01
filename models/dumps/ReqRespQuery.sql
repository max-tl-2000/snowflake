/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ReqRespQuery --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ReqRespQuery --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ReqRespQuery --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ReqRespQuery') }}

-- depends on: {{ ref('ReqRespDetails') }}

WITH SubmissionRequestWithStructureChange
AS (
    SELECT partyApplicationId::TEXT || '__' || applicantCount || '__' || guarantorCount AS applicationPartyChange
        , *
    FROM (
        SELECT created_at
            , id
            , partyApplicationId
            , propertyId
            , quoteId
            , LENGTH(REPLACE(applicantData::TEXT, '"Applicant"', '~')) - LENGTH(REPLACE(applicantData::TEXT, '"Applicant"', '')) AS applicantCount
            , LENGTH(REPLACE(applicantData::TEXT, '"Guarantor"', '~')) - LENGTH(REPLACE(applicantData::TEXT, '"Guarantor"', '')) AS guarantorCount
            , transactionNumber
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
        ) AS t
    )
    , QuoteCount
AS (
    SELECT partyId
        , count(*) AS quoteCount
    FROM {{ var("source_tenant") }}.Quote
    WHERE publishDate IS NOT NULL
    GROUP BY partyId
    )
    , SubmissionRequest
AS (
    WITH submissionsWihoutQuotes AS (
            SELECT *
            FROM SubmissionRequestWithStructureChange
            WHERE quoteId IS NOT NULL AND transactionNumber IS NOT NULL
            )
    SELECT t1.created_at AS submissionTime
        , t1.id
        , t1.partyApplicationId
        , t1.propertyId
        , t1.applicantCount
        , t1.guarantorCount
        , t1.applicationPartyChange
        , t1.transactionNumber
    FROM submissionsWihoutQuotes AS t1
    LEFT OUTER JOIN submissionsWihoutQuotes AS t2 ON (t1.applicationPartyChange = t2.applicationPartyChange AND t1.created_at > t2.created_at AND t1.quoteId IS NOT NULL)
    WHERE t2.applicationPartyChange IS NULL
    )
    , SubmissionResponse
AS (
    SELECT resp0.created_at AS responseTime
        , resp0.submissionRequestId
        , resp0.STATUS
        , resp0.applicationDecision
    FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse AS resp0
    INNER JOIN (
        SELECT id
            , rank() OVER (
                PARTITION BY submissionRequestId ORDER BY created_at DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
        ) AS mostRecent ON mostRecent.id = resp0.id AND mostRecent.theRank = 1
    )
    , QuotePromotion
AS (
    SELECT t1.created_at AS quotePromotionTime
        , t1.partyId
        , t1.promotionStatus
    FROM {{ var("source_tenant") }}.PartyQuotePromotions AS t1
    LEFT OUTER JOIN {{ var("source_tenant") }}.PartyQuotePromotions AS t2 ON (t1.partyId = t2.partyId AND t1.created_at > t2.created_at)
    WHERE t2.partyId IS NULL
    )
    , FirstSubmissionRequest
AS (
    SELECT *
    FROM (
        WITH submissionsWihoutQuotes AS (
                SELECT *
                FROM SubmissionRequestWithStructureChange
                WHERE quoteId IS NOT NULL
                )
        SELECT t1.created_at AS firstSubmissionTime
            , t1.id
            , t1.partyApplicationId
            , t1.propertyId
            , t1.transactionNumber
            , t1.applicationPartyChange
        FROM submissionsWihoutQuotes AS t1
        LEFT OUTER JOIN submissionsWihoutQuotes AS t2 ON (t1.applicationPartyChange = t2.applicationPartyChange AND t1.created_at > t2.created_at AND t1.quoteId IS NOT NULL)
        WHERE t2.applicationPartyChange IS NULL
        ) AS t
    WHERE transactionNumber IS NULL
    )
SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || pa.partyId AS "party"
    , p.name AS "property"
    , applicationPartyChange AS "applicationPartyChange"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',submissionTime)::TIMESTAMP_NTZ AS "submissionTime"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',responseTime)::TIMESTAMP_NTZ AS "responseTime"
    , CASE
        WHEN div0((TIMEDIFF(second, submissionTime, responseTime) - MOD(TIMEDIFF(second, submissionTime, responseTime), 86400)), (60 * 60 * 24)) > 0
            THEN (div0((TIMEDIFF(second, submissionTime, responseTime) - MOD(TIMEDIFF(second, submissionTime, responseTime), 86400)), (60 * 60 * 24))::INT)::VARCHAR || ' days'
        ELSE ''
      END::VARCHAR || ' ' || CAST(to_time(CAST(MOD(TIMEDIFF(second, submissionTime, responseTime), 86400) AS VARCHAR)) AS VARCHAR) AS "duration"
    , lpad(date_part(HOUR, to_time(CAST(MOD(TIMEDIFF(second, submissionTime, responseTime), 86400) AS VARCHAR)))::VARCHAR, 2, '0') || ':' || lpad(date_part(minute, to_time(CAST(MOD(TIMEDIFF(second, submissionTime, responseTime), 86400) AS VARCHAR)))::VARCHAR, 2, '0') AS "bucket"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',firstAndFailedSubmissionTime)::TIMESTAMP_NTZ AS "firstAndFailedSubmissionTime"
    , firstAndFailedSubmissionDecision AS "firstAndFailedSubmissionDecision"
    , CASE
        WHEN responseTime < firstAndFailedSubmissionTime
            THEN NULL
        ELSE datediff(DAY, firstAndFailedSubmissionTime, responseTime)::VARCHAR || ' days ' || CAST(to_time(CAST(MOD(TIMEDIFF(second, firstAndFailedSubmissionTime, responseTime), 86400) AS VARCHAR)) AS VARCHAR)
        END AS "fromFirstToCompleteResponse"
    , CASE
        WHEN TIMEDIFF(second, submissionTime, responseTime) <= 1 * 60
            THEN '< 1 min'
        WHEN TIMEDIFF(second, submissionTime, responseTime) <= 2 * 60
            THEN '< 2 min'
        WHEN TIMEDIFF(second, submissionTime, responseTime) <= 3 * 60
            THEN '< 3 min'
        WHEN TIMEDIFF(second, submissionTime, responseTime) <= 10 * 60
            THEN '< 10 min'
        WHEN TIMEDIFF(minute, submissionTime, responseTime) < 60
            THEN '< 1 hour'
        WHEN TIMEDIFF(minute, submissionTime, responseTime) < 2 * 60
            THEN '< 02 hour'
        WHEN TIMEDIFF(minute, submissionTime, responseTime) < 14 * 60
            THEN '< 14 hour'
        WHEN TIMEDIFF(hour, submissionTime, responseTime) < 24
            THEN '< 1 day'
        ELSE '> 1 day'
        END AS "bucket2"
    , applicationDecision AS "applicationDecision"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',quotePromotionTime)::TIMESTAMP_NTZ AS "quotePromotionTime"
    , promotionStatus AS "promotionStatus"
    , quotePromotionTime > responseTime AS "promotionAfterResult"
    , req.transactionNumber AS "transactionNumber"
    , quoteCount AS "quoteCount"
    , req.applicantCount AS "applicantCount"
    , req.guarantorCount AS "guarantorCount"
    , req.partyApplicationId AS "partyApplicationId"
    , (TIMEDIFF(second, submissionTime, responseTime))/60::DECIMAL(10,3) AS "DurationCalc"
    , CASE WHEN "DurationCalc" < 0.5 THEN 1 ELSE "DurationCalc" END AS "DurationCMin"
    , "DurationCMin"/60::DECIMAL(10,2) AS "DurationCHours"
    , CASE
        WHEN "DurationCMin" < 3 THEN '1 - < 3 minutes'
        WHEN "DurationCMin" < 11 THEN '2 - < 10 minutes'
        WHEN "DurationCMin" < 31 THEN '3 - < 30 minutes'
        WHEN "DurationCMin" < 61 THEN '4 - < 1 hour'
        WHEN "DurationCMin" < 121 THEN '5 - < 2 hours'
        ELSE '6 - > 2 hours'
      END AS "Duration Group"
    , "applicantCount" + "guarantorCount" AS "Party Size"
    , CASE WHEN "firstAndFailedSubmissionTime" IS NULL THEN 0 ELSE 1 END AS "IsFirstSuccessful"
    , CASE WHEN "firstAndFailedSubmissionTime" IS NULL THEN TIMEDIFF(second, submissionTime, responseTime)/60::DECIMAL(10,3) ELSE TIMEDIFF(minute, firstAndFailedSubmissionTime, responseTime)/60::DECIMAL(10,3) END "DurationCalcFF"
    , CASE WHEN "DurationCalcFF" < 0.5 THEN 1 ELSE "DurationCalcFF" END AS "DurationCalcFFMin"
    , "DurationCalcFF"/60::DECIMAL(10,2) AS "DurationCalcFFHours"
    , CASE
        WHEN "DurationCalcFFMin" < 3 THEN '1 - < 3 minutes'
        WHEN "DurationCalcFFMin" < 11 THEN '2 - < 10 minutes'
        WHEN "DurationCalcFFMin" < 31 THEN '3 - < 30 minutes'
        WHEN "DurationCalcFFMin" < 61 THEN '4 - < 1 hour'
        WHEN "DurationCalcFFMin" < 121 THEN '5 - < 2 hours'
        ELSE '6 - > 2 hours'
      END AS "Duration GroupFF"
    , CASE WHEN "firstAndFailedSubmissionDecision" IS NULL THEN '' ELSE "firstAndFailedSubmissionDecision" END AS "First Failed Decision"
    , 1 AS "RecCount"
    , CASE WHEN "DurationCMin" < 3 THEN 1 ELSE 0 END AS "0-2"
    , CASE WHEN "DurationCMin" BETWEEN 3 AND 10 THEN 1 ELSE 0 END AS "3-10"
    , CASE WHEN "DurationCMin" BETWEEN 11 AND 30 THEN 1 ELSE 0 END AS "11-30"
    , CASE WHEN "DurationCMin" BETWEEN 31 AND 60 THEN 1 ELSE 0 END AS "31-1Hr"
    , CASE WHEN "DurationCMin" BETWEEN 61 AND 120 THEN 1 ELSE 0 END AS "1-2Hr"
    , CASE WHEN "DurationCMin" > 120 THEN 1 ELSE 0 END AS "GT2Hr"
    , CASE WHEN crim."CriminalInProcessFlag" > 0 THEN 1 ELSE 0 END AS "CriminalInProcessFlag"
    , CASE WHEN crim."CriminalOfflineProcFlag" > 0 THEN 1 ELSE 0 END AS "CriminalOfflineProcessFlag"
    , CASE WHEN crim."CreditFreezeFlag" > 0 THEN 1 ELSE 0 END AS "CreditFreezeFlag"
FROM SubmissionRequest AS req
LEFT JOIN SubmissionResponse AS resp ON req.id = resp.submissionRequestId
INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication AS pa ON pa.id = req.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.Property AS p ON req.propertyId = p.id
INNER JOIN QuoteCount AS qc ON qc.partyId = pa.partyId
LEFT JOIN QuotePromotion AS qp ON qp.partyId = pa.partyId
LEFT JOIN (
    SELECT id
        , fsr.firstSubmissionTime AS firstAndFailedSubmissionTime
        , fsr.partyApplicationId
        , resp.applicationDecision AS firstAndFailedSubmissionDecision
    FROM FirstSubmissionRequest AS fsr
    LEFT JOIN SubmissionResponse AS resp ON fsr.id = resp.submissionRequestId
    ) AS fst ON fst.partyApplicationId = req.partyApplicationId
LEFT JOIN (
    SELECT "partyAppId"
        , SUM("CreditFreezeFlag") AS "CreditFreezeFlag"
        , SUM("CriminalOfflineProcFlag") AS "CriminalOfflineProcFlag"
        , SUM("CriminalInProcessFlag") AS "CriminalInProcessFlag"
    FROM {{ var("target_schema") }}."ReqRespDetails"
    GROUP BY "partyAppId"
    ) AS crim ON crim."partyAppId" = req.partyApplicationId
WHERE resp.STATUS = 'Complete' AND submissionTime > '2018-04-05'
