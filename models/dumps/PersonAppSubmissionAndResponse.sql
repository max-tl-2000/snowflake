/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.PersonAppSubmissionAndResponse --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.PersonAppSubmissionAndResponse --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.PersonAppSubmissionAndResponse --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='PersonAppSubmissionAndResponse') }}

-- depends on: {{ ref('PartyDump') }}

WITH w_rentapp_SubmissionResponse
AS (
    SELECT id
        , REPLACE(parse_json(REPLACE(fl.value::VARCHAR, '$', 'root')) :root :applicantid, '"', '') AS FADVapplicantId
    FROM {{ var("source_tenant") }}.RENTAPP_SUBMISSIONRESPONSE
        , LATERAL flatten(input => parse_json(rawResponse) :ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :ApplicationInformation [0] :ApplicantInformation) AS fl
    )
SELECT perApp.id AS "personAppId"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',perApp.created_at)::TIMESTAMP_NTZ AS "appCreated"
    , perApp.partyId AS "partyId"
    , perApp.personId AS "personId"
    , perApp.applicationStatus AS "applicationStatus"
    , pm.memberType AS "memberType"
    , COALESCE(perApp.applicationData: dateOfBirth, 'DBNULL') AS "dateOfBirth"
    , COALESCE((perApp.applicationData: grossIncomeMonthly::NUMERIC)::VARCHAR, 'DBNULL') AS "grossIncomeMonthly"
    , COALESCE((l.baselineData: publishedLease: unitRent::NUMERIC)::VARCHAR, 0 )::numeric AS "leaseBaseRent"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(sreq.created_at, '07/04/1776'))::TIMESTAMP_NTZ AS "appSubmissionSent"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(sres.created_at, '07/04/1776'))::TIMESTAMP_NTZ AS "appRequestReceived"
    , COALESCE(sres.applicationDecision, 'DBNULL') AS "applicationDecision"
    , COALESCE(prop.name, 'DBNULL') AS "appSubmissionProperty"
    , COALESCE(sreq.requestType, 'NULL') AS "requestType"
    , sreq.id AS "requestId"
    , sres.id AS "responseId"
    , s.FADVapplicantId AS "FADVapplicantId"
    , p.partyGroupId AS "partyGroupId"
    , pd."Property" AS "Property"
    , CASE
        WHEN cs.creditScore = 'No Credit File'
            THEN 0
        ELSE cs.creditScore
        END::INT AS "creditScore"
    , CASE
        WHEN sres.applicationDecision = 'approved'
            THEN 'Approved'
        WHEN sres.applicationDecision = 'Guarantor Required'
            THEN 'Conditional'
        WHEN sres.applicationDecision = 'approved_with_cond'
            THEN 'Conditional'
        WHEN sres.applicationDecision = 'declined'
            THEN 'Declined'
        ELSE sres.applicationDecision
        END AS "applicationDecisionClean"
FROM {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS partyApp ON partyApp.id = perApp.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = perApp.partyId
INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER AS pm ON pm.endDate IS NULL AND pm.personId = perApp.personId AND pm.partyId = perApp.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.LEASE AS l ON l.partyId = perApp.partyId AND l.STATUS = 'executed'
LEFT OUTER JOIN {{ var("source_tenant") }}.RENTAPP_SUBMISSIONREQUEST AS sreq ON sreq.partyApplicationId = partyApp.id
LEFT OUTER JOIN {{ var("source_tenant") }}.RENTAPP_SUBMISSIONRESPONSE AS sres ON sres.submissionRequestId = sreq.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = sreq.propertyId
INNER JOIN w_rentapp_SubmissionResponse AS s ON s.id = sres.id
LEFT OUTER JOIN {{ var("target_schema") }}."PartyDump" AS pd ON pd."partyIdNoURL" = perApp.partyId
LEFT OUTER JOIN (
    SELECT req.personId
        , req.requestId
        , resp.responseId
        , resp.creditScore AS creditScore
        , req.partyApplicationId
        , row_number() OVER (
            PARTITION BY req.partyApplicationId
            , req.personId ORDER BY req.created_at DESC
            ) AS theRank
        , row_number() OVER (
            PARTITION BY resp.responseId
            , req.personId ORDER BY resp.created_at DESC
            ) AS reqRespRank
    FROM (
        SELECT id AS requestId
            , REPLACE(fl.value: personId, '"', '') AS personId
            , REPLACE(fl.value: firstName, '"', '') AS firstName
            , REPLACE(fl.value: middleName, '"', '') AS middleName
            , REPLACE(fl.value: lastName, '"', '') AS lastName
            , REPLACE(fl.value: suffix, '"', '') AS suffix
            , partyApplicationId
            , created_at
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
            , LATERAL flatten(input => parse_json(applicantData: applicants)) AS fl
        ) AS req
    INNER JOIN (
        SELECT id AS responseId
            , created_at
            , submissionRequestId
            , TRANSLATE(fl.value: CreditScore, '"[]', '') AS creditScore
            , TRANSLATE(fl.value: ApplicantName, '"[]', '') AS applicantName
            , row_number() OVER (
                PARTITION BY submissionRequestId ORDER BY created_at DESC
                ) AS theRank
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
            , LATERAL flatten(input => parse_json(rawResponse) :ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :RentToIncomes [0] :Applicant) AS fl
        ) AS resp ON resp.submissionRequestId = req.requestId
        AND REPLACE(resp.applicantName, ' ', '') = REPLACE(COALESCE(req.firstName, '') || ' ' || COALESCE(req.middleName, '') || ' ' || COALESCE(req.lastName, '') || ' ' || COALESCE(req.suffix, ''), ' ', '')
    ) AS cs ON cs.personId = perApp.personId AND cs.responseId = sres.id
