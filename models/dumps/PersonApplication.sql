/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.PersonApplication --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.PersonApplication --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.PersonApplication --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='PersonApplication') }}

SELECT perApp.id AS "personAppId"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',perApp.created_at)::TIMESTAMP_NTZ AS "created_at"
    , perApp.partyId AS "partyId"
    , perApp.personId AS "personId"
    , perApp.applicationStatus AS "applicationStatus"
    , pm.memberType AS "memberType"
    , REPLACE(perApp.applicationData: dateOfBirth, '"', '') AS "dateOfBirth"
    , perApp.applicationData: grossIncomeMonthly::number AS "grossIncomeMonthly"
    , l.baselineData:publishedLease: unitRent AS "leaseBaseRent"
    , COALESCE(perApp.additionalData: incomeSourceHistory [0] :jobTitle, '') AS "jobTitle"
    , COALESCE(perApp.additionalData: incomeSourceHistory [0] :employerName, '') AS "employerName"
    , COALESCE(disclosures.hasConviction, 0) AS "hasDisclosedConviction"
    , COALESCE(disclosures.convictionName, '') AS "convictionName"
    , COALESCE(disclosures.convictionDisplayName, '') AS "convictionDisplayName"
    , COALESCE(disclosures.hasEviction, 0) AS "hasDisclosedEviction"
    , COALESCE(disclosures.evictionName, '') AS "evictionName"
    , COALESCE(disclosures.evictionDisplayName, '') AS "evictionDisplayName"
    , COALESCE(personalDocs.numDocs, 0) AS "numPersonalDocs"
    , COALESCE(partyDocs.numDocs, 0) AS "numPartyDocs"
    , perApp.partyApplicationId AS "partyApplicationId"
    , p.partyGroupId AS "partyGroupId"
    , REPLACE(perApp.applicationData: address: enteredByUser: postalCode, '"', '') AS "postalCode"
    , ps.fullname AS "personFullName"
    , CASE
        WHEN cs.creditScore = 'No Credit File'
            THEN 0
        WHEN cs.creditScore = ''
            THEN 0
        ELSE cs.creditScore::INT
        END AS "creditScore"
    , CASE
        WHEN (
                CASE
                    WHEN cs.creditScore = 'No Credit File'
                        THEN 0
                    WHEN cs.creditScore = ''
                        THEN 0
                    ELSE cs.creditScore::INT
                    END
                ) > 649
            THEN 1
        WHEN (
                CASE
                    WHEN cs.creditScore = 'No Credit File'
                        THEN 0
                    WHEN cs.creditScore = ''
                        THEN 0
                    ELSE cs.creditScore::INT
                    END
                ) > 549 OR (
                CASE
                    WHEN cs.creditScore = 'No Credit File'
                        THEN 0
                    WHEN cs.creditScore = ''
                        THEN 0
                    ELSE cs.creditScore::INT
                    END
                ) < 650
            THEN 0.75
        WHEN (
                CASE
                    WHEN cs.creditScore = 'No Credit File'
                        THEN 0
                    WHEN cs.creditScore = ''
                        THEN 0
                    ELSE cs.creditScore::INT
                    END
                ) < 550
            THEN 0.50
        END AS "creditScoreFactor"
    , CASE
        WHEN REPLACE(perApp.applicationData: dateOfBirth, '"', '') in ('', 'Invalid date')
            THEN 0
        ELSE datediff('year', REPLACE(perApp.applicationData: dateOfBirth, '"', ''), CURRENT_TIMESTAMP())
        END AS "Age"
FROM {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = perApp.partyId
INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER AS pm ON pm.endDate IS NULL AND pm.personId = perApp.personId AND pm.partyId = perApp.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.LEASE AS l ON l.partyId = perApp.partyId AND l.STATUS = 'executed'
LEFT OUTER JOIN (
    SELECT perApp.id AS perAppId
        , MAX(CASE
                WHEN lower(disc.name) = 'conviction'
                    THEN 1
                ELSE 0
                END) AS hasConviction
        , CASE
            WHEN MAX(CASE
                        WHEN lower(disc.name) = 'conviction'
                            THEN 1
                        ELSE 0
                        END) = 1
                THEN 'conviction'
            ELSE ''
            END AS convictionName
        , CASE
            WHEN MAX(CASE
                        WHEN lower(disc.name) = 'conviction'
                            THEN 1
                        ELSE 0
                        END) = 1
                THEN MAX(disc.displayName)
            ELSE ''
            END AS convictionDisplayName
        , MAX(CASE
                WHEN lower(disc.name) = 'eviction'
                    THEN 1
                ELSE 0
                END) AS hasEviction
        , CASE
            WHEN MAX(CASE
                        WHEN lower(disc.name) = 'eviction'
                            THEN 1
                        ELSE 0
                        END) = 1
                THEN 'eviction'
            ELSE ''
            END AS evictionName
        , CASE
            WHEN MAX(CASE
                        WHEN lower(disc.name) = 'eviction'
                            THEN 1
                        ELSE 0
                        END) = 1
                THEN MAX(disc.displayName)
            ELSE ''
            END AS evictionDisplayName
    FROM (
        SELECT id
            , partyApplicationId
            , fl.KEY::VARCHAR AS keys
            , personId
        FROM {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION
            , LATERAL flatten(input => parse_json(additionalData: disclosures)) AS fl
        ) AS perApp
    INNER JOIN {{ var("source_tenant") }}.DISCLOSURE AS disc ON disc.id::TEXT = perApp.keys
    WHERE lower(disc.name) IN ('conviction', 'eviction')
    GROUP BY perApp.id
    ) AS disclosures ON disclosures.perAppId = perApp.id
LEFT OUTER JOIN (
    SELECT personApplicationId
        , count(*) AS numDocs
    FROM {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATIONDOCUMENTS
    GROUP BY personApplicationId
    ) AS personalDocs ON personalDocs.personApplicationId = perApp.id
LEFT OUTER JOIN (
    SELECT partyApplicationId
        , count(*) AS numDocs
    FROM {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATIONDOCUMENTS
    GROUP BY partyApplicationId
    ) AS partyDocs ON partyDocs.partyApplicationId = perApp.partyApplicationId
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS ps ON ps.ID = perApp.personId
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
        ) req
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
    ) AS cs ON cs.personId = perApp.personId AND cs.partyApplicationId = perApp.partyApplicationId AND cs.theRank = 1
WHERE p.workflowName IN ('newLease', 'renewal')
