/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ExecutedLeaseCompliance --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ExecutedLeaseCompliance --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ExecutedLeaseCompliance --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ExecutedLeaseCompliance') }}

SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || l.partyId::TEXT AS "partyId"
    , l.STATUS AS "leaseStatus"
    , COALESCE(resp.applicationDecision, '[No Completed Application]') AS "applicationDecision"
    , CASE
        WHEN hasGuarantor.partyId IS NULL
            THEN 0
        ELSE 1
        END AS "hasGuarantor"
    , COALESCE(resp.allRecs, '[No System Recommendations]') AS "Recommendations"
    , COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0') AS "leaseUnitDeposit"
    , COALESCE(quoteDep.fees_amount, '0') AS "quoteUnitDeposit"
    , COALESCE(q.leaseTerms: originalBaseRent, '-1') AS "originalBaseRent"
    , (COALESCE(q.leaseTerms: overwrittenBaseRent, '-1')::NUMERIC)::VARCHAR AS "overwrittenBaseRent"
    , CASE
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'approved'
            THEN 0
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = '[No Completed Application]'
            THEN 1
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'declined'
            THEN 1
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'Guarantor Required'
            THEN CASE
                    WHEN hasGuarantor.partyId IS NULL
                        THEN 1
                    ELSE 0
                    END
        WHEN COALESCE(resp.applicationDecision, '[No Application]') = 'approved_with_cond'
            THEN /*additional deposit*/ CASE
                    WHEN COALESCE(resp.allRecs, '[No System Recommendations]') LIKE '%additional deposit%'
                        THEN CASE
                                WHEN COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')::NUMERIC > COALESCE(quoteDep.fees_amount, '0')::NUMERIC
                                    THEN 0
                                ELSE 1
                                END /*guarantor required - not seen in system with current config*/
                    WHEN COALESCE(resp.allRecs, '[No System Recommendations]') LIKE '%uarantor%'
                        THEN CASE
                                WHEN hasGuarantor.partyId IS NULL
                                    THEN 1
                                ELSE 0
                                END
                    ELSE 0
                    END
        WHEN COALESCE(resp.applicationDecision, '[No Application]') = 'further_review'
            THEN /*additional deposit*/ CASE
                    WHEN COALESCE(resp.allRecs, '[No System Recommendations]') LIKE '%additional deposit%'
                        THEN CASE
                                WHEN COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')::NUMERIC > COALESCE(quoteDep.fees_amount, '0')::NUMERIC
                                    THEN 0
                                ELSE 1
                                END /*guarantor required - not seen in system with current config*/
                    WHEN COALESCE(resp.allRecs, '[No System Recommendations]') LIKE '%uarantor%'
                        THEN CASE
                                WHEN hasGuarantor.partyId IS NULL
                                    THEN 1
                                ELSE 0
                                END
                    ELSE 0
                    END
        ELSE 0
        END AS "isNonCompliant"
    , COALESCE(approver.approvingAgentName, '') AS "approvingAgentName"
    , COALESCE(l.baselineData: additionalConditions: additionalNotes, '') AS "approverNotes"
    , CONVERT_TIMEZONE(prop.timezone, l.signDate)::TIMESTAMP_NTZ AS "signDate"
    , l.id AS "leaseId"
    , q.id AS "quoteId"
    ,CASE
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'approved'
            THEN 'Approved'
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'approved_with_cond'
            THEN 'Conditional Approval'
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'declined'
            THEN 'Declined'
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'further_review'
            THEN 'Further Review'
        WHEN COALESCE(resp.applicationDecision, '[No Completed Application]') = 'Guarantor Required'
            THEN 'Conditional Approval'
        ELSE COALESCE(resp.applicationDecision, '[No Completed Application]')
        END AS "ApplicationDecisionClean"
FROM {{ var("source_tenant") }}.LEASE l
INNER JOIN (
    SELECT fl.value AS leaseTerms
        , publishedQuoteData: leaseTerms [fl.index] :termLength::VARCHAR AS termLength
        , q0.ID
        , q0.INVENTORYID
    FROM {{ var("source_tenant") }}.QUOTE AS q0
    INNER JOIN {{ var("source_tenant") }}.LEASE AS l0 ON l0.quoteId = q0.ID
        , LATERAL flatten(input => q0.publishedQuoteData: leaseTerms) AS fl
    WHERE l0.STATUS = 'executed'
    ) q ON q.id = l.quoteId
INNER JOIN (
    SELECT *
    FROM (
        SELECT rank() OVER (
                PARTITION BY quoteId ORDER BY created_at DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.PARTYQUOTEPROMOTIONS
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) pq ON pq.quoteId = q.id
INNER JOIN {{ var("source_tenant") }}.LEASETERM AS lt ON lt.id = pq.leaseTermId AND lt.termLength = q.termLength
INNER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id = q.inventoryId
INNER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = i.propertyId
LEFT OUTER JOIN (
    SELECT u.fullName AS approvingAgentName
        , rank() OVER (
            PARTITION BY t.partyId ORDER BY t.created_at DESC
            ) AS theRank
        , REPLACE(fl.value, '"', '') AS promotedQuoteId
    FROM {{ var("source_tenant") }}.TASKS AS t
    INNER JOIN {{ var("source_tenant") }}."USERS" AS u ON u.id::TEXT = (t.metadata: completedBy)
        , LATERAL flatten(input => t.metadata: quotePromotions) AS fl
    WHERE t.name = 'REVIEW_APPLICATION' AND t.category = 'Application approval'
    ) AS approver ON approver.promotedQuoteId = pq.id::TEXT AND approver.theRank = 1
LEFT OUTER JOIN (
    SELECT rank() OVER (
            PARTITION BY req0.quoteId ORDER BY req0.created_at
                , id DESC
            ) AS theRank
        , *
    FROM {{ var("source_tenant") }}.RENTAPP_SUBMISSIONREQUEST req0
    ) req ON req.quoteId = q.id AND req.theRank = 1
LEFT OUTER JOIN (
    SELECT rank() OVER (
            PARTITION BY resp0.submissionRequestId ORDER BY resp0.created_at
                , id DESC
            ) AS theRank
        , *
    FROM {{ var("source_tenant") }}.RENTAPP_SUBMISSIONRESPONSE resp0
    LEFT OUTER JOIN (
        SELECT id AS responseId
            , listagg(unnested.recs: TEXT, ' | ') WITHIN
        GROUP (
                ORDER BY unnested.recs
                ) AS allRecs
        FROM (
            SELECT DISTINCT id
                , fl.value AS recs
            FROM {{ var("source_tenant") }}.RENTAPP_SUBMISSIONRESPONSE
                , LATERAL flatten(input => recommendations) AS fl
            ) unnested
        GROUP BY id
        ) recs ON recs.responseId = resp0.id
    WHERE resp0.STATUS = 'Complete'
    ) resp ON resp.submissionRequestId = req.id AND resp.theRank = 1
LEFT OUTER JOIN (
    SELECT DISTINCT partyId
    FROM {{ var("source_tenant") }}.PARTYMEMBER AS pm
    WHERE memberType = 'Guarantor' AND endDate IS NULL
    ) AS hasGuarantor ON hasGuarantor.partyId = l.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.propertyId = prop.id AND f.name = 'UnitDeposit'
LEFT OUTER JOIN (
    SELECT q0.ID
        , REPLACE(fl.value, '"', '')::VARCHAR AS fees
        , REPLACE(fl.value: id, '"', '')::VARCHAR AS fees_id
        , REPLACE(fl.value: amount, '"', '')::VARCHAR AS fees_amount
    FROM {{ var("source_tenant") }}.QUOTE AS q0
        , LATERAL flatten(input => q0.publishedQuoteData: additionalAndOneTimeCharges: oneTimeCharges) AS fl
    ) AS quoteDep ON quoteDep.fees_id = f.ID AND quoteDep.id = q.id
WHERE l.STATUS = 'executed'
