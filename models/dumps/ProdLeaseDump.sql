/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ProdLeaseDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ProdLeaseDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ProdLeaseDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ProdLeaseDump') }}

-- depends on: {{ ref('PartyDump') }}

SELECT date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP AS "dumpGenDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIME)::VARCHAR)::TIMESTAMP AS "dumpGenTime"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , l.id AS "leaseId"
    , q.id AS "quoteId"
    , l.STATUS AS "status"
    , prop.name AS "Property"
    , i.name AS "Inventory"
    , i.id AS "inventoryId"
    , ig.name AS "InventoryGroup"
    , b.name AS "Building"
    , CONVERT_TIMEZONE(prop.timezone, i.availabilityDate)::TIMESTAMP_NTZ AS "unitAvailabilityDate"
    , COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') AS "leaseTerm"
    , COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0) AS "leaseBaseRent"
    , COALESCE((l.baselineData: quote: totalAdditionalRent)::DECIMAL, 0) AS "leaseAdditionalRent"
    , COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0) + COALESCE((l.baselineData: quote: totalAdditionalRent)::DECIMAL, 0) AS "leaseTotalRent"
    , COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0) AS "quoteOriginalBaseRent"
    , CASE
        WHEN COALESCE(q.leaseTerms: overwrittenBaseRent, '0.0')::DECIMAL = 0
            THEN (q.leaseTerms: originalBaseRent)::DECIMAL
        ELSE (q.leaseTerms: overwrittenBaseRent)::DECIMAL
        END AS "quoteOverwrittenBaseRent"
    , ig.basePriceMonthly AS "IGCurrentMarketRent"
    , lay.surfaceArea AS "inventoryArea"
    , u.fullName AS "partyOwnerName"
    , date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(l.baselineData: publishedLease: leaseStartDate, '7/4/1776'))::TIMESTAMP)::TIMESTAMP_NTZ AS "leaseStartDate"
    , date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(l.baselineData: publishedLease: leaseEndDate, '7/4/1776')::TIMESTAMP))::TIMESTAMP_NTZ AS "leaseEndDate"
    , COALESCE(CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), l.signDate), '7/4/1776')::TIMESTAMP_NTZ AS "signDate"
    , COALESCE(l.baselineData: publishedLease: additionalCharges, 'NULL charges') AS "additionalCharges"
    , lay.surfaceArea AS SQFT
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), l.baselineData: quote: moveInDate)::TIMESTAMP_NTZ AS "moveInDate"
    , COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0') AS "leaseUnitDeposit"
    , CASE
        WHEN hasGuarantor.partyId IS NULL
            THEN 0
        ELSE 1
        END AS "hasGuarantor"
    , COALESCE(l.baselineData: additionalConditions: additionalNotes, '') AS "approverNotes"
    , COALESCE(pq.approvingAgentName, '') AS "approvingAgentName"
    , COALESCE(quoteDep.fees: amount, '0') AS "quoteUnitDeposit"
    , i.externalId AS "inventoryExternalId"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',l.created_at::TIMESTAMP)::TIMESTAMP_NTZ AS "leaseCreatedDate"
    , p.partyGroupId AS "partyGroupId"
    , (COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0)) - (COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0)) AS "Quote2LeaseDiff"
    , CASE
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '1 Month'
            THEN 1
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '2 months'
            THEN 2
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '3 months'
            THEN 3
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '4 months'
            THEN 4
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '5 months'
            THEN 5
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '6 months'
            THEN 6
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '7 months'
            THEN 7
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '8 months'
            THEN 8
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '9 months'
            THEN 9
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '10 months'
            THEN 10
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '11 months'
            THEN 11
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '12 months'
            THEN 12
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '13 months'
            THEN 13
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '14 months'
            THEN 14
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '15 months'
            THEN 15
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '16 months'
            THEN 16
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '17 months'
            THEN 17
        WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '18 months'
            THEN 18
        ELSE 0
        END AS "leaseTermNo"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',dateadd(month, CASE
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '1 Month'
                THEN 1
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '2 months'
                THEN 2
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '3 months'
                THEN 3
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '4 months'
                THEN 4
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '5 months'
                THEN 5
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '6 months'
                THEN 6
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '7 months'
                THEN 7
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '8 months'
                THEN 8
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '9 months'
                THEN 9
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '10 months'
                THEN 10
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '11 months'
                THEN 11
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '12 months'
                THEN 12
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '13 months'
                THEN 13
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '14 months'
                THEN 14
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '15 months'
                THEN 15
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '16 months'
                THEN 16
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '17 months'
                THEN 17
            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '18 months'
                THEN 18
            ELSE 0
            END, date_trunc('day', COALESCE(l.baselineData: publishedLease: leaseStartDate, '7/4/1776')::TIMESTAMP))::TIMESTAMP)::TIMESTAMP_NTZ AS "calcdEndDate"
    , date_trunc( 'day',CONVERT_TIMEZONE('{{ var("timezone") }}',dateadd(day, - 1, dateadd(month, CASE
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '1 Month'
                    THEN 1
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '2 months'
                    THEN 2
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '3 months'
                    THEN 3
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '4 months'
                    THEN 4
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '5 months'
                    THEN 5
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '6 months'
                    THEN 6
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '7 months'
                    THEN 7
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '8 months'
                    THEN 8
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '9 months'
                    THEN 9
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '10 months'
                    THEN 10
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '11 months'
                    THEN 11
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '12 months'
                    THEN 12
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '13 months'
                    THEN 13
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '14 months'
                    THEN 14
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '15 months'
                    THEN 15
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '16 months'
                    THEN 16
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '17 months'
                    THEN 17
                WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '18 months'
                    THEN 18
                ELSE 0
                END, COALESCE(l.baselineData: publishedLease: leaseStartDate, '7/4/1776')::TIMESTAMP)::TIMESTAMP)))::TIMESTAMP_NTZ AS "finalCalcEndDate"
    , CASE
        WHEN (date_trunc('day', COALESCE(l.baselineData: publishedLease: leaseEndDate, '7/4/1776')::TIMESTAMP)::TIMESTAMP) = (
                dateadd(day, - 1, dateadd(month, CASE
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '1 Month'
                                THEN 1
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '2 months'
                                THEN 2
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '3 months'
                                THEN 3
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '4 months'
                                THEN 4
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '5 months'
                                THEN 5
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '6 months'
                                THEN 6
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '7 months'
                                THEN 7
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '8 months'
                                THEN 8
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '9 months'
                                THEN 9
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '10 months'
                                THEN 10
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '11 months'
                                THEN 11
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '12 months'
                                THEN 12
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '13 months'
                                THEN 13
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '14 months'
                                THEN 14
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '15 months'
                                THEN 15
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '16 months'
                                THEN 16
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '17 months'
                                THEN 17
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '18 months'
                                THEN 18
                            ELSE 0
                            END, date_trunc('day', COALESCE(l.baselineData: publishedLease: leaseStartDate, '7/4/1776')::TIMESTAMP)::TIMESTAMP))
                )
            THEN 1
        ELSE 0
        END AS "leaseEndsOnTerm"
    , round((
            datediff(month, dateadd(day, - 1, dateadd(month, CASE
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '1 Month'
                                THEN 1
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '2 months'
                                THEN 2
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '3 months'
                                THEN 3
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '4 months'
                                THEN 4
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '5 months'
                                THEN 5
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '6 months'
                                THEN 6
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '7 months'
                                THEN 7
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '8 months'
                                THEN 8
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '9 months'
                                THEN 9
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '10 months'
                                THEN 10
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '11 months'
                                THEN 11
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '12 months'
                                THEN 12
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '13 months'
                                THEN 13
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '14 months'
                                THEN 14
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '15 months'
                                THEN 15
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '16 months'
                                THEN 16
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '17 months'
                                THEN 17
                            WHEN COALESCE(l.baselineData: quote: leaseTerm, 'NULL Lease Term') = '18 months'
                                THEN 18
                            ELSE 0
                            END, date_trunc('day', COALESCE(l.baselineData: publishedLease: leaseStartDate, '7/4/1776')::TIMESTAMP)::TIMESTAMP)), date_trunc('day', COALESCE(l.baselineData: publishedLease: leaseEndDate, '7/4/1776')::TIMESTAMP)::TIMESTAMP)
            ) / 30) AS "leaseEndDiffMo"
    , CASE
        WHEN sc.APPLICATIONDECISION = 'approved'
            THEN 'Approved'
        WHEN sc.APPLICATIONDECISION = 'approved_with_cond'
            THEN 'Conditional Approval'
        WHEN sc.APPLICATIONDECISION = 'declined'
            THEN 'Declined'
        WHEN sc.APPLICATIONDECISION = 'further_review'
            THEN 'Further Review'
        WHEN sc.APPLICATIONDECISION = 'Guarantor Required'
            THEN 'Conditional Approval'
        ELSE CASE
                WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                    THEN '[No Completed Application]'
                ELSE sc.APPLICATIONDECISION
                END
        END AS "applicationDecisionClean"
    , CASE
        WHEN l.STATUS = 'draft'
            THEN 'Ready to publish'
        WHEN l.STATUS = 'submitted'
            THEN 'Published for signature'
        WHEN l.STATUS = 'executed'
            THEN 'Fully executed'
        WHEN l.STATUS = 'voided'
            THEN 'Lease voided'
        ELSE 'Error'
        END AS "leaseStatusClean"
    , CASE
        WHEN COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0') > ((COALESCE(quoteDep.fees: amount, '0')) * 1.4)
            THEN 1
        ELSE 0
        END AS "hasIncreasedLeaseDeposit"
    , CASE
        WHEN (COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')) > ((COALESCE(quoteDep.fees: amount, '0')) * 1.4)
            THEN 1
        WHEN (
                CASE
                    WHEN hasGuarantor.partyId IS NULL
                        THEN 0
                    ELSE 1
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "hasGuarORIncDeposit"
    , CASE
			WHEN (COALESCE(l.baselineData:publishedLease:oneTimeCharges[f.id]:amount, 0))::numeric < COALESCE(quoteDep.fees:amount, 0)::numeric
            THEN 1
        ELSE 0
        END AS "hasNoDeposit"
    , CASE
        WHEN (COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0)) < (COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0)) AND (
                CASE
                    WHEN p.workflowName = 'renewal'
                        THEN 1
                    ELSE 0
                    END
                ) = 0
            THEN 1
        WHEN (COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0)) < (COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0)) AND (
                CASE
                    WHEN p.workflowName = 'renewal'
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "hasRentBelowQuote"
    , CASE
        WHEN l.STATUS = 'executed' AND (
                CASE
                    WHEN sc.APPLICATIONDECISION = 'approved'
                        THEN 'Approved'
                    WHEN sc.APPLICATIONDECISION = 'approved_with_cond'
                        THEN 'Conditional Approval'
                    WHEN sc.APPLICATIONDECISION = 'declined'
                        THEN 'Declined'
                    WHEN sc.APPLICATIONDECISION = 'further_review'
                        THEN 'Further Review'
                    WHEN sc.APPLICATIONDECISION = 'Guarantor Required'
                        THEN 'Conditional Approval'
                    ELSE CASE
                            WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                THEN '[No Completed Application]'
                            ELSE sc.APPLICATIONDECISION
                            END
                    END
                ) = 'Conditional Approval' AND (
                CASE
                    WHEN (COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')) > ((COALESCE(quoteDep.fees: amount, '0')) * 1.4)
                        THEN 1
                    WHEN (
                            CASE
                                WHEN hasGuarantor.partyId IS NULL
                                    THEN 0
                                ELSE 1
                                END
                            ) = 1
                        THEN 1
                    ELSE 0
                    END
                ) = 0
            THEN 1
        WHEN l.STATUS = 'executed' AND (
                CASE
                    WHEN sc.APPLICATIONDECISION = 'approved'
                        THEN 'Approved'
                    WHEN sc.APPLICATIONDECISION = 'approved_with_cond'
                        THEN 'Conditional Approval'
                    WHEN sc.APPLICATIONDECISION = 'declined'
                        THEN 'Declined'
                    WHEN sc.APPLICATIONDECISION = 'further_review'
                        THEN 'Further Review'
                    WHEN sc.APPLICATIONDECISION = 'Guarantor Required'
                        THEN 'Conditional Approval'
                    ELSE CASE
                            WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                THEN '[No Completed Application]'
                            ELSE sc.APPLICATIONDECISION
                            END
                    END
                ) = 'Declined'
            THEN 1
        WHEN l.STATUS = 'executed' AND (
                CASE
                    WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.APPLICATIONDECISION
                    END
                ) = 'further_review'
            THEN 1
        WHEN l.STATUS = 'executed' AND (
                CASE
                    WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.APPLICATIONDECISION
                    END
                ) = 'pending'
            THEN 1
        WHEN l.STATUS = 'executed' AND (
                CASE
                    WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.APPLICATIONDECISION
                    END
                ) = '[No Completed Application]' AND (
                pd."isRenewal" = 0 or pd."isTransfer" = 0
                )
            THEN 1
        ELSE 0
        END AS "isNonCompliant2"
    , CASE
        WHEN (CASE
                WHEN l.STATUS = 'executed' AND (
                        CASE
                            WHEN sc.APPLICATIONDECISION = 'approved'
                                THEN 'Approved'
                            WHEN sc.APPLICATIONDECISION = 'approved_with_cond'
                                THEN 'Conditional Approval'
                            WHEN sc.APPLICATIONDECISION = 'declined'
                                THEN 'Declined'
                            WHEN sc.APPLICATIONDECISION = 'further_review'
                                THEN 'Further Review'
                            WHEN sc.APPLICATIONDECISION = 'Guarantor Required'
                                THEN 'Conditional Approval'
                            ELSE CASE
                                    WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                        THEN '[No Completed Application]'
                                    ELSE sc.APPLICATIONDECISION
                                    END
                            END
                        ) = 'Conditional Approval' AND (
                        CASE
                            WHEN (COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')) > ((COALESCE(quoteDep.fees: amount, '0')) * 1.4)
                                THEN 1
                            WHEN (
                                    CASE
                                        WHEN hasGuarantor.partyId IS NULL
                                            THEN 0
                                        ELSE 1
                                        END
                                    ) = 1
                                THEN 1
                            ELSE 0
                            END
                        ) = 0
                    THEN 1
                WHEN l.STATUS = 'executed' AND (
                        CASE
                            WHEN sc.APPLICATIONDECISION = 'approved'
                                THEN 'Approved'
                            WHEN sc.APPLICATIONDECISION = 'approved_with_cond'
                                THEN 'Conditional Approval'
                            WHEN sc.APPLICATIONDECISION = 'declined'
                                THEN 'Declined'
                            WHEN sc.APPLICATIONDECISION = 'further_review'
                                THEN 'Further Review'
                            WHEN sc.APPLICATIONDECISION = 'Guarantor Required'
                                THEN 'Conditional Approval'
                            ELSE CASE
                                    WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                        THEN '[No Completed Application]'
                                    ELSE sc.APPLICATIONDECISION
                                    END
                            END
                        ) = 'Declined'
                    THEN 1
                WHEN l.STATUS = 'executed' AND (
                        CASE
                            WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                THEN '[No Completed Application]'
                            ELSE sc.APPLICATIONDECISION
                            END
                        ) = 'further_review'
                    THEN 1
                WHEN l.STATUS = 'executed' AND (
                        CASE
                            WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                THEN '[No Completed Application]'
                            ELSE sc.APPLICATIONDECISION
                            END
                        ) = 'pending'
                    THEN 1
                WHEN l.STATUS = 'executed' AND (
                        CASE
                            WHEN COALESCE(sc.APPLICATIONDECISION, '') = ''
                                THEN '[No Completed Application]'
                            ELSE sc.APPLICATIONDECISION
                            END
                        ) = '[No Completed Application]' AND (
                        pd."isRenewal" = 0 or pd."isTransfer" = 0
                        )
                    THEN 1
                ELSE 0
                END
                ) = 1
            THEN 1
        WHEN (CASE
                    WHEN (COALESCE(l.baselineData:publishedLease:oneTimeCharges[f.id]:amount, 0))::numeric < COALESCE(quoteDep.fees:amount, 0)::numeric
                    THEN 1
                ELSE 0
                END
                ) = 1
            THEN 1
        WHEN (
                CASE
                    WHEN (COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0)) < (COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0)) AND (
                            CASE
                                WHEN p.workflowName = 'renewal'
                                    THEN 1
                                ELSE 0
                                END
                            ) = 0
                        THEN 1
                    WHEN (COALESCE((l.baselineData: publishedLease: unitRent)::DECIMAL, 0)) < (COALESCE((q.leaseTerms: originalBaseRent)::DECIMAL, 0)) AND (
                            CASE
                                WHEN p.workflowName = 'renewal'
                                    THEN 1
                                ELSE 0
                                END
                            ) = 1
                        THEN 1
                    ELSE 0
                    END
                ) = 1
            THEN 1
        ELSE 0
        END AS "anyNonCompliance"
    , CASE
        WHEN position('transfer', lower(COALESCE(l.baselineData: additionalConditions: additionalNotes, ''))) > 0
            THEN 1
        ELSE 0
        END AS "potentialUnitTransfer"
    , CASE
        WHEN POSITION('income', lower(COALESCE(l.baselineData: additionalConditions: additionalNotes, ''))) > 0
            THEN 1
        ELSE 0
        END AS "potentialIncomeShortcut"
    , CASE
        WHEN (COALESCE(l.baselineData: additionalConditions: additionalNotes, '')) = '' AND (
                CASE
                    WHEN p.workflowName = 'renewal'
                        THEN 1
                    ELSE 0
                    END
                ) = 0
            THEN 1
        ELSE 0
        END AS "nonCompliantScreenNoNotes"
    , CASE
        WHEN COALESCE(sc.applicationDecision, '') = ''
            THEN '[No Completed Application]'
        ELSE sc.applicationDecision
        END AS "applicationDecision"
    , CASE
        WHEN COALESCE(mr.applicationDecision, '') = ''
            THEN '[No Completed Application]'
        ELSE mr.applicationDecision
        END AS "currentApplicationDecision"
    , CASE
        WHEN COALESCE(sc.allRecs, '') = ''
            THEN '[No System Recommendations]'
        ELSE sc.allRecs
        END AS "Recommendations"
    , CASE
        WHEN COALESCE(mr.allRecs, '') = ''
            THEN '[No System Recommendations]'
        ELSE mr.allRecs
        END AS "currentRecommendations"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',CASE
        WHEN COALESCE(mr.quoteId, '') = ''
            THEN to_date('1900-01-01')
        ELSE mr.responseTime
        END::TIMESTAMP)::TIMESTAMP_NTZ AS "currentScreeningResponseDate"
    , CASE
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = 'approved'
            THEN 0
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = '[No Completed Application]'
            THEN 1
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = 'declined'
            THEN 1
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = 'Guarantor Required'
            THEN CASE
                    WHEN COALESCE((
                                CASE
                                    WHEN hasGuarantor.partyId IS NULL
                                        THEN 0
                                    ELSE 1
                                    END::VARCHAR
                                ), '') = ''
                        THEN 1
                    ELSE 0
                    END
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = 'approved_with_cond'
            THEN CASE
                    WHEN (
                            CASE
                                WHEN COALESCE(sc.allRecs, '') = ''
                                    THEN '[No System Recommendations]'
                                ELSE sc.allRecs
                                END
                            ) LIKE '%additional deposit%'
                        THEN CASE
                                WHEN (COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')) > (COALESCE(quoteDep.fees: amount, '0'))
                                    THEN 0
                                ELSE 1
                                END
                    WHEN (
                            CASE
                                WHEN COALESCE(sc.allRecs, '') = ''
                                    THEN '[No System Recommendations]'
                                ELSE sc.allRecs
                                END
                            ) LIKE '%uarantor%'
                        THEN CASE
                                WHEN (
                                        CASE
                                            WHEN hasGuarantor.partyId IS NULL
                                                THEN 0
                                            ELSE 1
                                            END
                                        ) = 0
                                    THEN 1
                                ELSE 0
                                END
                    ELSE 0
                    END
        WHEN (
                CASE
                    WHEN COALESCE(sc.applicationDecision, '') = ''
                        THEN '[No Completed Application]'
                    ELSE sc.applicationDecision
                    END
                ) = 'further_review'
            THEN CASE
                    WHEN (
                            CASE
                                WHEN COALESCE(sc.allRecs, '') = ''
                                    THEN '[No System Recommendations]'
                                ELSE sc.allRecs
                                END
                            ) LIKE '%additional deposit%'
                        THEN CASE
                                WHEN (COALESCE(l.baselineData: publishedLease: oneTimeCharges [f.id] :amount, '0')) > (COALESCE(quoteDep.fees: amount, '0'))
                                    THEN 0
                                ELSE 1
                                END
                    WHEN (
                            CASE
                                WHEN COALESCE(sc.allRecs, '') = ''
                                    THEN '[No System Recommendations]'
                                ELSE sc.allRecs
                                END
                            ) LIKE '%uarantor%'
                        THEN CASE
                                WHEN (
                                        CASE
                                            WHEN hasGuarantor.partyId IS NULL
                                                THEN 0
                                            ELSE 1
                                            END
                                        ) = 0
                                    THEN 1
                                ELSE 0
                                END
                    ELSE 0
                    END
        ELSE 0
        END AS "isNonCompliant"
        , pd."LeaseTypeNN"
        , pd."externalId" AS "externalId"
        , pd."fullyQualified" AS "fullyQualified"
FROM {{ var("source_tenant") }}.LEASE AS l
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = l.partyId
INNER JOIN (
    SELECT *
        , fl.value AS leaseTerms
        , publishedQuoteData: leaseTerms [fl.index] :termLength AS termLength
    FROM {{ var("source_tenant") }}.QUOTE
        , LATERAL flatten(input => publishedQuoteData: leaseTerms) fl
    ) AS q ON q.id = l.quoteId
INNER JOIN (
    SELECT mostRecent.*
        , u.fullName AS approvingAgentName
    FROM (
        SELECT rank() OVER (
                PARTITION BY quoteId ORDER BY created_at DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.PARTYQUOTEPROMOTIONS
        ) AS mostRecent
    LEFT OUTER JOIN {{ var("source_tenant") }}."USERS" u ON u.id = mostRecent.approvedBy
    WHERE mostRecent.theRank = 1
    ) AS pq ON pq.quoteId = q.id
INNER JOIN {{ var("source_tenant") }}.LEASETERM AS lt ON lt.id = pq.leaseTermId AND lt.termLength = q.termLength
INNER JOIN {{ var("source_tenant") }}.INVENTORY AS i ON i.id = q.inventoryId
INNER JOIN {{ var("source_tenant") }}.LAYOUT AS lay ON lay.id = i.layoutId
INNER JOIN {{ var("source_tenant") }}.INVENTORYGROUP AS ig ON ig.id = i.inventoryGroupId
INNER JOIN {{ var("source_tenant") }}.BUILDING AS b ON b.id = i.buildingId
INNER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = i.propertyId
LEFT OUTER JOIN {{ var("source_tenant") }}."USERS" AS u ON u.id = p.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.propertyId = prop.id AND lower(f.name) = 'unitdeposit'
LEFT OUTER JOIN (
    SELECT DISTINCT partyId
    FROM {{ var("source_tenant") }}.PARTYMEMBER pm
    WHERE memberType = 'Guarantor' AND endDate IS NULL
    ) AS hasGuarantor ON hasGuarantor.partyId = l.partyId
LEFT JOIN LATERAL(SELECT id, fl.value AS fees FROM {{ var("source_tenant") }}.QUOTE q0
        , LATERAL flatten(input => parse_json(q0.publishedQuoteData: additionalAndOneTimeCharges: oneTimeCharges)) fl WHERE q0.id = q.id) quoteDep ON quoteDep.fees: id = f.id::TEXT
LEFT JOIN (
    SELECT resp0.leaseId
        , resp0.quoteId
        , recs.allRecs
        , resp0.applicationDecision AS applicationDecision
        , resp0.created_at
    FROM (
        SELECT rank() OVER (
                PARTITION BY req00.quoteId ORDER BY resp00.created_at DESC
                    , resp00.id
                ) AS theRank
            , l00.id AS leaseId
            , q00.id AS quoteId
            , resp00.*
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse resp00
        INNER JOIN {{ var("source_tenant") }}.rentapp_SubmissionRequest req00 ON req00.id = resp00.submissionRequestId
        INNER JOIN {{ var("source_tenant") }}.Quote q00 ON q00.id = req00.quoteId
        INNER JOIN {{ var("source_tenant") }}.Lease l00 ON l00.quoteId = q00.id
        WHERE resp00.STATUS = 'Complete' AND resp00.created_at <= l00.created_at
        ) resp0
    LEFT OUTER JOIN (
        SELECT id AS responseId
            , listagg(unnested.recs: text, ' | ') WITHIN
        GROUP (
                ORDER BY unnested.recs
                ) AS allRecs
        FROM (
            SELECT DISTINCT id
                , fl.value AS recs
            FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
                , LATERAL flatten(input => recommendations) fl
            ) unnested
        GROUP BY id
        ) recs ON recs.responseId = resp0.id
    WHERE resp0.theRank = 1
    ) sc ON sc.LEASEID = l.ID
LEFT JOIN (
    SELECT req.quoteId
        , resp.applicationDecision
        , resp.allRecs
        , req.created_at AS requestTime
        , resp.created_at AS responseTime
        , resp.id AS responseId
    FROM (
        SELECT rank() OVER (
                PARTITION BY req0.quoteId ORDER BY req0.created_at DESC
                    , id DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest req0
        ) req
    LEFT OUTER JOIN (
        SELECT rank() OVER (
                PARTITION BY resp0.submissionRequestId ORDER BY resp0.created_at DESC
                    , id DESC
                ) AS theRank
            , *
        FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse resp0
        LEFT OUTER JOIN (
            SELECT id AS responseId
                , listagg(unnested.recs: text, ' | ') WITHIN
            GROUP (
                    ORDER BY unnested.recs
                    ) AS allRecs
            FROM (
                SELECT DISTINCT id
                    , fl.value AS recs
                FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse
                    , LATERAL flatten(input => recommendations) fl
                ) unnested
            GROUP BY id
            ) recs ON recs.responseId = resp0.id
        WHERE resp0.STATUS = 'Complete'
        ) resp ON resp.submissionRequestId = req.id AND resp.theRank = 1
    WHERE req.theRank = 1
    ) mr ON mr.quoteId = q.id
	LEFT JOIN (SELECT DISTINCT "partyIdNoURL", "partyId", "LeaseTypeNN", "externalId", "fullyQualified", "isRenewal", "isTransfer"
                FROM {{ var("target_schema") }}."PartyDump") AS pd ON pd."partyIdNoURL" = p.id
