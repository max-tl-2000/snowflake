/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.AppFeesRefundsAndWaivers --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.AppFeesRefundsAndWaivers --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.AppFeesRefundsAndWaivers --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='App Fees, Refunds, and Waivers') }}


SELECT ai.id AS "id"
    , prop.name AS "Property"
    , prop.displayName AS "displayName"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE(atran.created_at, ai.created_at))::TIMESTAMP_NTZ AS "Date"
    , atran.transactionType AS "transactionType"
    , primPm.externalId AS "tCode"
    , primPm.externalProspectId AS "pCode"
    , primPer.fullName AS "PrimaryTenant"
    , atran.externalId AS "AptexxRef"
    , COALESCE((atran.transactionData: firstName), '') || ' ' || COALESCE((atran.transactionData: lastName), '') AS "PaidBy"
    , COALESCE((atran.transactionData: brandType), '') || ' ' || COALESCE((atran.transactionData: channelType), '') AS "PaymentMethod"
    , ((atran.transactionData: amount)::NUMERIC / 100) * CASE
        WHEN COALESCE(atran.transactionType, 'payment') = 'payment'
            THEN 1
        ELSE - 1
        END AS "Amount"
    , appPer.fullName AS "applicantName"
    , ai.paymentCompleted AS "paymentCompleted"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , p.id AS "partyIdNoURL"
    , feeProp.name AS "appFeeProperty"
    , active.isActive AS "applicantIsActive"
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, 'America/Los_Angeles'),active.inactiveDate)::TIMESTAMP_NTZ AS "applicantInactiveDate"
    , f.name AS "feeName"
    , CASE
        WHEN feeProp.name = prop.name
            THEN 1
        ELSE 0
        END AS "PartyAndAppPropertyAreSame"
FROM {{ var("source_tenant") }}.RENTAPP_APPLICATIONINVOICES AS ai
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = ai.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp ON perApp.id = ai.personApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = pa.partyId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_APPLICATIONTRANSACTIONS AS atran ON atran.externalId = ai.appFeeTransactionId
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS appPer ON appPer.id = perApp.personId
LEFT OUTER JOIN (
    SELECT personId
        , partyId
        , CASE
            WHEN endDate IS NULL
                THEN 1
            ELSE 0
            END AS isActive
        , endDate AS inactiveDate
        , rank() OVER (
            PARTITION BY partyId
            , personId ORDER BY COALESCE(endDate, '2999-01-01') DESC
            ) AS theRank
    FROM {{ var("source_tenant") }}.PARTYMEMBER pm
    ) AS active ON active.personId = appPer.id AND active.partyId = perApp.partyId AND active.theRank = 1
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
        ) mostRecent
    WHERE mostRecent.theRank = 1
    ) AS primPm ON primPm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS primPer ON primPer.id = primPm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.id = ai.applicationFeeId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS feeProp ON feeProp.id = f.propertyId
WHERE ai.paymentCompleted = 'true' AND atran.transactionType IN ('payment', 'refund')

UNION ALL

/*Hold Deposits*/
SELECT ai.id
    , prop.name AS Property
    , prop.displayName
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE(atran.created_at, ai.created_at))::TIMESTAMP_NTZ AS DATE
    , atran.transactionType
    , primPm.externalId AS tCode
    , primPm.externalProspectId AS pCode
    , primPer.fullName AS PrimaryTenant
    , atran.externalId AS AptexxRef
    , COALESCE((atran.transactionData: firstName), '') || ' ' || COALESCE((atran.transactionData: lastName), '') AS PaidBy
    , COALESCE((atran.transactionData: brandType), '') || ' ' || COALESCE((atran.transactionData: channelType), '') AS PaymentMethod
    , ((atran.transactionData: amount)::NUMERIC / 100) * CASE
        WHEN COALESCE(atran.transactionType, 'payment') = 'payment'
            THEN 1
        ELSE - 1
        END AS Amount
    , appPer.fullName AS applicantName
    , ai.paymentCompleted
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS partyId
    , p.id AS partyIdNoURL
    , feeProp.name AS appFeeProperty
    , active.isActive AS applicantIsActive
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, 'America/Los_Angeles'),active.inactiveDate)::TIMESTAMP_NTZ AS "applicantInactiveDate"
    , f.name AS feeName
    , CASE
        WHEN feeProp.name = prop.name
            THEN 1
        ELSE 0
        END AS PartyAndAppPropertyAreSame
FROM {{ var("source_tenant") }}.RENTAPP_APPLICATIONINVOICES AS ai
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = ai.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp ON perApp.id = ai.personApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = pa.partyId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_APPLICATIONTRANSACTIONS AS atran ON atran.externalId = ai.holdDepositTransactionId
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS appPer ON appPer.id = perApp.personId
LEFT OUTER JOIN (
    SELECT personId
        , partyId
        , CASE
            WHEN endDate IS NULL
                THEN 1
            ELSE 0
            END AS isActive
        , endDate AS inactiveDate
        , rank() OVER (
            PARTITION BY partyId
            , personId ORDER BY COALESCE(endDate, '2999-01-01') DESC
            ) AS theRank
    FROM {{ var("source_tenant") }}.PARTYMEMBER AS pm
    ) AS active ON active.personId = appPer.id AND active.partyId = perApp.partyId AND active.theRank = 1
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
        FROM {{ var("source_tenant") }}.EXTERNALPARTYMEMBERINFO AS ei
        INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER AS pm ON pm.id = ei.partyMemberId
        WHERE ei.isPrimary = 'true'
        ) mostRecent
    WHERE mostRecent.theRank = 1
    ) AS primPm ON primPm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON primPer ON primPer.id = primPm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE f ON f.id = ai.holdDepositFeeId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY feeProp ON feeProp.id = f.propertyId
WHERE ai.paymentCompleted = 'true' AND atran.transactionType IN ('payment', 'refund')

UNION ALL

/*BAD Data - no atran.externalId match*/
SELECT ai.id
    , prop.name AS Property
    , prop.displayName
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), COALESCE(atran.created_at, ai.created_at))::TIMESTAMP_NTZ AS DATE
    , atran.transactionType
    , primPm.externalId AS tCode
    , primPm.externalProspectId AS pCode
    , primPer.fullName AS PrimaryTenant
    , atran.externalId AS AptexxRef
    , COALESCE((atran.transactionData: firstName), '') || ' ' || COALESCE((atran.transactionData: lastName), '') AS PaidBy
    , COALESCE((atran.transactionData: brandType), '') || ' ' || COALESCE((atran.transactionData: channelType), '') AS PaymentMethod
    , ((atran.transactionData: amount)::NUMERIC / 100) * CASE
        WHEN COALESCE(atran.transactionType, 'payment') = 'payment'
            THEN 1
        ELSE - 1
        END AS Amount
    , appPer.fullName AS applicantName
    , ai.paymentCompleted
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS partyId
    , p.id AS partyIdNoURL
    , feeProp.name AS appFeeProperty
    , active.isActive AS applicantIsActive
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, 'America/Los_Angeles'),active.inactiveDate)::TIMESTAMP_NTZ AS "applicantInactiveDate"
    , CASE
        WHEN ((atran.transactionData: amount)::NUMERIC / 100) = ai.holdDepositFeeIdAmount
            THEN 'AssumedDeposit'
        WHEN ((atran.transactionData: amount)::NUMERIC / 100) = ai.holdDepositFeeIdAmount + ai.applicationFeeAmount
            THEN 'AssumedAppFeeAndHoldDeposit'
        ELSE 'AssumedAppFee'
        END AS feeName
    , CASE
        WHEN feeProp.name = prop.name
            THEN 1
        ELSE 0
        END AS PartyAndAppPropertyAreSame
FROM {{ var("source_tenant") }}.RENTAPP_APPLICATIONINVOICES AS ai
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = ai.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp ON perApp.id = ai.personApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = pa.partyId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_APPLICATIONTRANSACTIONS AS atran ON atran.invoiceId = ai.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS appPer ON appPer.id = perApp.personId
LEFT OUTER JOIN (
    SELECT personId
        , partyId
        , CASE
            WHEN endDate IS NULL
                THEN 1
            ELSE 0
            END AS isActive
        , endDate AS inactiveDate
        , rank() OVER (
            PARTITION BY partyId
            , personId ORDER BY COALESCE(endDate, '2999-01-01') DESC
            ) AS theRank
    FROM {{ var("source_tenant") }}.PARTYMEMBER pm
    ) AS active ON active.personId = appPer.id AND active.partyId = perApp.partyId AND active.theRank = 1
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
        ) mostRecent
    WHERE mostRecent.theRank = 1
    ) AS primPm ON primPm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS primPer ON primPer.id = primPm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.id = ai.applicationFeeId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS feeProp ON feeProp.id = f.propertyId
WHERE ai.paymentCompleted = 'true' AND atran.transactionType IN ('payment', 'refund') AND atran.externalId <> COALESCE(ai.appFeeTransactionId, '') AND atran.externalId <> COALESCE(ai.holdDepositTransactionId, '')

UNION ALL

/*Waivers*/
SELECT ai.id
    , prop.name AS Property
    , prop.displayName
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), ai.created_at)::TIMESTAMP_NTZ AS DATE
    , 'waiver' AS transactionType
    , primPm.externalId AS tCode
    , primPm.externalProspectId AS pCode
    , primPer.fullName AS PrimaryTenant
    , 'N/A' AS AptexxRef
    , 'Waiver' AS PaidBy
    , 'N/A' AS PaymentMethod
    , (ai.applicationFeeWaiverAmount) * - 1 AS Amount
    , appPer.fullName AS applicantName
    , ai.paymentCompleted
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS partyId
    , p.id AS partyIdNoURL
    , feeProp.name AS appFeeProperty
    , active.isActive AS applicantIsActive
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, 'America/Los_Angeles'),active.inactiveDate)::TIMESTAMP_NTZ AS "applicantInactiveDate"
    , f.name AS feeName
    , CASE
        WHEN feeProp.name = prop.name
            THEN 1
        ELSE 0
        END AS PartyAndAppPropertyAreSame
FROM {{ var("source_tenant") }}.RENTAPP_APPLICATIONINVOICES AS ai
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = ai.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp ON perApp.id = ai.personApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = pa.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS appPer ON appPer.id = perApp.personId
LEFT OUTER JOIN (
    SELECT personId
        , partyId
        , CASE
            WHEN endDate IS NULL
                THEN 1
            ELSE 0
            END AS isActive
        , endDate AS inactiveDate
        , rank() OVER (
            PARTITION BY partyId
            , personId ORDER BY COALESCE(endDate, '2999-01-01') DESC
            ) AS theRank
    FROM {{ var("source_tenant") }}.PARTYMEMBER pm
    ) AS active ON active.personId = appPer.id AND active.partyId = perApp.partyId AND active.theRank = 1
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
        ) mostRecent
    WHERE mostRecent.theRank = 1
    ) AS primPm ON primPm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS primPer ON primPer.id = primPm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.id = ai.applicationFeeId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS feeProp ON feeProp.id = f.propertyId
WHERE ai.paymentCompleted = 'true' AND ai.applicationFeeWaiverAmount IS NOT NULL

UNION ALL

/*Waiver Invoices*/
SELECT ai.id
    , prop.name AS Property
    , prop.displayName
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), ai.created_at)::TIMESTAMP_NTZ AS DATE
    , 'waiverInvoice' AS transactionType
    , primPm.externalId AS tCode
    , primPm.externalProspectId AS pCode
    , primPer.fullName AS PrimaryTenant
    , 'N/A' AS AptexxRef
    , 'Waiver Invoice' AS PaidBy
    , 'N/A' AS PaymentMethod
    , (ai.applicationFeeWaiverAmount) AS Amount
    , appPer.fullName AS applicantName
    , ai.paymentCompleted
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS partyId
    , p.id AS partyIdNoURL
    , feeProp.name AS appFeeProperty
    , active.isActive AS applicantIsActive
    , CONVERT_TIMEZONE(COALESCE(prop.timezone, 'America/Los_Angeles'),active.inactiveDate)::TIMESTAMP_NTZ AS "applicantInactiveDate"
    , f.name AS feeName
    , CASE
        WHEN feeProp.name = prop.name
            THEN 1
        ELSE 0
        END AS PartyAndAppPropertyAreSame
FROM {{ var("source_tenant") }}.RENTAPP_APPLICATIONINVOICES AS ai
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PARTYAPPLICATION AS pa ON pa.id = ai.partyApplicationId
INNER JOIN {{ var("source_tenant") }}.RENTAPP_PERSONAPPLICATION AS perApp ON perApp.id = ai.personApplicationId
INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id = pa.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS appPer ON appPer.id = perApp.personId
LEFT OUTER JOIN (
    SELECT personId
        , partyId
        , CASE
            WHEN endDate IS NULL
                THEN 1
            ELSE 0
            END AS isActive
        , endDate AS inactiveDate
        , rank() OVER (
            PARTITION BY partyId
            , personId ORDER BY COALESCE(endDate, '2999-01-01') DESC
            ) AS theRank
    FROM {{ var("source_tenant") }}.PARTYMEMBER AS pm
    ) AS active ON active.personId = appPer.id AND active.partyId = perApp.partyId AND active.theRank = 1
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
        FROM {{ var("source_tenant") }}.EXTERNALPARTYMEMBERINFO AS ei
        INNER JOIN {{ var("source_tenant") }}.PARTYMEMBER AS pm ON pm.id = ei.partyMemberId
        WHERE ei.isPrimary = 'true'
        ) AS mostRecent
    WHERE mostRecent.theRank = 1
    ) AS primPm ON primPm.partyId = p.id
LEFT OUTER JOIN {{ var("source_tenant") }}.PERSON AS primPer ON primPer.id = primPm.personId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.FEE AS f ON f.id = ai.applicationFeeId
LEFT OUTER JOIN {{ var("source_tenant") }}.PROPERTY AS feeProp ON feeProp.id = f.propertyId
WHERE ai.paymentCompleted = 'true' AND ai.applicationFeeWaiverAmount IS NOT NULL
