/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ApplicantTypeInfo --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ApplicantTypeInfo --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ApplicantTypeInfo --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ApplicantTypeInfo') }}

SELECT prop.displayName AS "displayName"
    , req.transactionNumber AS "applicationID"
	, REPLACE(fl.value:ApplicantID[0],'"','') AS "applicantId"
	, REPLACE(fl.value:ApplicantName[0],'"','') AS "applicantName"
	, REPLACE(fl.value:ApplicantType[0],'"','') AS "FADVResponseType"
    , pa.partyId AS "partyId"
FROM {{ var("source_tenant") }}.rentapp_SubmissionResponse AS resp
INNER JOIN (
    SELECT rank() OVER (
            PARTITION BY partyApplicationId ORDER BY created_at DESC
            ) AS theRank
        , transactionNumber
        , completeSubmissionResponseId
        , propertyId
        , partyApplicationId
        , id
    FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
    ) AS req ON trim(req.completeSubmissionResponseId) = trim(resp.id) AND req.theRank = 1
LEFT JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = req.propertyId
INNER JOIN {{ var("source_tenant") }}.rentapp_PartyApplication AS pa ON pa.id = req.partyApplicationId
, LATERAL FLATTEN(input => parse_json(resp.rawResponse):ApplicantScreening :CustomRecordsExtended [0] :Record [0] :Value [0] :AEROReport [0] :RentToIncomes [0] :Applicant) fl
WHERE resp.created_at >= '2019-01-01'
