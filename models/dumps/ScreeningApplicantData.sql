/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.ScreeningApplicantData --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.ScreeningApplicantData --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.ScreeningApplicantData --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='ScreeningApplicantData') }}

SELECT
    id AS "requestId",
    requestType AS "requestType",
    REPLACE(fl.value: type, '"', '') as "applicant_type",
    REPLACE(fl.value: email, '"', '') as "applicant_email",
    REPLACE(fl.value: phone, '"', '') as "applicant_phone",
    REPLACE(fl.value: lastName, '"', '') as "applicant_lastName",
    REPLACE(fl.value: personId, '"', '')  as "applicant_personId",
    REPLACE(fl.value: firstName, '"', '') as "applicant_firstName",
    REPLACE(fl.value: applicantId, '"', '') as "applicant_applicantId",
    REPLACE(fl.value: dateOfBirth, '"', '') as "applicant_dateOfBirt",
    REPLACE(fl.value: grossIncome, '"', '') as "applicant_grossIncome",
    applicantData AS "applicantData",
    COALESCE(dataChange."hasPrimaryDataChange", 0) AS "hasPrimaryDataChange",
    COALESCE(dataChange."arrayVals" :: text, '') AS "diffData",
    CASE WHEN requestType = 'New' THEN 1 ELSE 0 END AS "isNew",
    CASE WHEN requestType = 'Modify' AND "hasPrimaryDataChange" = 1 THEN 1 ELSE 0 END AS "isRevision"
FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest AS req
    LEFT JOIN (
        SELECT
            id AS "requestId",
            (requestDataDiff: diff) AS "arrayVals",
            1 AS "hasPrimaryDataChange"
        FROM {{ var("source_tenant") }}.rentapp_SubmissionRequest
        WHERE requestDataDiff: diff LIKE ANY ('%"prop":%"address"%','%"prop":%"addressLine"%','%"prop":%"applicantId"%','%"prop":%"applicants"%','%"prop":%"city"%','%"prop":%"dateOfBirth"%'
                    ,'%"prop":%"firstName"%','%"prop":%"lastName"%','%"prop":%"line1"%','%"prop":%"line2"%','%"prop":%"locality"%','%"prop":%"middleName"%','%"prop":%"otherApplicants"%'
                    ,'%"prop":%"personId"%','%"prop":%"postalCode"%','%"prop":%"socSecNumber"%','%"prop":%"state"%','%"prop":%"tenantId"%','%"prop":%"unparsedAddress"%','%"prop":%"zip5"%')
    ) AS dataChange ON dataChange."requestId" = req.id
    , LATERAL flatten(input => parse_json(applicantData: applicants)) AS fl
