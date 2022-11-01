/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.NewCampaignsAndSources --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "client": "customernew", "timezone": "America/Chicago"}'
dbt run --select dumps.NewCampaignsAndSources --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "client": "maximus", "timezone": "America/Los_Angeles"}'
dbt run --select dumps.NewCampaignsAndSources --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='New Campaigns and Sources') }}

SELECT prog.id AS "campaignId"
    , prog.name AS "campaignName"
    , prog.displayName AS "campaignDisplayName"
    , prog.reportingDisplayName AS "programReportingDisplayName"
    , prog.path AS "programPath"
    , s.name AS "sourceName"
    , s.displayName AS "sourceDisplayName"
    , '[Deprecated]' AS "sourceMedium"
    , s.type AS "sourceType"
    , prog.directEmailIdentifier || '@' || '{{ var("client") }}' || '.mail.reva.tech' AS "directEmailIdentifier"
    , TRANSLATE(prog.outsideDedicatedEmails,'"[]','') AS "outsideDedicatedEmails"
    , prog.displayEmail AS "displayEmail"
    , prog.directPhoneIdentifier AS "directPhoneIdentifier"
    , prog.displayPhoneNumber AS "displayPhoneNumber"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',prog.created_at)::TIMESTAMP_NTZ AS "programCreatedDate"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(prog.endDate, '2200-01-01'))::TIMESTAMP_NTZ AS "programEndDate"
    , CONVERT_TIMEZONE('{{ var("timezone") }}',COALESCE(prog.endDateSetOn, '2200-01-01'))::TIMESTAMP_NTZ AS "programEndDateSetOn"
    , prog.displayUrl AS "displayUrl"
    , CASE
        WHEN prog.NAME LIKE '%via-website%'
            THEN '/?rtm_campaign=' || prog.NAME
        ELSE NULL
        END AS "LandingPage"
    , CASE
        WHEN "programEndDate" = '2020-01-01'
            THEN 'Active'
        ELSE 'Inactive'
        END AS "programState"
FROM {{ var("source_tenant") }}.PROGRAMS AS prog
INNER JOIN {{ var("source_tenant") }}.SOURCES AS s ON s.id = prog.sourceId

