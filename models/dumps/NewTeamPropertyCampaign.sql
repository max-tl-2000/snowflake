/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.NewTeamPropertyCampaign --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "client": "customernew"}'
dbt run --select dumps.NewTeamPropertyCampaign --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "client": "maximus"}'
dbt run --select dumps.NewTeamPropertyCampaign --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "client": "glick"}'
*/

{{ config(alias='New Team Property Campaign') }}

SELECT prop.name AS "propertyCode"
    , t.displayName AS "Team"
    , prog.name AS "campaignName"
    , prog.displayName AS "campaignDisplayName"
    , tpp.commDirection AS "commDirection"
FROM {{ var("source_tenant") }}.TEAMPROPERTYPROGRAM tpp
INNER JOIN {{ var("source_tenant") }}.PROGRAMS prog ON prog.id = tpp.programId
INNER JOIN {{ var("source_tenant") }}.TEAMS t ON t.id = tpp.teamId
INNER JOIN {{ var("source_tenant") }}.PROPERTY prop ON prop.id = tpp.propertyId
