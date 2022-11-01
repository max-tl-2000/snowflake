/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.SourceList --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.SourceList --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.SourceList --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='SourceList') }}

-- depends on: {{ ref('NewCampaignsAndSources') }}

SELECT DISTINCT cs."sourceDisplayName" AS "Source Display Name"
    , cs."sourceName" AS "Source Internal Reva Name"
    , cs."sourceType" AS "Source Type"
FROM {{ var("target_schema") }}."New Campaigns and Sources" AS cs
