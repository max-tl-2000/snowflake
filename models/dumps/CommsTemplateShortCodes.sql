/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.CommsTemplateShortCodes --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.CommsTemplateShortCodes --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.CommsTemplateShortCodes --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='CommsTemplateShortCodes') }}

SELECT p.DISPLAYNAME AS "PropertyName"
    , tsc.SHORTCODE AS "TemplateShortCode"
    , ct.name AS "TemplateName"
    , ct.DISPLAYNAME AS "TemplateDisplayName"
    , COALESCE(ct.DESCRIPTION, 'N/A') AS "TemplateDescription"
    , COALESCE(ct.EMAILSUBJECT, 'N/A') AS "EmailSubject"
    , ct.EMAILTEMPLATE AS "EmailTemplate"
    , ct.SMSTEMPLATE AS "SmsTemplate"
FROM {{ var("source_tenant") }}."TEMPLATESHORTCODE" as tsc
    INNER JOIN {{ var("source_tenant") }}."PROPERTY" As p ON tsc.PROPERTYID = p.ID
    INNER JOIN {{ var("source_tenant") }}."COMMSTEMPLATE" ct ON tsc.TEMPLATEID = ct.ID
