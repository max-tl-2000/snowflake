/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.UserSecurity --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.UserSecurity --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.UserSecurity --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='User Security') }}

SELECT DISTINCT u.email AS "userEmail"
    , prop.name AS "property"
FROM {{ var("source_tenant") }}.Users AS u
INNER JOIN {{ var("source_tenant") }}.TeamMembers AS tm ON tm.userId = u.id
INNER JOIN {{ var("source_tenant") }}.Teams AS t ON t.id = tm.teamId
INNER JOIN {{ var("source_tenant") }}.TeamProperties AS tp ON tp.teamId = t.id
INNER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = tp.propertyId
WHERE u.email <> 'admin@reva.tech' AND tm.inactive = 'false'

UNION ALL

SELECT prop.name AS "userEmail"
    , prop.name AS "property"
FROM {{ var("source_tenant") }}.Property AS prop
ORDER BY 1, 2
