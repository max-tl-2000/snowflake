/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.SalesStages --vars '{"target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"}'
dbt run --select dumps.SalesStages --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
dbt run --select dumps.SalesStages --vars '{"target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E"}'
*/

{{ config(alias='SalesStages') }}

SELECT '1-Prospect' AS "SalesStage"
UNION ALL SELECT '2-Contacts'
UNION ALL SELECT '3-Tour/Quoted'
UNION ALL SELECT '4-Applicant'
UNION ALL SELECT '5-Leasing'
UNION ALL SELECT '6-Future Resident'
UNION ALL SELECT '7-Resident'
