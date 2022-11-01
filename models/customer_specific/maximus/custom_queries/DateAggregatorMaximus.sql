/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.DateAggregator --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='DateAggregator_maximus') }}
{{ config(
    pre_hook='DROP TABLE IF EXISTS "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DateAggregator"',
    post_hook='ALTER TABLE "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DateAggregator_maximus" RENAME TO "ANALYTICS"."SNOW_9F27B14E_6973_48A5_B746_434828265538"."DateAggregator"'
) }}

-- depends on: {{ ref('InventoryDataMaximus') }}

SELECT row_number() OVER (
        ORDER BY id."yeardaynum"
            , id."ReportingDate" DESC
        ) AS "theRank"
    , id."ReportingDate"
FROM {{ var("target_schema") }}."InventoryData" AS id
GROUP BY id."yeardaynum"
    , id."ReportingDate"
