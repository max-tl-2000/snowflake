/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.DailyStatisticsProperty --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='DailyStatisticsProperty') }}

-- depends on: {{ ref('DailyStatisticsMaximus') }}

SELECT d."Report Date" AS "Report Date"
    , d."Property" AS "Property"
    /* Summary Ratios */
    , CASE WHEN sum(d."Occupied") <> 0 then (CAST(sum(d."Occupied") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d."DOM") AS NUMBER(38,16)))::varchar else 0 end AS "Occupancy"
    , CASE WHEN sum(d."Occupied") <> 0 then (CAST(sum(d."Occupied" - d."OnNotice30" + d."Reserved30") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d."DOM") AS NUMBER(38,16)))::varchar else 0 end AS "Occupancy30"
    , CASE WHEN sum(d."Occupied") <> 0 then (CAST(sum(d."Occupied" - d."OnNotice60" + d."Reserved60") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d."DOM") AS NUMBER(38,16)))::varchar else 0 end AS "Occupancy60"
    /* Unit counts */
    , sum(d."Total Units")::varchar  AS "Total Units"
    , sum(d."Occupied")::varchar AS "Occupied excluding Notice"
    , CASE WHEN sum(d."Occupied") <> 0 then (
        CASE
        WHEN ((CAST(sum(d."Occupied") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d."DOM") AS NUMBER(38,16))) < 0.95) OR ((CAST(sum(d."Occupied") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d."DOM") AS NUMBER(38,16))) < 0.93) OR ((CAST(sum(d."Occupied") AS NUMBER(38,16)) / CAST(sum(d."Total Units" - d.DOM) AS NUMBER(38,16))) < 0.90)
            THEN 1
        ELSE 0
        END)
      ELSE 0
      END AS "watchList"
    , date_trunc('day', CONVERT_TIMEZONE('{{ var("timezone") }}',d."Report Date"))::TIMESTAMP_NTZ  as "reportDateOnly"
FROM {{ var("target_schema") }}."DailyStatistics" AS d
GROUP BY d."Property"
    , d."Report Date"