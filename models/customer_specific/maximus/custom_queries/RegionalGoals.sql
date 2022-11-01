/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.RegionalGoals --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='RegionalGoals') }}

-- depends on: {{ ref('ReportingAttributes') }}
-- depends on: {{ ref('PropertyGoals') }}

SELECT r."Region" AS "Region"
    , p."Month" AS "Goal Month"
    , dayofweek(current_timestamp) AS "CurrentDayNo"
    , sum(p."Contacts Goal") AS "Contacts Goal"
    , sum(p."Qcontacts Goal") AS "QContacts Goal"
    , sum(p."Tour Goal") AS "Tour Goal"
    , sum(p."Sales Goal") "Sales Goal"
    , sum(p."Move Ins Goal") "Move IN Goal"
    , sum(p."CtQC% Goal") "CtQC% Goal"
    , sum(p."CtT% Goal") "CtT% Goal"
    , sum(p."QCtT% Goal") "QCtT% Goal"
    , sum(p."CtS% Goal") "CtS% Goal"
    , sum(p."QCtS% Goal") "QCtS% Goal"
    , sum(p."TtS% Goal") AS "TtS% Goal"
    , sum(p."Sales Goal") / 30 AS "Daily Sales Goal"
FROM {{ var("target_schema") }}."PropertyGoals" AS p
LEFT JOIN {{ var("target_schema") }}."ReportingAttributes" AS r ON p."Property" = r."PropertyName"
GROUP BY r."Region"
    , p."Month"
