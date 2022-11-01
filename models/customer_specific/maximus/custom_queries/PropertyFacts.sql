/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.PropertyFacts --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='PropertyFacts') }}

-- depends on: {{ ref('PartyStatsTime') }}
-- depends on: {{ ref('Property') }}
-- depends on: {{ ref('PropertyGoals') }}

SELECT prop."name" AS "Property"
    , pg."Month"
    , sum(pst."Sign") AS "Sale"
    , max(pg."Sales Goal") AS "SalesGoal"
FROM {{ var("target_schema") }}."Property" AS prop
LEFT OUTER JOIN {{ var("target_schema") }}."PartyStatsTime" AS pst ON pst."Property" = prop."name"
LEFT OUTER JOIN {{ var("target_schema") }}."PropertyGoals" AS pg ON pg."Property" = prop."name" AND pg."Month" = DATE_FROM_PARTS(DATE_PART('year', pst."Report Date"), DATE_PART('month', pst."Report Date"), 1)
GROUP BY prop."name"
    , pg."Month"
ORDER BY prop."name"
    , pg."Month"
