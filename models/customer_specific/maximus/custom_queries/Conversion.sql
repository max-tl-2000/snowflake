/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.Conversion --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='Conversion') }}

-- depends on: {{ ref('PropertyStatsMonth') }}

SELECT psm."Property" AS "Property"
    , psm."Report Date" AS "Report Month"
    , div0(Sum(psm."Sign"::DECIMAL(10, 2)), sum(psm."Contacts"::DECIMAL(10, 2))) AS "Lead to Sign"
    , div0(Sum(psm."Tour"::DECIMAL(10, 2)), sum(psm."Contacts"::DECIMAL(10, 2))) AS "Lead to Tour"
    , div0(Sum(psm."Sign"::DECIMAL(10, 2)), sum(psm."Tour"::DECIMAL(10, 2))) AS "Tour to Sign"
FROM {{ var("target_schema") }}."PropertyStatsMonth" AS psm
GROUP BY psm."Report Date"
    , psm."Property"
