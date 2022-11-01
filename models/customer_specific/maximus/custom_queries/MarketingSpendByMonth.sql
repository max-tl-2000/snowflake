/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
    dbt run --select customer_specific.maximus.custom_queries.MarketingSpendByMonth --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='MarketingSpendByMonth') }}

-- depends on: {{ ref('MarketingSpend') }}

SELECT "Property"
    , "Reporting Month"
    , SUM("Budget"::DECIMAL(10,2)) AS "budget"
    , SUM("Actual"::DECIMAL(10,2)) AS "actual"
FROM {{ var("target_schema") }}."MarketingSpend"
GROUP BY "Property"
    , "Reporting Month"
