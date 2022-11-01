/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.PropertyStatsMonth --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='PropertyStatsMonth') }}

-- depends on: {{ ref('PartyStatsTime') }}
-- depends on: {{ ref('ReportingMonth') }}
-- depends on: {{ ref('MarketingSpend') }}
-- depends on: {{ ref('MarketingSpendByMonth') }}

SELECT r."ReportingMonth" AS "Report Date"
    , p."Property" AS "Property"
    , SUM(CASE
            WHEN p."Include in Reporting" = 'Include' AND p."Contact" = 1
                THEN 1
            ELSE 0
            END) AS "Contacts"
    , SUM(CASE
            WHEN p."Include in Reporting" = 'Include' AND p."Qualified" = 1
                THEN 1
            ELSE 0
            END) AS "Qualified"
    , SUM(CASE
            WHEN p."Include in Reporting" = 'Include' AND p."Tour" = 1
                THEN 1
            ELSE 0
            END) AS "Tour"
    , SUM(CASE
            WHEN p."Include in Reporting" = 'Include' AND p."Applied" = 1
                THEN 1
            ELSE 0
            END) AS "Applied"
    , SUM(CASE
            WHEN p."Include in Reporting" = 'Include' AND p."Sign" = 1
                THEN 1
            ELSE 0
            END) AS "Sign"
    , MAX(m."budget")::NUMBER(10,2) AS "Budget"
    , MAX(m."actual")::NUMBER(10,2) AS "Actual"
    , MAX(g."Sales Goal") AS "Sales Goal"
    , MAX(g."Contacts Goal") AS "Contacts Goal"
    , AVG(p."DaysToClose")::NUMBER(20,10) AS "Average DTC"
    , MEDIAN(p."DaysToClose")::NUMBER(20,10)AS "Med DTC"
FROM {{ var("target_schema") }}."ReportingMonth" AS r
LEFT JOIN {{ var("target_schema") }}."PartyStatsTime" AS p ON DATE (r."ReportingMonth") = DATE_FROM_PARTS(DATE_PART('year', p."Report Date"), DATE_PART('month', p."Report Date"), 1)
LEFT JOIN {{ var("target_schema") }}."MarketingSpendByMonth" m ON m."Reporting Month" = r."ReportingMonth" AND m."Property" = p."Property"
LEFT JOIN (
    SELECT pg."Property"
        , pg."Month"
        , SUM(pg."Sales Goal") AS "Sales Goal"
        , SUM(pg."Contacts Goal") AS "Contacts Goal"
    FROM {{ var("target_schema") }}."PropertyGoals" AS pg
    GROUP BY pg."Property"
        , pg."Month"
    ) AS g ON g."Month" = r."ReportingMonth" AND g."Property" = p."Property"
GROUP BY r."ReportingMonth"
    , p."Property"
