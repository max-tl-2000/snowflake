/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.SalesCycle --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='SalesCycle') }}

-- depends on: {{ ref('PartyDump') }}

SELECT pd."Property" AS "Property"
    , pd."signMonth"::TIMESTAMP_NTZ AS "Month Signed"
    , Avg(datediff(day,pd."PartyCreatedDate" , pd."signDate")) AS "Average Sales Days"
    , Median(datediff(day, pd."PartyCreatedDate",pd."signDate")) AS "Median Sales Days"
    , stddev(datediff(day, pd."PartyCreatedDate",pd."signDate"))::NUMBER(38,20) AS "StdDev Sales Days"
FROM {{ var("target_schema") }}."PartyDump" AS pd
GROUP BY pd."Property"
    , pd."signMonth"
