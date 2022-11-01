/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** MS EXCEL
dbt run --select customer_specific.maximus.imports.LROforUAD --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='LRO for UAD') }}

SELECT property AS "Property"
    , unittype AS "UnitType"
    , unit AS "Unit"
    , REPLACE(rent, ',', '')::INTEGER AS "Rent"
    , REPLACE(SQFT, ',', '')::INTEGER AS "SQFT"
    , STATUS AS "Status"
    , dns AS "DNS"
    , CASE
	    WHEN DNS = 'No' THEN ' '
        ELSE DNS
      END AS "DNSDisplay"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".LRO12MonthPricesREVA_sutro
