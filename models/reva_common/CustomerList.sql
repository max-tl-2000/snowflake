/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select reva_common.CustomerList --vars '{"target_schema": "SNOW_REVA_COMMON"}'
*/

{{ config(alias='CustomerList') }}

SELECT
    cl.CUSTOMER_NAME_QUICK_BOOKS AS "CustomerNameQuickBooks"
  , cl.PROPERTY_NAME AS "Property Name"
  , cl.TENANT AS "Tenant"
  , CASE WHEN rs.PROPERTY_NAME IS NOT NULL THEN 1 ELSE 0 END AS "QuickBooksMatchFADV"
FROM "RAW"."REVA_COMMON"."CUSTOMERLIST" AS cl
  LEFT JOIN
    ( SELECT DISTINCT PROPERTY_NAME
      FROM "RAW"."REVA_COMMON"."REVATECHNOLOGIESSCREENINGINV"
     ) AS rs ON cl.PROPERTY_NAME = rs.PROPERTY_NAME
