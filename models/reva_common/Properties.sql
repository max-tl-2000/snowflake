/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select reva_common.Properties --vars '{"target_schema": "SNOW_REVA_COMMON"}'
*/

{{ config(alias='Properties') }}

SELECT DISTINCT
      PROPERTY AS "Property"
    , TENANT AS "Tenant"
FROM "RAW"."REVA_COMMON"."PROPERTIES"
