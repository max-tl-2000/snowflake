/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select reva_common.StuckTransactions --vars '{"target_schema": "SNOW_REVA_COMMON"}'
*/

{{ config(alias='StuckTransactions') }}

SELECT
    TENANT as "Tenant"
  --, convert_timezone('UTC', to_timestamp(left(FIRST_REQUEST,19), 'YYYY-MM-DD hh24:mi:ss'))::TIMESTAMP_NTZ  AS "firstRequest"
  , to_timestamp_tz(REPLACE(FIRST_REQUEST,'PDT','-07:00'), 'YYYY-MM-DD hh24:mi:ss TZH:TZM')  AS "firstRequest"
  , PROPERTY AS "property"
  , APPLICATION_ID AS "applicationId"
  , APPLICANT_ID AS "applicantId"
  , APPLICATION_NAME AS "applicationName"
  , TYPE AS "type"
  , DUP_TEST AS "Dup Test"
  , to_timestamp_tz(REPLACE(FIRST_REQUEST,'PDT','-07:00'), 'YYYY-MM-DD hh24:mi:ss TZH:TZM')::DATE AS "firstRequestDate"
  , 1 AS "recordcount"
from "RAW"."REVA_COMMON"."STUCKTRANSACTIONS"
