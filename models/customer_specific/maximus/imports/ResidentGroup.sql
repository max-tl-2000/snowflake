/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** MS Excel
dbt run --select customer_specific.maximus.imports.ResidentGroup --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='ResidentGroup') }}

SELECT PropCode AS "PropCode"
    , UnitCode AS "UnitCode"
    , LeaseType AS "LeaseType"
    , ResCode AS "ResCode"
    , ResStatus AS "ResStatus"
    , ResMTM::INTEGER AS "ResMTM"
    , TO_TIMESTAMP(LeaseFrom, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ AS "LeaseFrom"
    , TO_TIMESTAMP(MoveIn, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ AS "MoveIn"
    , TO_TIMESTAMP(Notice, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ AS "Notice"
    , TO_TIMESTAMP(LeaseTo, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ AS "LeaseTo"
    , TO_TIMESTAMP(MoveOut, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ AS "MoveOut"
    , CASE
        WHEN TO_TIMESTAMP(MoveOut, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ < TO_TIMESTAMP(LeaseTo, 'MM/DD/YY HH:MI')::TIMESTAMP_NTZ
            THEN 1
        ELSE 0
        END "EarlyTermination"
    , CASE
        WHEN ResMTM = '0'
            THEN 0
        ELSE 1
        END "MTMLease"
    , 1 AS "RecordCount"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".RESIDENTGROUP
