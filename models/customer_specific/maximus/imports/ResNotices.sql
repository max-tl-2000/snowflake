/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** CSV
dbt run --select customer_specific.maximus.imports.ResNotices --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "client": "maximus"}'
*/

{{ config(alias='ResNotices') }}

SELECT PropCode AS "PropCode"
    , Tcode AS "Tcode"
    , TO_DATE(NoticeDate, 'MM/DD/YYYY')::TIMESTAMP_NTZ AS "NoticeDate"
    , NoticeReason AS "NoticeReason"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".RESNOTICES
