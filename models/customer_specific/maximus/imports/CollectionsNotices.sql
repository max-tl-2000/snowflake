/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** MS EXCEL
dbt run --select customer_specific.maximus.imports.CollectionsNotices --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='CollectionsNotices') }}

SELECT property AS "Property"
    , party AS "Party"
    , to_timestamp(datecreated, 'MM/DD/YY hh24:mi') AS "DateCreated"
    , type AS "Type"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".CollectionsNotices
