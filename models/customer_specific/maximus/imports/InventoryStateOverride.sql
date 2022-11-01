/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** MS EXCEL
dbt run --select customer_specific.maximus.imports.InventoryStateOverride --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='InventoryStateOverride') }}

SELECT 1 AS "Count"
    , property AS "Property"
    , unittype AS "Unit Type"
    , unit AS "Unit"
    , leasetype AS "Lease Type"
    , LeasedUtilStatus AS "Leased Util Status"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".InventoryStateOverride
