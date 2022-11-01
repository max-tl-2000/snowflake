/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** manual data
dbt run --select customer_specific.maximus.imports.ReportingAttributes --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "client": "maximus"}'
*/

{{ config(alias='ReportingAttributes') }}

SELECT 'cove' AS "PropertyName", 'The Cove LLC' AS "PropertyLegalName", 'North Bay' AS "Region", '' AS "SameStore", '' AS "RegionalManager", 'Maximus' AS "Customer"
UNION ALL SELECT 'lark', 'Serenity LLC', 'North Bay', '', '', 'Maximus'
UNION ALL SELECT 'sharon', 'Maximus RAR2 Sharon Green Owner LLC', 'South Bay', '', '', 'Maximus'
UNION ALL SELECT 'shore', 'South Shore Apartments', 'East Bay', '', '', 'Maximus'
UNION ALL SELECT 'swparkme', 'Parkmerced LLC', 'San Francisco', '', '', 'Maximus'
UNION ALL SELECT 'wood', 'Woodchase Apartment Homes', 'East Bay', '', '', 'Maximus'
