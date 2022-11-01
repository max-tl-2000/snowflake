/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** GoogleSheets
dbt run --select customer_specific.maximus.imports.PortfolioEditorial --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='Portfolio Editorial') }}

SELECT TO_DATE(REPORTING_MONTH,'MM/DD/YY')::TIMESTAMP_NTZ AS "reportingmonth"
    , CONTACTS_GENERATED AS "contactsgenerated"
    , TOURS_AND_SALES AS "toursandsales"
    , CONTACTS_CONVERSION AS "contactsconversion"
    , TOUR_CONVERSION AS "tourconversion"
    , PROGRAM_PERFORMANCE AS "programperformance"
    , MARKETING_COSTS AS "marketingcosts"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538"."PORTFOLIOEDITORIAL"
