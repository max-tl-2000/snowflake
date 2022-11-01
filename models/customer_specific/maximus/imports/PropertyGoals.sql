/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** GoogleSheets
dbt run --select customer_specific.maximus.imports.PropertyGoals --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='PropertyGoals') }}

SELECT property AS "Property"
    , to_date(month, 'MM/DD/YY') AS "Month"
    , contacts_goal::number(20,10) AS "Contacts Goal"
    , ct_qc_goal::number(20,10) AS "CtQC% Goal"
    , tt_a_goal::number(20,10) AS "TtA% Goal"
    , tt_s_goal::number(20,10) AS "TtS% Goal"
    , ct_s_goal::number(20,10) AS "CtS% Goal"
    , st_m_goal::number(20,10) AS "StM% Goal"
    , qcontacts_goal::number(20,10) AS "Qcontacts Goal"
    , tour_goal::number(20,10) AS "Tour Goal"
    , apply_goal::number(20,10) AS "Apply Goal"
    , sales_goal::number(20,10) AS "Sales Goal"
    , move_ins_goal::number(20,10) AS "Move Ins Goal"
    , ct_t_goal::number(20,10) AS "CtT% Goal"
    , qct_t_goal::number(20,10) AS "QCtT% Goal"
    , qct_s_goal::number(20,10) AS "QCtS% Goal"
    , target_occupancy::number(20,10) AS "Target Occupancy"
    , _30_day_occupancy::number(20,10) AS "30-Day Occupancy"
    , _60_day_occupancy::number(20,10) AS "60-Day Occupancy"
    , (sales_goal / 30)::number(20,10) AS "SalesGoalDaily"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538"."PROPERTYGOALS"
