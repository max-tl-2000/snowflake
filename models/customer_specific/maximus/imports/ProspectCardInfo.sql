/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** GoogleSheets
dbt run --select customer_specific.maximus.imports.ProspectCardInfo --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='ProspectCardInfo') }}

SELECT TIMESTAMP::TIMESTAMP_NTZ AS "Timestamp"
    , ENTER_PARTY_URL AS "Enter Party URL"
    , AGE_CHOOSE_ALL_THAT_APPLY AS "Age - Choose all that apply"
    , HOUSEHOLD_COMPOSITION_CHOOSE_THE_MOST_CORRECT AS "Household Composition - Choose the most correct"
    , COLLEGE_STUDENT_Y_N_ AS "College Student (Y/N)"
    , TOTAL_PARTY_ANNUAL_INCOME_CHOOSE_THE_MOST_CORRECT AS "Total Party Annual Income - Choose the most correct"
    , EMPLOYER AS "Employer"
    , CURRENT_RESIDENCE_LOCATION_CITY AS "Current Residence Location - City"
    , CURRENT_RESIDENCE_LOCATION_STATE_OR_COUNTRY AS "Current Residence Location - State or Country"
    , CURRENT_RESIDENCE_RENTAL_Y_N_ AS "Current Residence - Rental (Y/N)"
    , IF_RENTING_WHICH_APARTMENT_COMMUNITY_IF_KNOWN_ AS "If Renting, which apartment community (if known)?"
    , REASON_FOR_MOVING_TO_PROPERTY_LOCATION_SELECT_ALL_THAT_APPLY AS "Reason for moving to property location - select all that apply"
    , TIME_SPENT_LOOKING_FOR_NEW_HOME AS "Time spent looking for new home"
    , MOST_IMPORTANT_FACTORS_IN_CHOOSING_NEW_HOME_CHOOSE_ALL_THAT_APPLY AS "Most Important Factors in Choosing New Home - Choose all that apply"
    , PREFERRED_BUILDING_TYPE AS "Preferred Building Type"
    , IMPORTANT_APARTMENT_AMENITIES_SELECT_ALL_THAT_APPLY_ AS "Important Apartment Amenities - Select All that Apply:"
    , IMPORTANT_COMMUNITY_AMENITIES_SELECT_ALL_THAT_APPLY_ AS "Important Community Amenities - Select all that apply:"
    , INTEREST_IN_CURATED_EVENTS_SELECT_ALL_THAT_APPLY_ AS "Interest in Curated Events - Select all that apply:"
    , CLOSE_PROXIMITY_TO_ AS "Close proximity to:"
    , FACTORS_IN_CHOOSING_A_NEIGHBORHOOD_LOCATION_ AS "Factors in choosing a Neighborhood/location:"
    , FORM_RESPONSE_EDIT_URL AS "Form Response Edit URL"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538"."PROSPECTINFOCARD"
