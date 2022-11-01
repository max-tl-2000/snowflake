/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** manual data
dbt run --select customer_specific.maximus.imports.UtilizationMetrics --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='Utilization Metrics') }}

SELECT Property AS "Property"
    , UnitType AS "UnitType"
    , Unit AS "Unit"
    , BlockName AS "BlockName"
    , Type AS "Type"
    , Reno AS "Reno"
    , Units::INTEGER AS "#Units"
    , REPLACE(SQFT, ',', '')::INTEGER AS "SQFT"
    , IsOccupied::INTEGER AS "IsOccupied"
    , IsOccupied_::INTEGER AS "IsOccupied%"
    , REPLACE(Rent, ',', '')::NUMERIC(10,2) AS "Rent"
    , REPLACE(Rent_SQFT, ',', '')::NUMERIC(10,2) AS "Rent/SQFT"
    , REPLACE(LTL, ',', '')::NUMERIC(10,2) AS "LTL"
    , REPLACE(LengthofStay, ',', '')::NUMERIC(10,2) AS "LengthofStay"
    , Median::INTEGER AS "Median"
    , REPLACE(IsTraditional, ',', '')::NUMERIC(10,2) AS "IsTraditional"
    , REPLACE(TradRent, ',', '')::NUMERIC(10,2) AS "TradRent"
    , REPLACE(TradLenghtofStay, ',', '')::NUMERIC(10,2) AS "TradLenghtofStay"
    , TradMedian::INTEGER AS "TradMedian"
    , IsStudent::INTEGER AS "IsStudent"
    , REPLACE(StudRent, ',', '')::NUMERIC(10,2) AS "StudRent"
    , REPLACE(StudLengthofStay, ',', '')::NUMERIC(10,2) AS "StudLengthofStay"
    , StudMedian::INTEGER AS "StudMedian"
    , IsCorporate::INTEGER AS "IsCorporate"
    , REPLACE(CorpRent, ',', '')::NUMERIC(10,2) AS "CorpRent"
    , REPLACE(CorpLengthofStay, ',', '')::NUMERIC(10,2) AS "CorpLengthofStay"
    , IsSec8::INTEGER AS "IsSec8"
    , REPLACE(Sec8Rent, ',', '')::NUMERIC(10,2) AS "Sec8Rent"
    , REPLACE(Sec8LengthofStay, ',', '')::NUMERIC(10,2) AS "Sec8LengthofStay"
    , IsGoodSam::INTEGER AS "IsGoodSam"
    , REPLACE(GSRent, ',', '')::NUMERIC(10,2) AS "GSRent"
    , REPLACE(GSLengthofStay, ',', '')::NUMERIC(10,2) AS "GSLengthofStay"
    , IsEmp::INTEGER AS "IsEmp"
    , REPLACE(EmpRent, ',', '')::NUMERIC(10,2) AS "EmpRent"
    , REPLACE(EmpLengthofStay, ',', '')::NUMERIC(10,2) AS "EmpLengthofStay"
    , IsOther::INTEGER AS "IsOther"
    , REPLACE(OtherRent, ',', '')::NUMERIC(10,2) AS "OtherRent"
    , REPLACE(OtherLengthofStay, ',', '')::NUMERIC(10,2) AS "OtherLengthofStay"
    , IsVacantRented::INTEGER AS "IsVacantRented"
    , IsVacantUnrented::INTEGER AS "IsVacantUnrented"
    , IsVacant::INTEGER AS "IsVacant"
    , DaysVacant::INTEGER AS "DaysVacant"
    , AvgDaysVacant::INTEGER AS "AvgDaysVacant"
    , IsVacant14Days::INTEGER AS "IsVacant14Days"
    , REPLACE(VacantRent, ',', '')::NUMERIC(10,2) AS "VacantRent"
    , REPLACE(VacantRent_SQFT, ',', '')::NUMERIC(10,2) AS "VacantRent/SQFT"
    , IsVacantRented2::INTEGER AS "IsVacantRented2"
    , IsNoticeRented::INTEGER AS "IsNoticeRented"
    , IsATR::INTEGER AS "IsATR"
    , IsLeasedOcc::INTEGER AS "IsLeasedOcc"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".UtilizationMetrics
