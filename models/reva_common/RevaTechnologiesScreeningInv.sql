/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select reva_common.RevaTechnologiesScreeningInv --vars '{"target_schema": "SNOW_REVA_COMMON"}'
*/

{{ config(alias='Reva Technologies Screening Inv') }}

-- depends on: {{ ref('StuckTransactions') }}
-- depends on: {{ ref('Properties') }}


SELECT CUSTOMER AS "Customer"
	, PROPERTY_NAME AS "Property Name"
	, APPLICANT AS "Applicant"
	, TO_DATE(DATE, 'MM/DD/YY')::TIMESTAMP_NTZ AS "Date"
	, FEE_DESCRIPTION AS "Fee Description"
	, APPLICATION_ID::INTEGER AS "Application ID"
	, APPLICANT_NO_::INTEGER AS "Applicant No."
	, BASE_FEE::NUMBER(20, 10) AS "Base Fee"
	, CASE WHEN SALES_TAX = '$ -' THEN '' ELSE SALES_TAX END AS "Sales Tax"
	, AMOUNT::NUMBER(20, 10) AS "Amount"
	, CREDIT AS "Credit"
	, REVISED_INVOICE::NUMBER(20, 10) AS "Revised Invoice"
	, TO_DATE(INVOICE_MONTH, 'MM/DD/YYYY')::TIMESTAMP_NTZ AS "Invoice Month"
	, MONTH(TO_DATE(DATE, 'MM/DD/YY')::TIMESTAMP_NTZ) AS "monthNum"
	, YEAR(TO_DATE(DATE, 'MM/DD/YY')::TIMESTAMP_NTZ) AS "YearNum"
	, CASE
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (16, 14, 17.75) THEN 15
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (8.75, 9.75, 9.25, 8.25, 8) THEN 9.5
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (4) THEN 8
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (20, 21.75) THEN 19
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (24) THEN 23
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (28) THEN 27
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (9.25, 17.75) THEN 16
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (16, 9.75) THEN 26
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (8) THEN 12
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (21.75) THEN 32
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (13.75) THEN 22
      ELSE AMOUNT
    END AS "CustomerChargeAmount"
	, CASE
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (16, 17.75, 14) THEN 'Resident Screening'
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (8.75, 9.75, 9.25, 8.25, 8) THEN 'Guarantor Screening'
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (4) THEN 'Refresh Screening Results'
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (20, 21.75, 24, 28) THEN 'Screening plus 1 or more Results Refresh'
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (16, 9.75) THEN 'Resident Screening'
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (9.25, 17.75) THEN 'Guarantor Screening'
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (8) THEN 'Refresh Sreening Results'
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (21.75, 13.75) THEN 'Screening plus 1 or more Results Refresh'
      ELSE 'No Description Found'
    END AS "ServiceDescription"
	, 'Screening' AS "Service"
	, APPLICANT::VARCHAR || ' ' || APPLICANT_NO_::VARCHAR || ' ' || FEE_DESCRIPTION::VARCHAR AS "TransactionDescription"
	, APPLICANT::VARCHAR || ' ' || APPLICANT_NO_::VARCHAR AS "ApplicantDescription"
	, 15 AS "Term"
	, 1 AS "Quantity"
	, "CustomerChargeAmount" AS "Rate"
	, CASE
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (16, 13) THEN 15
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (6.75, 9.75, 9.25, 6.25) THEN 9.5
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (8) THEN 12
      WHEN p."Tenant" = 'CUSTOMEROLD' AND AMOUNT IN (4) THEN 6
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (9.25) THEN 15
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (6.75) THEN 11
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (6.25) THEN 10
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (9.75) THEN 16
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (13) THEN 21
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (16) THEN 26
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (8) THEN 12
      WHEN p."Tenant" = 'Maximus' AND AMOUNT IN (4) THEN 6
      ELSE Amount
    END AS "CustomerChargeAmount2"
	, "CustomerChargeAmount2" AS "Rate2"
	, CASE WHEN st."applicantId" IS NULL THEN 0 ELSE 1 END AS "isStuck"
	, CASE
      WHEN FEE_DESCRIPTION = 'BS' THEN 'Resident Financial Screening'
      WHEN FEE_DESCRIPTION = 'CMNT' THEN 'Criminal'
      WHEN FEE_DESCRIPTION = 'EV' THEN 'Eviction'
      WHEN FEE_DESCRIPTION = 'FC' THEN 'Lease Forms'
      WHEN FEE_DESCRIPTION = 'GNBS' THEN 'Guarantor Financial Screening'
      WHEN FEE_DESCRIPTION = 'REV' THEN 'Full Revision'
      WHEN FEE_DESCRIPTION = 'REVON' THEN 'Online Revision'
      WHEN FEE_DESCRIPTION = 'WFBS' THEN 'Spouse Financial Screening'
      ELSE FEE_DESCRIPTION
    END AS "ItemDescription"
	, p."Tenant" AS "Company"
	, st.recordcount AS "isStuck2"
FROM "RAW"."REVA_COMMON"."REVATECHNOLOGIESSCREENINGINV" AS rs
LEFT JOIN "ANALYTICS"."SNOW_REVA_COMMON"."Properties" p ON rs.PROPERTY_NAME = p."Property"
LEFT JOIN (
	SELECT "applicantId"
		, count(1) AS recordcount
	FROM "ANALYTICS"."SNOW_REVA_COMMON"."StuckTransactions"
	GROUP BY "applicantId"
	) st ON rs.APPLICANT_NO_ = st."applicantId"
