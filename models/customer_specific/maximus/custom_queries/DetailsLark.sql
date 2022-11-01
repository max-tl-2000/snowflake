/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.DetailsLark --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='Details-Lark') }}

-- depends on: {{ ref('PartyDump') }}
-- depends on: {{ ref('SourceTranslation') }}

SELECT bs.prop AS "Property"
    , bs.TYPE AS "Type"
    , bs.count AS "Prospect"
    , CAST(bs.sort_order AS INTEGER) AS "Sort Order"
    , TO_DATE(left(bs.DT_DATE, len(bs.DT_DATE)-8) , 'MON DD YYYY') AS "Date"
    , bs.contact_type AS "Contact Type"
    , bs.s_rating AS "Rating"
    , bs.source AS "Source"
    , bs.agent AS "Agent"
    , lease_type AS "field10"
    , bedrooms AS "field11"
    , student AS "field12"
    , CASE
        WHEN TYPE = 'Contact'
            THEN 1
        ELSE 0
        END AS "isContact"
    , CASE
        WHEN TYPE = 'Show'
            THEN 1
        ELSE 0
        END "isShow"
    , CASE
        WHEN TYPE = 'Sale'
            THEN 1
        ELSE 0
        END "isSale"
    , CASE
        WHEN s_rating IN ('gold', 'Gold')
            THEN '1-Gold'
        WHEN s_rating IN ('silver', 'Silver')
            THEN '2-Silver'
        WHEN s_rating IN ('bronze', 'Bronze')
            THEN '3-Bronze'
        WHEN s_rating IN ('prospect', 'Prospect')
            THEN '4-Prospect'
        WHEN s_rating IN ('unqualified', 'Unqualified')
            THEN '5-Unqualified'
        ELSE s_rating
        END AS "LeadScore"
    , CASE
        WHEN s_rating IN ('gold', 'Gold', 'silver', 'Silver', 'bronze', 'Bronze')
            THEN 'Yes'
        ELSE 'No'
        END "QualifiedLead"
    , CASE
        WHEN TYPE IN ('show', 'Show')
            THEN '3-Tour'
        WHEN TYPE IN ('sale', 'Sale')
            THEN '4-Sale'
        WHEN "QualifiedLead" = 'Yes'
            THEN '1-Qualified'
        WHEN "QualifiedLead" = 'No'
            THEN '2-Not Qualified'
        END "ForDaily"
    , CASE
        WHEN bs.prop IS NULL
            THEN 'No Property'
        ELSE Rtrim(Ltrim(bs.prop))
        END "PropertyNN"
    , CASE
        WHEN agent = 'Andreas Vathis'
            THEN 'Andy Vathis'
        ELSE agent
        END AS "AgentClean"
    , CASE
        WHEN "QualifiedLead" = 'Yes'
            THEN 1
        ELSE 0
        END "isQualified"
    , CASE
        WHEN "QualifiedLead" = 'No'
            THEN 1
        ELSE 0
        END AS "NotQualified"
    , CASE
        WHEN bs.source IS NULL
            THEN 'Sharon Green website'
        WHEN bs.source = 'Reva'
            THEN 'Agent Entered'
        WHEN bs.source = 'google-searchPaid'
            THEN 'Google paid ad'
        WHEN bs.source = 'website-property'
            THEN 'Property website'
        ELSE bs.source
        END "SourceNonNull"
    , CASE
        WHEN lease_type = 'FAIR_MARKET' OR lease_type = 'Traditional'
            THEN 'Traditional'
        WHEN lease_type = 'Corporate'
            THEN 'Corporate'
        WHEN lease_type = 'Section8'
            THEN 'Section 8'
        WHEN lease_type = 'Employee'
            THEN 'Employee'
        WHEN lease_type = 'GoodSam'
            THEN 'Good Samaritan'
        WHEN lease_type = 'Student'
            THEN 'Student'
        ELSE 'Not Yet Determined'
        END "LeaseTypeClean"
    , date_from_parts(date_part('year', TO_DATE(left(bs.DT_DATE, len(bs.DT_DATE)-8) , 'MON DD YYYY')), date_part('month', TO_DATE(left(bs.DT_DATE, len(bs.DT_DATE)-8) , 'MON DD YYYY')), 1) "ReportingMonth"
    , DAYOFMONTH(CURRENT_TIMESTAMP()) "currentDayNo"
    , pd."QQStarted" AS "QQStartedFromParty"
    , st."DB Source" AS "DBSourceLookup"
    , CASE
        WHEN "DBSourceLookup" IS NULL
            THEN 'Internet'
        ELSE "DBSourceLookup"
        END "DBSourceFInal"
    , CASE
        WHEN "DBSourceLookup" IS NULL
            THEN 'Add'
        ELSE 'Exists'
        END "AddToSourceTranslation"
FROM "RAW"."CUST_9F27B14E_6973_48A5_B746_434828265538".BI_SALES AS bs
	LEFT JOIN (select distinct "pCode", "QQStarted" from {{ var("target_schema") }}."PartyDump") AS pd ON bs.count = pd."pCode"
    LEFT JOIN {{ var("target_schema") }}."Source Translation" AS st ON
        CASE
        WHEN bs.source IS NULL
            THEN 'Sharon Green website'
        WHEN bs.source = 'Reva'
            THEN 'Agent Entered'
        WHEN bs.source = 'google-searchPaid'
            THEN 'Google paid ad'
        WHEN bs.source = 'website-property'
            THEN 'Property website'
        ELSE bs.source
        END = st."BISource"
