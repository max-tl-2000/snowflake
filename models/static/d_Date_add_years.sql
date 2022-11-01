/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select static.d_Date_add_years --vars '{"target_schema": "SNOW_REVA_COMMON"}'
*/

{{ config(alias='d_Date_add_years') }}
{{ config(
  post_hook='
              --MERGE INTO ANALYTICS.SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8."d_Date" AS dest -- run this for Centerspace
              --MERGE INTO ANALYTICS.SNOW_9F27B14E_6973_48A5_B746_434828265538."d_Date" AS dest -- run this for Maximus
              MERGE INTO ANALYTICS.SNOW_18B4A573_560E_4755_8316_8E99AEFB004E."d_Date" AS dest -- run this for Glick
              USING ANALYTICS.SNOW_REVA_COMMON."d_Date_add_years" AS src
              ON
                    src.DATEKEY = dest.DATEKEY
              WHEN NOT MATCHED THEN INSERT
                    (DATEKEY, ONLYDATE, FULLDATE, YEAR, QUARTER, QUARTERNAME, MONTH, MONTHNAME, YEARMONTH, WEEK, WEEKNAME, YEARWEEK, DAYOFYEAR, DAYOFWEEK, DAYOFMONTH,
                     DAYNAME, FIRSTDAYOFMONTH, LASTDAYOFMONTH, FIRSTDAYOFWEEK, LASTDAYOFWEEK, ISHOLIDAY, ISWEEKEND, HOLIDAYNAME, "onlyDate2", "onlyDate3", "onlyDate4", "onlyDate5")
              VALUES
                    (DATEKEY, ONLYDATE, FULLDATE, YEAR, QUARTER, QUARTERNAME, MONTH, MONTHNAME, YEARMONTH, WEEK, WEEKNAME, YEARWEEK, DAYOFYEAR, DAYOFWEEK, DAYOFMONTH,
                     DAYNAME, FIRSTDAYOFMONTH, LASTDAYOFMONTH, FIRSTDAYOFWEEK, LASTDAYOFWEEK, ISHOLIDAY, ISWEEKEND, HOLIDAYNAME, "onlyDate2", "onlyDate3", "onlyDate4", "onlyDate5")
            ;
            --DROP VIEW "ANALYTICS"."SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8"."d_Date_add_years";
             '
) }}

SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INTEGER AS dateKey,
    datum AS onlyDate,
    datum::TIMESTAMP_NTZ AS fullDate,
    EXTRACT(YEAR FROM datum) AS year,
	EXTRACT(quarter FROM datum) AS quarter,
	EXTRACT(YEAR FROM datum)::text || '-Q' || EXTRACT(quarter FROM datum)::text AS quarterName,
	EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum,'MMMM') AS monthName,
    to_char(datum, 'yyyymm')::integer AS yearMonth,
	EXTRACT(week FROM datum) AS week,
	'Week ' || EXTRACT(week FROM datum)::text AS weekName,
    EXTRACT(YEAR FROM datum) || CASE WHEN EXTRACT(week FROM datum) < 9 then '0' else '' end || EXTRACT(week FROM datum) as yearWeek,
	EXTRACT(doy FROM datum) AS DayOfYear,
	EXTRACT(dow FROM datum) AS dayOfWeek,
	date_part('day', datum) AS dayOfMonth,
	dayname(datum) || CASE WHEN dayname(datum) in ('Mon','Sun', 'Fri') then 'day'
                           WHEN dayname(datum) in ('Tue', '') then 'sday'
                           WHEN dayname(datum) in ('Wed') then 'nesday'
                           WHEN dayname(datum) in ('Thu') then 'rsday'
                           WHEN dayname(datum) in ('Sat') then 'urday'
                           END AS dayName,
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER AS firstDayOfMonth,
    last_day(datum, 'month') AS lastDayOfMonth,
	datum + (1 - EXTRACT(dow_iso FROM datum))::INTEGER AS firstDayOfWeek,
	datum + (7 - EXTRACT(dow_iso FROM datum))::INTEGER AS lastDayOfWeek,
	'No' AS isHoliday,
	CASE WHEN EXTRACT(dow_iso FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS isWeekend,
    'Holiday Name' as HolidayName,
    datum AS "onlyDate2",
    datum AS "onlyDate3",
    datum AS "onlyDate4",
    datum AS "onlyDate5"
FROM (
	-- There are 2 leap years in range 2024-2029, so calculate 365 * 5 + 2 records - total 5 years
	SELECT dateadd(day, seq4(), '2024-01-02'::DATE) as datum
    FROM TABLE (generator(rowcount => 1827))
     ) DQ
ORDER BY 1
