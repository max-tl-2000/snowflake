/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.CommResponseTime --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.CommResponseTime --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.CommResponseTime --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='CommResponseTime') }}

with party_comms AS (
    SELECT c.id,
        fl.value::varchar as party_id,
        type,
        direction,
        COALESCE(c.message: isMissed, 'false') as "isMissed",
        COALESCE(c.message: isVoiceMail, 'false') as "isVoiceMail",
        CASE WHEN COALESCE(c.message: isMissed, 'false') = 'false'
                AND COALESCE(c.message: isVoiceMail, 'false') = 'false'
                AND c.message: duration is not null
                AND EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT
             THEN 1
             ELSE 0
        END as connected,
        c.created_at as orig_created_at,
        t.name as "teamName",
        t.DISPLAYNAME as "teamDisplayName"
    FROM {{ var("source_tenant") }}.COMMUNICATION c
        LEFT JOIN {{ var("source_tenant") }}.TEAMS as t on t.id = c.teams[0]
      , LATERAL flatten(input => parse_json(c.parties)) fl
    WHERE type <> 'ContactEvent'
)
, comms_ord as (
    SELECT pc.*,
        prop.name as "propertyName",
        CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), pc.orig_created_at)::TIMESTAMP_NTZ as created_at,
        row_number() over (partition by pc.party_id order by pc.orig_created_at) ord_time
    FROM party_comms pc
        INNER JOIN {{ var("source_tenant") }}.PARTY p on p.id = party_id
        LEFT OUTER join {{ var("source_tenant") }}.PROPERTY prop on prop.id = p.ASSIGNEDPROPERTYID
)
, all_curr_out_prev_in as (
    SELECT curr.party_id,
        curr.created_at as curr_created_at,
        curr.direction as curr_direction,
        curr.id as curr_id,
        row_number() over (partition by curr.party_id order by curr.created_at) ord_fin,
        curr."teamName",
        curr."teamDisplayName"
    FROM comms_ord as curr
    LEFT JOIN comms_ord as prev on curr.party_id = prev.party_id and curr.ord_time = prev.ord_time + 1
    WHERE (curr.direction = 'out' or curr.connected = 1) and (prev.direction = 'in' and prev.connected = 0)
)
, all_curr_in_prev_out AS (
    SELECT curr.party_id,
        curr.created_at as curr_created_at,
        curr.direction as curr_direction,
        curr.id as curr_id,
        curr."isMissed" as curr_is_missed,
        curr."isVoiceMail" as curr_is_voicemail,
        row_number() over (partition by curr.party_id order by curr.created_at) ord_fin,
        curr."propertyName",
        curr."teamName",
        curr."teamDisplayName"
    FROM comms_ord as curr
        LEFT JOIN comms_ord as prev on curr.party_id = prev.party_id and curr.ord_time = prev.ord_time + 1
    WHERE curr.direction = 'in' and curr.connected = 0 and (prev.direction = 'out' or prev.direction is null or prev.connected = 1)
)
SELECT 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || c_in.party_id::text as "partyId",
    c_in."propertyName",
    c_in."teamName" as "InTeamName",
    c_in."teamDisplayName" as "InTeamDisplayName",
    c_out."teamName" as "OutTeamName",
    c_out."teamDisplayName" as "OutTeamDisplayName",
    c_in.curr_created_at as "in_created_at",
    c_out.curr_created_at as "out_created_at",
    timestampdiff(second, c_in.curr_created_at, c_out.curr_created_at) as "responseTimeSeconds",
    ceil(timestampdiff(second, c_in.curr_created_at, c_out.curr_created_at)/60)::int as "responseTimeRawMinutes",
    eh."workHours" as "responseTimeWorkMinutes", -- this is computed only for the last 3 month, 1st day of the month
    c_in.curr_id as "in_comm_id",
    c_out.curr_id as "out_comm_id",
    CASE WHEN c_in.curr_is_Missed = 'true' THEN 1 ELSE 0 END as "isMissed",
    CASE WHEN cqs.callerRequestedAction = 'call back' THEN 1 ELSE 0 END as "isCallbackRequest",
    CASE WHEN c_in.curr_is_voicemail = 'true' THEN 1
         WHEN c_in.curr_is_voicemail = 'false' AND cqs.callerRequestedAction = 'voicemail' THEN 1
         ELSE 0 END as "hasVoiceMail"
FROM all_curr_in_prev_out c_in
    JOIN all_curr_out_prev_in c_out on c_in.party_id = c_out.party_id and c_in.ord_fin = c_out.ord_fin
    LEFT JOIN {{ var("source_tenant") }}.CALLQUEUESTATISTICS cqs on cqs.COMMUNICATIONID = c_in.curr_id
    LEFT JOIN LATERAL
        (
        SELECT count(1) as "workHours"
        FROM
            (SELECT dateadd(minute, seq4(), DATE_TRUNC('month',add_months(current_date,-3))::TIMESTAMP_NTZ) as dte -- first comm date
            FROM TABLE (generator(rowcount => 175000)) -- ~4m of data
            WHERE dayofweekiso(dte) < 7 -- exclude Sunday
                AND dte::time >= '08:00'
                AND dte::time <= '17:00') a
        WHERE dte >= c_in.curr_created_at
            and dte <= c_out.curr_created_at
        ) eh
