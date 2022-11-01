/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select customer_specific.maximus.custom_queries.MostRecentCommByGroupId --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
*/

{{ config(alias='MostRecentCommByGroupId') }}


SELECT *
FROM (
    SELECT mostRecentGroupComm.*
        , p.partyGroupId AS "partyGroupId"
        , rank() OVER (
            PARTITION BY p.partyGroupId ORDER BY mostRecentGroupComm."created_at" DESC
            ) AS "theRank"
    FROM (
        SELECT fl.value::VARCHAR AS "partyId"
            , CONVERT_TIMEZONE('{{ var("timezone") }}',c.created_at)::TIMESTAMP_NTZ AS "created_at"
            , c.direction AS "direction"
            , c.type AS "type"
            , CASE
                WHEN c.type = 'Call'
                    THEN COALESCE(c.message: isMissed, 'false')
                ELSE NULL
                END AS "isMissed"
            , REPLACE(c.message: duration, '"', '') AS "duration"
            , EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT AS "CallSeconds"
            , CASE
                WHEN c.type = 'Call'
                    THEN COALESCE(c.message: isVoiceMail, 'false')
                ELSE NULL
                END AS "isVoiceMail"
        FROM {{ var("source_tenant") }}.COMMUNICATION AS c
            , LATERAL flatten(input => parse_json(c.parties)) AS fl
        WHERE COALESCE(c.message: isMissed, 'false') = 'false' AND (
                CASE
                    WHEN c.type = 'Call'
                        THEN (EXTRACT(MINUTE FROM to_time('00:' || (c.message: duration::VARCHAR))) * 60 + EXTRACT(second FROM to_time('00:' || (c.message: duration::VARCHAR)))::INT) > 15
                    ELSE true
                    END
                )
        ) AS mostRecentGroupComm
    INNER JOIN {{ var("source_tenant") }}.PARTY AS p ON p.id::TEXT = mostRecentGroupComm."partyId"
    ) AS a
WHERE a."theRank" = 1
