/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
dbt run --select dumps.TaskDump --vars '{"source_tenant": "RAW.PRODW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "target_schema": "SNOW_8904DEEF_7CF9_4675_AC66_B9A64C8B86F8", "timezone": "America/Chicago", "client": "customernew"}'
dbt run --select dumps.TaskDump --vars '{"source_tenant": "RAW.PRODW_9F27B14E_6973_48A5_B746_434828265538", "target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538", "timezone": "America/Los_Angeles", "client": "maximus"}'
dbt run --select dumps.TaskDump --vars '{"source_tenant": "RAW.PRODW_18B4A573_560E_4755_8316_8E99AEFB004E", "target_schema": "SNOW_18B4A573_560E_4755_8316_8E99AEFB004E", "timezone": "America/New_York", "client": "glick"}'
*/

{{ config(alias='TaskDump') }}

SELECT date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP AS "dumpGenDate"
    , ('1970-01-01 ' || (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)::TIME)::VARCHAR)::TIMESTAMP AS "dumpGenTime"
    , 'https://' || '{{ var("client") }}' || '.reva.tech/party/' || p.id::TEXT AS "partyId"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at))::TIMESTAMP AS "dateTaskCreated"
    , date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP AS "dateTaskDue"
    , CASE t.STATE
        WHEN 'Completed'
            THEN date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate)))::TIMESTAMP
        WHEN 'Canceled'
            THEN date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.updated_at)))::TIMESTAMP
        END AS "dateTaskCompleted"
    , CASE
        WHEN t.STATE = 'Completed'
            THEN CASE
                    WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                        THEN 'Yes'
                    ELSE 'No'
                    END
        WHEN t.STATE = 'Active'
            THEN CASE
                    WHEN date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                        THEN 'Yes'
                    ELSE 'No'
                    END
        ELSE 'No'
        END AS "overdueFlag"
    , CASE t.STATE
        WHEN 'Active'
            THEN DATEDIFF(day, date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP))::TIMESTAMP) + 1
        WHEN 'Canceled'
            THEN DATEDIFF(day, date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.updated_at))::TIMESTAMP) + 1
        WHEN 'Completed'
            THEN DATEDIFF(DAY, date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.created_at)))::TIMESTAMP, date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate))::TIMESTAMP) + 1
        ELSE - 1
        END AS "numDaysOpen"
    , t.STATE AS "state"
    , tu.taskOwner AS "taskOwner"
    , pu.fullName AS "partyOwner"
    , p.STATE AS "salesStage"
    , t.category AS "taskType"
    , t.id AS "id"
    , '"' || t.NAME || '"' AS "taskName"
    , prop.name AS "propertyName"
    , CASE
        WHEN p.endDate IS NULL
            THEN NULL
        ELSE CASE
                WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                    THEN 'MERGED_WITH_ANOTHER_PARTY'
                ELSE p.metadata: closeReasonId
                END
        END AS "partyCloseReason"
    , CASE
        WHEN t.metadata: completedBy = 'SYSTEM'
            THEN 'SYSTEM'
        ELSE COALESCE(tcb.fullName, '')
        END AS "taskCompletedBy"
    , CASE
        WHEN t.category = 'Manual'
            THEN 'YES'
        ELSE ''
        END AS "AgentCreated"
    , CASE
        WHEN (
                CASE
                    WHEN p.endDate IS NULL
                        THEN NULL
                    ELSE CASE
                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                            ELSE p.metadata: closeReasonId
                            END
                    END
                ) IS NULL
            THEN 'Open'
        ELSE (
                CASE
                    WHEN p.endDate IS NULL
                        THEN NULL
                    ELSE CASE
                            WHEN p.metadata: archiveReasonId = 'MERGED_WITH_ANOTHER_PARTY'
                                THEN 'MERGED_WITH_ANOTHER_PARTY'
                            ELSE p.metadata: closeReasonId
                            END
                    END
                )
        END AS "partyCloseReasonNN"
    , CASE
        WHEN p.STATE = 'Contact'
            THEN '1-Prospect'
        WHEN p.STATE = 'Lead'
            THEN '2-Contacts'
        WHEN p.STATE = 'Prospect'
            THEN '3-Tour/Quoted'
        WHEN p.STATE = 'Applicant'
            THEN '4-Applicant'
        WHEN p.STATE = 'Lease'
            THEN '5-Leasing'
        WHEN p.STATE = 'FutureResident'
            THEN '6-Future Resident'
        WHEN p.STATE = 'Resident'
            THEN '7-Resident'
        END AS "stagesort"
    , CASE
        WHEN (
                CASE
                    WHEN t.STATE = 'Completed'
                        THEN CASE
                                WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                                    THEN 'Yes'
                                ELSE 'No'
                                END
                    WHEN t.STATE = 'Active'
                        THEN CASE
                                WHEN date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                                    THEN 'Yes'
                                ELSE 'No'
                                END
                    ELSE 'No'
                    END
                ) = 'Yes'
            THEN 1
        WHEN (
                CASE
                    WHEN t.STATE = 'Completed'
                        THEN CASE
                                WHEN date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.completionDate))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                                    THEN 'Yes'
                                ELSE 'No'
                                END
                    WHEN t.STATE = 'Active'
                        THEN CASE
                                WHEN date_trunc('day', (CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), CURRENT_TIMESTAMP)))::TIMESTAMP > date_trunc('day', CONVERT_TIMEZONE(COALESCE(prop.timezone, '{{ var("timezone") }}'), t.dueDate))::TIMESTAMP
                                    THEN 'Yes'
                                ELSE 'No'
                                END
                    ELSE 'No'
                    END
                ) = 'No'
            THEN 0
        END AS "overdueflagcount"
FROM {{ var("source_tenant") }}.Tasks AS t
LEFT OUTER JOIN (
    SELECT t.id AS taskId
        , LISTAGG(u.FULLNAME, ' | ') AS taskOwner
    FROM {{ var("source_tenant") }}.Tasks AS t
    INNER JOIN {{ var("source_tenant") }}.Users AS u ON ARRAY_CONTAINS(u.id::VARIANT, t.userIds)
    GROUP BY t.id
    ) AS tu ON tu.taskId = t.id
LEFT OUTER JOIN {{ var("source_tenant") }}.Party AS p ON p.id = t.partyId
LEFT OUTER JOIN {{ var("source_tenant") }}.Property AS prop ON prop.id = p.assignedPropertyId
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS pu ON pu.id = p.userId
LEFT OUTER JOIN {{ var("source_tenant") }}.Users AS tcb ON tcb.id::VARCHAR = t.metadata: completedBy
WHERE p.workflowName IN ('newLease', 'renewal')
