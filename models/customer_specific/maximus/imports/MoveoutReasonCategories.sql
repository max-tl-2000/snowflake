/*
 * Copyright (c) 2022 Reva Technology Inc., all rights reserved.
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Licensed under the Elastic License 2.0; you may not use this file except
 * in compliance with the Elastic License 2.0.
 */
/*
** manual data
dbt run --select customer_specific.maximus.imports.MoveoutReasonCategories --vars '{"target_schema": "SNOW_9F27B14E_6973_48A5_B746_434828265538"}'
*/

{{ config(alias='Move-outReasonCategories') }}

SELECT 'Act of God' AS "Notice Category", 'Act of God / Disaster' AS "NoticeReason"
UNION ALL SELECT 'Construction', 'Construction'
UNION ALL SELECT 'Construction', 'Home Renovation Complete'
UNION ALL SELECT 'Corporate Lease', 'Corporate'
UNION ALL SELECT 'Corporate Lease', 'Corporate Lease'
UNION ALL SELECT 'Cost', 'Price'
UNION ALL SELECT 'Cost', 'Rent too high'
UNION ALL SELECT 'Crime', 'Crime'
UNION ALL SELECT 'Death or Illness', 'Death in Unit'
UNION ALL SELECT 'Death or Illness', 'Death or Illness'
UNION ALL SELECT 'Financial ', 'Financial Difficulties'
UNION ALL SELECT 'Financial ', 'Termination of Government Assistance'
UNION ALL SELECT 'Job Related', 'Job Loss'
UNION ALL SELECT 'Job Related', 'Job Transfer - out of area'
UNION ALL SELECT 'Job Related', 'Job Transfer (out of area)'
UNION ALL SELECT 'Legal', 'Lease Violation'
UNION ALL SELECT 'Legal', 'LEGAL - Legal Stipulation'
UNION ALL SELECT 'Legal', 'Legal Stipulation'
UNION ALL SELECT 'Legal', 'Mutual Release'
UNION ALL SELECT 'Legal', 'Unlawful Detainer'
UNION ALL SELECT 'Life Event', 'End of School/Semester'
UNION ALL SELECT 'Life Event', 'Graduation'
UNION ALL SELECT 'Moving to SFH', 'Bought Home'
UNION ALL SELECT 'Moving to SFH', 'Single Family Home'
UNION ALL SELECT 'Moving to SFH', 'Single Family Home - Bought'
UNION ALL SELECT 'Moving to SFH', 'Single Family Home - Rented'
UNION ALL SELECT 'NO REASON PROVIDED', 'N/A'
UNION ALL SELECT 'Other', 'Costa Hawkins'
UNION ALL SELECT 'Other', 'Leaseholder Replacement'
UNION ALL SELECT 'Other', 'Skip'
UNION ALL SELECT 'Relocation', 'Relocating'
UNION ALL SELECT 'Relocation', 'Relocating (out of area)'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Location'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Need more parking'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Need more space'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Parking'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Property Unsatisfactory'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Service'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Unit Unsatisfactory'
UNION ALL SELECT 'Satisfaction with Property or Unit', 'Unsatisfactory School District'
UNION ALL SELECT 'Transfer', 'NUR'
UNION ALL SELECT 'Transfer', 'On-Site Transfer'
