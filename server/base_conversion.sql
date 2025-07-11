# Copyright 2023-2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

WITH
  EventTable AS (
    SELECT
      device.advertising_id AS user,
      event_name,
      event_date,
    FROM `{source_table}`
    WHERE
      _TABLE_SUFFIX BETWEEN '{date_start}' AND '{date_end}'
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000', '0000-0000')
      AND app_info.id = '{app_id}'
      AND event_name IN ({all_events_list})
  ),
  AudienceUsers AS (
    SELECT DISTINCT F.user
    FROM `{all_users_table}` AS F
    WHERE
      {countries_clause}
      AND app_id = '{app_id}'
      AND EXISTS (
        SELECT user
        FROM EventTable
        WHERE user = F.user
          AND event_name IN ({audience_events_list})
          AND event_date BETWEEN '{date_audience_start}' AND '{date_audience_end}'
      )
      AND NOT EXISTS (
        SELECT user
        FROM EventTable
        WHERE user = F.user AND event_name = 'app_remove'
      )
  ),
  ConvertedUsers AS (
    SELECT DISTINCT F.user
    FROM `{all_users_table}` AS F
    WHERE
      {countries_clause}
      AND app_id = '{app_id}'
      AND EXISTS (
        SELECT user
        FROM EventTable
        WHERE user = F.user
          AND event_name IN ({conversion_events_list})
          AND event_date BETWEEN '{date_conversion_start}' AND  '{date_conversion_end}'
      )
      AND NOT EXISTS (
        SELECT user
        FROM EventTable
        WHERE user = F.user AND event_name = 'app_remove'
      )
  )
SELECT
  COUNT(A.user) AS audience,
  COUNT(C.user) AS converted,
  SAFE_DIVIDE(COUNT(C.user), COUNT(A.user)) AS cr
FROM AudienceUsers AS A
LEFT JOIN ConvertedUsers AS C USING(user)
