# Copyright 2023 Google LLC
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
  UserEvents AS (
    SELECT
      device.advertising_id AS user,
      ARRAY_AGG(event_name) AS events,
      ANY_VALUE(user_first_touch_timestamp) AS user_first_touch_timestamp,
      SUM(IF(event_name = 'session_start', 1, 0)) AS n_sessions
    FROM
      `{source_table}`
    WHERE
      _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND app_info.id = '{app_id}'
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN
        ('', '00000000-0000-0000-0000-000000000000', '0000-0000')
      AND event_name IN ({all_events_list})
    GROUP BY
      1
  ),
  FilteredUsers AS (
    SELECT
      F.user,
      F.mobile_brand_name AS brand,
      F.operating_system_version AS osv,
      F.acquisition_source,
      F.acquisition_medium,
      UserEvents.user_first_touch_timestamp,
      UserEvents.n_sessions,
      F.country
    FROM
      UserEvents
      INNER JOIN `{all_users_table}` AS F USING (user)
    WHERE
      {countries_clause}
      AND F.app_id = '{app_id}'
      AND {SEARCH_CONDITIONS}
  )
SELECT DISTINCT
  user,
  brand,
  osv,
  DATE_DIFF(
    CURRENT_DATE(),
    DATE(TIMESTAMP_MICROS(user_first_touch_timestamp)),
    DAY
  ) AS days_since_install,
  IFNULL(acquisition_source,'') || '_' || IFNULL(acquisition_medium,'') AS src,
  n_sessions,
  country
FROM
  FilteredUsers
