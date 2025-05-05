# Copyright 2023-2005 Google LLC
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
  UserLatestEvents AS (
    SELECT
      device.advertising_id AS user,
      app_info.id AS app_id,
      geo.country AS country,
      geo.city AS city,
      geo.region AS region,
      traffic_source.source AS acquisition_source,
      traffic_source.medium AS acquisition_medium,
      device.mobile_brand_name,
      device.operating_system_version,
      event_date,
      event_timestamp,
      ROW_NUMBER() OVER (PARTITION BY device.advertising_id, app_info.id
        ORDER BY event_timestamp DESC) AS row_num
    FROM `{source_table}`
    WHERE
      event_name IN ('session_start', 'first_open')
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000', '0000-0000')
      {SEARCH_CONDITIONS}
  )
SELECT
  user,
  app_id,
  country,
  city,
  region,
  acquisition_source,
  acquisition_medium,
  mobile_brand_name,
  operating_system_version,
  NULL AS days_since_install, -- for backward compatibility
  event_date,
  event_timestamp
FROM UserLatestEvents
WHERE
  row_num = 1
