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
      ROW_NUMBER() OVER (PARTITION BY device.advertising_id, app_info.id ORDER BY event_timestamp DESC) AS row_num
    FROM
      `{source_table}`
    WHERE
      event_name IN ('session_start', 'first_open')
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id != ''
      AND device.advertising_id != '00000000-0000-0000-0000-000000000000'
      AND _TABLE_SUFFIX BETWEEN
        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) AND
        FORMAT_DATE('%Y%m%d', CURRENT_DATE())
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
  DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y%m%d', event_date), DAY) AS days_since_install,
  event_date,
  event_timestamp
FROM
  UserLatestEvents
WHERE
  row_num = 1
