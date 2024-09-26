# Copyright 2024 Google LLC
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
-- Calculates conversions for users in treatment and control groups.
--
-- @param source_table: A wildcarded name of GA4 table (events_*).
-- @param day_start: A start date formatted as %Y%m%d.
-- @param date_start: A start date formatted as %Y-%m-%d.
-- @param day_end: An end date formatted as %Y%m%d.
-- @param date_end: An end date formatted as %Y-%m-%d.
-- @param app_id: An application id.
-- @param events: A list of event names.
-- @param all_users_table: A fully qualified name of 'users_normalized' table.
-- @param test_users_table: A wildcarded name of table with test users.
-- @param control_users_table: A wildcarded name of table with control users.
-- @param TotalCounts: A string with additional query that provides a
--  `TotalCounts` subquery, which contains its own params
-- @param conv_window: a number of days for conversion window

WITH
  AllConversions AS (
    SELECT
      device.advertising_id AS user,
      event_date AS reg_date,
      RANK() OVER (PARTITION BY device.advertising_id ORDER BY event_date) AS rr
    FROM `{source_table}`
    WHERE
      device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000', '0000-0000')
      AND _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND app_info.id = '{app_id}'
      AND event_name IN ({events})
  ),
  Conversions AS (
    SELECT *
    FROM AllConversions
    INNER JOIN `{all_users_table}`
      USING (user)
    WHERE
      {SEARCH_CONDITIONS}
  ),
  UserFirstAppearance AS (
    SELECT user, MIN(_TABLE_SUFFIX) AS first_appearance
    FROM `{test_users_table}`
    WHERE _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
    GROUP BY user
    UNION ALL
    SELECT user, MIN(_TABLE_SUFFIX) AS first_appearance
    FROM `{control_users_table}`
    WHERE _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
    GROUP BY user
  ),
  TestConverted AS (
    SELECT DISTINCT U.user, C.reg_date
    FROM `{test_users_table}` AS U
    INNER JOIN Conversions AS C
      ON U.user = C.user
    INNER JOIN UserFirstAppearance AS FA
      ON U.user = FA.user
    WHERE
      U._TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND C.reg_date >= FA.first_appearance
      AND C.reg_date <= FORMAT_DATE('%Y%m%d', LEAST(
        DATE_ADD(PARSE_DATE('%Y%m%d', FA.first_appearance), INTERVAL {conv_window} DAY),
        PARSE_DATE('%Y%m%d', '{day_end}')
      ))
  ),
  ControlConverted AS (
    SELECT DISTINCT U.user, C.reg_date
    FROM `{control_users_table}` AS U
    INNER JOIN Conversions AS C
      ON U.user = C.user
    INNER JOIN UserFirstAppearance AS FA
      ON U.user = FA.user
    WHERE
      U._TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND C.reg_date >= FA.first_appearance
      AND C.reg_date <= FORMAT_DATE('%Y%m%d', LEAST(
        DATE_ADD(PARSE_DATE('%Y%m%d', FA.first_appearance), INTERVAL {conv_window} DAY),
        PARSE_DATE('%Y%m%d', '{day_end}')
      ))
  ),
  Dates AS (
    SELECT GENERATE_DATE_ARRAY('{date_start}', '{date_end}', INTERVAL 1 DAY) AS date_array
  ),
  DatesFormatted AS (
    SELECT
      `date`,
      FORMAT_DATE('%Y%m%d',`date`) AS date_formatted
    FROM Dates, UNNEST(date_array) AS `date`
  ),
  GroupedConversions AS (
    SELECT
      `date`,
      (SELECT COUNT(DISTINCT user) FROM TestConverted AS T WHERE T.reg_date = date_formatted)
        AS test_regs,
      (SELECT COUNT(DISTINCT user) FROM ControlConverted AS T WHERE T.reg_date = date_formatted)
        AS control_regs,
    FROM
      DatesFormatted
    ORDER BY 1 ASC
  ),
  TotalCounts AS (
{TotalCounts}
  ),
  ConversionsByUsers AS (
    SELECT
      C.date,
      SUM(test_regs) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_test_regs,
      SUM(control_regs) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_control_regs,
      COALESCE(T.total_user_count,
        LAST_VALUE(T.total_user_count IGNORE NULLS)
        OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        0
      ) AS total_user_count,
      COALESCE(T.total_control_user_count,
        LAST_VALUE(T.total_control_user_count IGNORE NULLS)
        OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        0
      ) AS total_control_user_count
    FROM GroupedConversions AS C
    LEFT JOIN
      (SELECT * FROM TotalCounts WHERE r = 1) AS T
      ON C.date = T.day
  )
SELECT
  `date`,
  cum_test_regs,
  cum_control_regs,
  total_user_count,
  total_control_user_count,
  SAFE_DIVIDE(cum_test_regs, total_user_count) AS cr_test,
  SAFE_DIVIDE(cum_control_regs, total_control_user_count) AS cr_control
FROM ConversionsByUsers
ORDER BY 1
