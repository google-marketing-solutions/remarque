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
      event_timestamp AS reg_ts,
      event_name,
      CASE
        WHEN COALESCE(event_value_in_usd, 0)>0 THEN COALESCE(event_value_in_usd, 0)
        ELSE COALESCE(
          (SELECT CAST(value.double_value AS FLOAT64)
            FROM UNNEST(event_params)
            WHERE key = 'value'), 0)
        END AS conversion_value
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
    INNER JOIN `{all_users_table}` AS UN
      USING (user)
    WHERE
      UN.app_id = '{app_id}'
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
    SELECT U.user, C.reg_date, C.reg_ts, C.conversion_value
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
    SELECT U.user, C.reg_date, C.reg_ts, C.conversion_value
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
  TestSessionCounts AS (
    SELECT
      PARSE_DATE('%Y%m%d', E.event_date) AS `date`,
      COUNT(U.user) AS test_session_count,
    FROM `{source_table}` AS E
    INNER JOIN `{test_users_table}` AS U
      ON E.device.advertising_id = U.user AND E._TABLE_SUFFIX = U._TABLE_SUFFIX
    INNER JOIN UserFirstAppearance AS FA
      ON U.user = FA.user
    WHERE
      U._TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND E.event_name = 'session_start'
      AND E.app_info.id = '{app_id}'
      AND E.event_date >= FA.first_appearance
      AND E.event_date <= FORMAT_DATE('%Y%m%d', LEAST(
        DATE_ADD(PARSE_DATE('%Y%m%d', FA.first_appearance), INTERVAL {conv_window} DAY),
        PARSE_DATE('%Y%m%d', '{day_end}')
      ))
    GROUP BY 1
  ),
  ControlSessionCounts AS (
    SELECT
      PARSE_DATE('%Y%m%d', E.event_date) AS `date`,
      COUNT(U.user) AS control_session_count,
    FROM `{source_table}` AS E
    INNER JOIN `{control_users_table}` AS U
      ON E.device.advertising_id = U.user AND E._TABLE_SUFFIX = U._TABLE_SUFFIX
    INNER JOIN UserFirstAppearance AS FA
      ON U.user = FA.user
    WHERE
      U._TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND E.event_name = 'session_start'
      AND E.app_info.id = '{app_id}'
      AND E.event_date >= FA.first_appearance
      AND E.event_date <= FORMAT_DATE('%Y%m%d', LEAST(
        DATE_ADD(PARSE_DATE('%Y%m%d', FA.first_appearance), INTERVAL {conv_window} DAY),
        PARSE_DATE('%Y%m%d', '{day_end}')
      ))
    GROUP BY 1
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
      DF.date,
      date_formatted,
      (SELECT COUNT(DISTINCT user) FROM TestConverted WHERE reg_date = DF.date_formatted)
        AS test_regs,
      (SELECT COUNT(*) FROM TestConverted WHERE reg_date = DF.date_formatted)
        AS test_events,
      (SELECT COUNT(DISTINCT user) FROM ControlConverted WHERE reg_date = DF.date_formatted)
        AS control_regs,
      (SELECT COUNT(*) FROM ControlConverted WHERE reg_date = DF.date_formatted)
        AS control_events,
      (SELECT SUM(conversion_value) FROM TestConverted WHERE reg_date = DF.date_formatted)
        AS test_conv_value,
      (SELECT SUM(conversion_value) FROM ControlConverted WHERE reg_date = DF.date_formatted)
        AS control_conv_value,
    FROM DatesFormatted AS DF
  ),
  TotalCounts AS (
{TotalCounts}
  ),
  ConversionsByUsers AS (
    SELECT
      C.date,
      SUM(C.test_regs) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_test_users,
      SUM(C.test_events) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_test_events,
      SUM(C.control_regs) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_control_users,
      SUM(C.control_events) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_control_events,
      COALESCE(T.total_test_user_count,
        LAST_VALUE(T.total_test_user_count IGNORE NULLS)
          OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        0)
        AS total_test_user_count,
      COALESCE(T.total_control_user_count,
        LAST_VALUE(T.total_control_user_count IGNORE NULLS)
          OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        0)
        AS total_control_user_count,
      SUM(C.test_conv_value) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_test_conv_value,
      SUM(C.control_conv_value) OVER (ORDER BY C.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cum_control_conv_value,
      SUM(TSC.test_session_count) OVER (ORDER BY TSC.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS test_session_count,
      SUM(CSC.control_session_count) OVER (ORDER BY CSC.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS control_session_count
    FROM GroupedConversions AS C
    LEFT JOIN TotalCounts AS T
      ON C.date = T.day
    LEFT JOIN TestSessionCounts AS TSC
      ON C.date = TSC.date
    LEFT JOIN ControlSessionCounts AS CSC
      ON C.date = CSC.date
  )
SELECT
  `date`,
  cum_test_users,
  cum_control_users,
  cum_test_events,
  cum_control_events,
  total_test_user_count,
  total_control_user_count,
  cum_test_conv_value,
  cum_control_conv_value,
  COALESCE(test_session_count,
    LAST_VALUE(test_session_count)
      OVER (ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    0)
    AS test_session_count,
  COALESCE(control_session_count,
    LAST_VALUE(control_session_count)
      OVER (ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
    0)
    AS control_session_count
FROM ConversionsByUsers
ORDER BY 1
