WITH
  all_conversions AS (
    SELECT
      DISTINCT device.advertising_id user,
      event_date reg_date,
      rank() over(partition by device.advertising_id order by event_date) rr
    FROM
      `{source_table}`
    WHERE
      device.category = 'mobile'
      AND device.operating_system = 'Android'
      AND device.advertising_id IS NOT NULL
      AND device.advertising_id NOT IN ('', '00000000-0000-0000-0000-000000000000')
      AND _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
      AND event_name IN ({events})
  ),
  conversions AS (
    SELECT * FROM all_conversions
    WHERE rr = 1
  ),
  test_converted AS (
    SELECT
      DISTINCT user,
      reg_date
    FROM
      `{test_users_table}`
      JOIN conversions USING (user)
    WHERE _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
  ),
  control_converted AS (
    SELECT
      DISTINCT user,
      reg_date
    FROM
      `{control_users_table}`
      JOIN conversions USING (user)
    WHERE _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
  ),
  test_counts AS (
    SELECT
      _TABLE_SUFFIX AS date,
      count(*) AS user_count
    FROM `{test_users_table}`
    WHERE LENGTH(_TABLE_SUFFIX) = 8
    GROUP BY _TABLE_SUFFIX
    HAVING _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
  ),
  control_counts AS (
    SELECT
      _TABLE_SUFFIX AS date,
      count(*) AS user_count
    FROM `{control_users_table}`
    WHERE LENGTH(_TABLE_SUFFIX) = 8
    GROUP BY _TABLE_SUFFIX
    HAVING _TABLE_SUFFIX BETWEEN '{day_start}' AND '{day_end}'
  ),
  dates AS (
    SELECT GENERATE_DATE_ARRAY('{date_start}', '{date_end}', INTERVAL 1 DAY) AS date_array
  ),
  dates_formatted AS (
    SELECT date, FORMAT_DATE('%Y%m%d', date) as date_formatted
    FROM dates, UNNEST(date_array) AS date
  ),
  grouped_conversions AS (
    SELECT
      date,
      (SELECT count(user) FROM test_converted t WHERE t.reg_date = date_formatted) AS test_regs,
      (SELECT count(user) FROM control_converted t WHERE t.reg_date = date_formatted) AS control_regs,
      -- total number of test users on the 'date' date
      -- total number of control users on the 'date' date
      --(SELECT user_count FROM test_counts t WHERE t.date = date_formatted) AS test_counts,
      --(SELECT user_count FROM control_counts t WHERE t.date = date_formatted) AS control_counts,
    FROM
      dates_formatted d
    ORDER BY 1 ASC
  ),
  total_counts AS (
    SELECT DISTINCT DATE(date) as day, total_user_count, total_control_user_count
    FROM `{audiences_log}` l
    WHERE NAME = '{audience_name}'
  )
SELECT
  date,
  SUM(test_regs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_test_regs,
  SUM(control_regs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_control_regs,
  coalesce(t.total_user_count,
    LAST_VALUE(t.total_user_count IGNORE NULLS)
    OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
  ) AS total_user_count,
  coalesce(t.total_control_user_count,
    LAST_VALUE(t.total_control_user_count IGNORE NULLS)
    OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
  ) AS total_control_user_count
FROM grouped_conversions c
  LEFT JOIN total_counts t ON c.date = t.day
ORDER BY 1
