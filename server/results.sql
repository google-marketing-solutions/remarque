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
      (SELECT count(user) FROM control_converted t WHERE t.reg_date = date_formatted) AS control_regs
    FROM
      dates_formatted d
    ORDER BY 1 ASC
  )
SELECT
  date,
  SUM(test_regs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_test_regs,
  SUM(control_regs) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_control_regs
FROM grouped_conversions
ORDER BY 1
